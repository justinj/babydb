use std::{fmt::Write, io::Read};

use rand::Rng;

use crate::fs::{DbDir, DbFile, MockDir};

use super::Db;

#[derive(Debug, Clone)]
enum Op {
    Insert(String, String),
    Delete(String),
    Get(String),
    FlushMemtable,
    Reload,
    Merge((usize, usize)),
    ScheduleHardCrash(usize),
}

impl Op {
    fn to_trace(&self) -> String {
        match self {
            Op::Insert(k, v) => format!("insert\n{}={}\n----\n", k, v),
            Op::Delete(k) => format!("delete\n{}\n----\n", k),
            Op::Get(k) => format!("get\n{}\n----\n", k),
            Op::FlushMemtable => "flush-memtable\n----\n".to_owned(),
            Op::Reload => "reload\n----\n".to_owned(),
            Op::Merge((level, index)) => format!("merge\n{},{}\n----\n", level, index),
            Op::ScheduleHardCrash(ops) => format!("crash-in\n{}\n----\n", ops),
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum Reduction {
    DeleteLogicalOp(usize),
    DeletePhysicalOp(usize),
}

#[derive(Debug, Clone)]
struct TestCase {
    logical_ops: Vec<Op>,
    physical_ops: Vec<(usize, Op)>,
    logical_idx: usize,
    physical_idx: usize,
}

impl TestCase {
    fn new(logical_ops: Vec<Op>, physical_ops: Vec<(usize, Op)>) -> Self {
        TestCase {
            logical_ops,
            physical_ops,
            logical_idx: 0,
            physical_idx: 0,
        }
    }

    fn add_physical_op(&mut self, idx: usize, op: Op) {
        self.physical_ops.push((idx, op));
    }

    fn sort(&mut self) {
        self.physical_ops.sort_by_key(|(idx, _)| *idx);
    }

    fn logical_len(&self) -> usize {
        self.logical_ops.len()
    }

    fn physical_len(&self) -> usize {
        self.physical_ops.len()
    }

    fn apply_reduction(&mut self, reduction: Reduction) {
        match reduction {
            Reduction::DeletePhysicalOp(idx) => {
                self.physical_ops.remove(idx);
            }
            Reduction::DeleteLogicalOp(idx) => {
                self.logical_ops.remove(idx);
                for op in self.physical_ops.iter_mut() {
                    if op.0 > idx {
                        op.0 -= 1;
                    }
                }
            }
        }
    }

    fn run(&self) -> Vec<Option<String>> {
        Self::run_iter(self.clone())
    }

    fn run_logical(&self) -> Vec<Option<String>> {
        Self::run_iter(self.logical_ops.clone().into_iter())
    }

    fn run_iter<I: Iterator<Item = Op>>(it: I) -> Vec<Option<String>> {
        let mut dir = MockDir::new();

        let mut db: Db<_, String, String> = Db::new(dir.clone()).unwrap();

        let mut out = Vec::new();

        for input in it {
            loop {
                let cloned = input.clone();
                let result = match cloned {
                    Op::Insert(k, v) => db.insert(k, v),
                    Op::Delete(k) => db.delete(k),
                    Op::Get(k) => {
                        out.push(db.get(&k).unwrap());
                        Ok(())
                    }
                    Op::FlushMemtable => db.flush_memtable(),
                    Op::Reload => match Db::new(dir.clone()) {
                        Ok(new) => {
                            db = new;
                            Ok(())
                        }
                        Err(e) => Err(e),
                    },
                    Op::Merge(target) => {
                        // It's ok if this doesn't do anything.
                        db.merge(vec![target], target.0 + 1)
                    }
                    Op::ScheduleHardCrash(ops) => {
                        (*dir.fs).borrow_mut().schedule_crash(ops);
                        Ok(())
                    }
                };

                if result.is_ok() {
                    break;
                } else {
                    // If this errors, it means we have hard crashed. We
                    // should reboot the filesystem and database and retry
                    // the operation.
                    (*dir.fs).borrow_mut().reboot();
                    // println!("fs = {:?}", (*dir.fs).borrow_mut());
                    // let f = dir.open(&"ROOT").unwrap();
                    // println!("f = {:?}", f.file_id);
                    // let contents = f.read_all();
                    // println!("contents = {:?}", contents);
                    db = Db::new(dir.clone()).unwrap();
                }
            }
        }

        out
    }

    fn formatted(self) -> String {
        let mut out = String::new();
        for op in self {
            writeln!(&mut out, "{}", op.to_trace()).unwrap();
        }
        out
    }
}

impl Iterator for TestCase {
    type Item = Op;

    fn next(&mut self) -> Option<Self::Item> {
        if self.physical_idx < self.physical_ops.len()
            && self.physical_ops[self.physical_idx].0 <= self.logical_idx
        {
            self.physical_idx += 1;
            Some(self.physical_ops[self.physical_idx - 1].1.clone())
        } else if self.logical_idx < self.logical_ops.len() {
            self.logical_idx += 1;
            Some(self.logical_ops[self.logical_idx - 1].clone())
        } else {
            None
        }
    }
}

#[test]
fn metamorphic_test() {
    for _ in 0..100 {
        let mut rng = rand::thread_rng();
        let inputs = (0..50)
            .map(|_| match rng.gen_range(0..3) {
                0 => Op::Insert(
                    format!("key{}", rng.gen_range(0..10)),
                    format!("value{}", rng.gen_range(0..10)),
                ),
                1 => Op::Delete(format!("key{}", rng.gen_range(0..10))),
                2 => Op::Get(format!("key{}", rng.gen_range(0..10))),
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();

        let mut test_case = TestCase::new(inputs, Vec::new());

        for _ in 0..100 {
            let idx = rng.gen_range(0..test_case.logical_len());
            match rng.gen_range(0..4) {
                0 => test_case.add_physical_op(idx, Op::FlushMemtable),
                1 => test_case.add_physical_op(idx, Op::Reload),
                2 => test_case
                    .add_physical_op(idx, Op::Merge((rng.gen_range(0..3), rng.gen_range(0..3)))),
                3 => test_case.add_physical_op(idx, Op::ScheduleHardCrash(rng.gen_range(0..10))),
                _ => unreachable!(),
            }
        }

        test_case.sort();

        let expected_output = test_case.run_logical();
        let new_output = test_case.run();

        if new_output != expected_output {
            let reduced_case = loop {
                let mut better_case = None;

                for idx in 0..test_case.logical_len() {
                    let mut reduced = test_case.clone();
                    reduced.apply_reduction(Reduction::DeleteLogicalOp(idx));

                    let a_output = reduced.run();
                    let b_output = reduced.run_logical();

                    if a_output != b_output {
                        better_case = Some(reduced);
                        break;
                    }
                }
                if let Some(tc) = better_case {
                    test_case = tc;
                    continue;
                }

                for idx in 0..test_case.physical_len() {
                    let mut reduced = test_case.clone();
                    reduced.apply_reduction(Reduction::DeletePhysicalOp(idx));

                    let a_output = reduced.run();
                    let b_output = reduced.run_logical();

                    if a_output != b_output {
                        better_case = Some(reduced);
                        break;
                    }
                }
                if let Some(tc) = better_case {
                    test_case = tc;
                    continue;
                }

                break test_case;
            };

            println!("\n\n\nreduced case:\n\n\n{}", reduced_case.formatted());
            panic!("they differed!")
        }
    }
}
