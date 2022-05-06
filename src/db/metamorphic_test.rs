use std::fmt::Write;

use rand::Rng;

use crate::fs::MockDir;

use super::Db;

#[derive(Debug, Clone)]
enum Op {
    Insert(String, String),
    Delete(String),
    Get(String),
    FlushMemtable,
    Reload,
    Merge((usize, usize)),
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
        let dir = MockDir::new();

        let mut db: Db<_, String, String> = Db::new(dir.clone()).unwrap();

        let mut out = Vec::new();

        for input in it {
            match input {
                Op::Insert(k, v) => db.insert(k, v),
                Op::Delete(k) => db.delete(k),
                Op::Get(k) => out.push(db.get(&k).unwrap()),
                Op::FlushMemtable => {
                    db.flush_memtable().unwrap();
                }
                Op::Reload => {
                    db = Db::new(dir.clone()).unwrap();
                }
                Op::Merge(target) => {
                    // It's ok if this doesn't do anything.
                    let _ = db.merge(vec![target], target.0 + 1);
                }
            }
        }

        out
    }

    fn formatted(self) -> String {
        let mut out = String::new();
        for op in self {
            writeln!(&mut out, "{:?}", op).unwrap();
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
    for _ in 0..1000 {
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
            match rng.gen_range(0..3) {
                0 => test_case.add_physical_op(idx, Op::FlushMemtable),
                1 => test_case.add_physical_op(idx, Op::Reload),
                2 => test_case
                    .add_physical_op(idx, Op::Merge((rng.gen_range(0..3), rng.gen_range(0..3)))),
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
        }
    }
    panic!("they differed!")
}
