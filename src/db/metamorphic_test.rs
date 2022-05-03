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

fn run_sequence(inputs: TestCase) -> Vec<Option<String>> {
    let dir = MockDir::new();

    let mut db: Db<_, String, String> = Db::new(dir.clone()).unwrap();

    let mut out = Vec::new();

    for input in inputs {
        match input {
            Op::Insert(k, v) => db.insert(k, v),
            Op::Delete(k) => db.delete(k),
            Op::Get(k) => out.push(db.get(&k)),
            Op::FlushMemtable => {
                db.flush_memtable().unwrap();
            }
            Op::Reload => {
                db = Db::new(dir.clone()).unwrap();
            }
            Op::Merge(target) => {
                // It's ok if this doesn't do anything.
                let _ = db.merge(&[target]);
            }
        }
    }

    out
}

#[test]
fn metamorphic_test() {
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

    let mut inputs = TestCase::new(inputs, Vec::new());

    let mut new_inputs = inputs.clone();

    let expected_output = run_sequence(inputs.clone());

    for _ in 0..100 {
        let idx = rng.gen_range(0..new_inputs.logical_len());
        match rng.gen_range(0..3) {
            0 => new_inputs.add_physical_op(idx, Op::FlushMemtable),
            1 => new_inputs.add_physical_op(idx, Op::Reload),
            2 => new_inputs
                .add_physical_op(idx, Op::Merge((rng.gen_range(0..3), rng.gen_range(0..3)))),
            _ => unreachable!(),
        }
    }

    new_inputs.sort();

    let new_output = run_sequence(new_inputs.clone());

    if new_output != expected_output {
        let reduced_case = loop {
            let mut better_case = None;

            for idx in 0..inputs.logical_len() {
                let mut inputs_reduced = inputs.clone();
                let mut new_inputs_reduced = new_inputs.clone();
                inputs_reduced.apply_reduction(Reduction::DeleteLogicalOp(idx));
                new_inputs_reduced.apply_reduction(Reduction::DeleteLogicalOp(idx));

                let a_output = run_sequence(inputs_reduced.clone());
                let b_output = run_sequence(new_inputs_reduced.clone());

                if a_output != b_output {
                    better_case = Some((inputs_reduced, new_inputs_reduced));
                    break;
                }
            }
            if let Some((a, b)) = better_case {
                inputs = a;
                new_inputs = b;
                continue;
            }

            for idx in 0..new_inputs.physical_len() {
                let mut new_inputs_reduced = new_inputs.clone();
                new_inputs_reduced.apply_reduction(Reduction::DeletePhysicalOp(idx));

                let a_output = run_sequence(inputs.clone());
                let b_output = run_sequence(new_inputs_reduced.clone());

                if a_output != b_output {
                    better_case = Some((inputs.clone(), new_inputs_reduced));
                    break;
                }
            }
            if let Some((a, b)) = better_case {
                inputs = a;
                new_inputs = b;
                continue;
            }

            break (inputs, new_inputs);
        };

        println!("reduced case: {:#?}", reduced_case);
        panic!("they differed!")
    }
}
