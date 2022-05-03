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
}

fn run_sequence(inputs: Vec<Op>) -> Vec<Option<String>> {
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

    let mut new_inputs = inputs.clone();

    let expected_output = run_sequence(inputs);

    for _ in 0..100 {
        let idx = rng.gen_range(0..new_inputs.len());
        match rng.gen_range(0..2) {
            0 => new_inputs.insert(idx, Op::FlushMemtable),
            1 => new_inputs.insert(idx, Op::Reload),
            _ => unreachable!(),
        }
    }

    let new_output = run_sequence(new_inputs);

    if new_output != expected_output {
        panic!("they differed!")
    }
}
