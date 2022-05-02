use crate::fs::MockDir;
use std::fmt::Write;

use super::Db;

#[test]
fn test_db_trace() {
    datadriven::walk("src/db/testdata/", |f| {
        let dir = MockDir::new();
        let mut db: Db<_, String, String> = Db::new(dir.clone()).unwrap();
        f.run(|test_case| match test_case.directive.as_str() {
            "insert" => {
                for line in test_case.input.lines() {
                    let eq_idx = line.find('=').unwrap();
                    let key = line[0..eq_idx].to_owned();
                    let val = line[eq_idx + 1..].to_owned();
                    db.insert(key, val);
                }
                "ok\n".into()
            }
            "get" => {
                let key = test_case.input.trim();
                let iter = db.get(&key.to_owned());

                format!("{:?}\n", iter)
            }
            "scan" => db
                .scan()
                .map(|x| format!("{:?}\n", x))
                .collect::<Vec<_>>()
                .join(""),
            "flush-memtable" => {
                db.flush_memtable().unwrap();
                "ok\n".into()
            }
            "merge" => {
                let targets = test_case
                    .input
                    .lines()
                    .map(|line| line.split_once(',').unwrap())
                    .map(|(l1, i1)| (l1.parse().unwrap(), i1.parse().unwrap()))
                    .collect::<Vec<_>>();

                db.merge(&targets);

                "ok\n".into()
            }
            "trace" => {
                let mut result = String::new();
                for event in (*dir.fs).borrow_mut().take_events() {
                    event.write_abbrev(&mut result).unwrap();
                    result.push('\n');
                }
                if test_case.args.contains_key("squelch") {
                    "ok\n".into()
                } else {
                    result
                }
            }
            "dump" => {
                let mut out = String::new();
                for line in test_case.input.lines() {
                    match line.trim() {
                        "root" => writeln!(&mut out, "{:#?}", db.root.data).unwrap(),
                        "layout" => writeln!(&mut out, "{:#?}", db.layout).unwrap(),
                        _ => writeln!(&mut out, "can't dump {:?}", line.trim()).unwrap(),
                    }
                }
                out
            }
            "reload" => {
                db = Db::new(dir.clone()).unwrap();
                "ok\n".into()
            }
            _ => {
                panic!("unhandled");
            }
        })
    })
}
