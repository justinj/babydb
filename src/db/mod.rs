#![allow(dead_code)]
use std::{cell::RefCell, marker::PhantomData, rc::Rc};

use crate::{
    log::{LogEntry, LogSet, Logger},
    memtable::{KVIter, Memtable, MergingIter, SeqnumIter},
    sst::{reader::SstReader, writer::SstWriter, Decode, Encode},
};

struct DbIterator<K, V, I>
where
    K: Ord,
    I: KVIter<K, V>,
{
    iter: I,
    _marker: PhantomData<(K, V)>,
}

impl<K, V, I> Iterator for DbIterator<K, V, I>
where
    K: Ord + Default + Clone,
    V: Default + Clone,
    I: KVIter<K, V>,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        let (k, v) = self.iter.next()?;
        Some((k.clone(), v.clone()))
    }
}

#[derive(Debug, Clone)]
enum DBEntry<K, V>
where
    K: std::fmt::Debug,
    V: std::fmt::Debug,
{
    Write(usize, K, V),
    Delete(usize, K),
}

impl<K, V> LogEntry for DBEntry<K, V>
where
    K: std::fmt::Debug + Clone,
    V: std::fmt::Debug + Clone,
{
    fn seqnum(&self) -> usize {
        match self {
            DBEntry::Write(x, _, _) => *x,
            DBEntry::Delete(x, _) => *x,
        }
    }
}

#[derive(Debug)]
struct Layout<K, V>
where
    K: Ord,
{
    active_memtable: Memtable<K, V>,
    ssts: Vec<String>,
}

impl<K, V> Layout<K, V>
where
    K: Ord + Default + Clone + std::fmt::Debug,
    V: Default + Clone + std::fmt::Debug,
{
    fn new(memtable: Memtable<K, V>) -> Self {
        Layout {
            active_memtable: memtable,
            ssts: Vec::new(),
        }
    }

    fn flush_memtable(&mut self) {
        self.active_memtable = Memtable::new();
    }
}

struct Db<K, V, L>
where
    K: std::fmt::Debug + Ord + Clone,
    V: std::fmt::Debug + Clone,
    L: Logger<DBEntry<K, V>>,
{
    layout: Layout<K, V>,
    wal_set: LogSet<DBEntry<K, V>, L>,
    dir: String,
    next_seqnum: usize,
}

impl<K, V, L> Db<K, V, L>
where
    K: Ord + Default + Clone + std::fmt::Debug + Decode + Encode,
    V: Default + Clone + std::fmt::Debug + Decode + Encode,
    L: Logger<DBEntry<K, V>>,
{
    fn new(dir: String) -> Self {
        Self {
            layout: Layout::new(Memtable::new()),
            wal_set: LogSet::open_dir(dir.clone()),
            dir,
            next_seqnum: 0,
        }
    }

    fn insert(&mut self, k: K, v: V) {
        self.next_seqnum += 1;
        // TODO: this clone is probably not needed.
        self.wal_set
            .current()
            .write(DBEntry::Write(self.next_seqnum, k.clone(), v.clone()));
        self.layout.active_memtable.insert(self.next_seqnum, k, v);
    }

    fn delete(&mut self, k: K) {
        self.next_seqnum += 1;
        // TODO: this clone is probably not needed.
        self.wal_set
            .current()
            .write(DBEntry::Delete(self.next_seqnum, k.clone()));
        self.layout.active_memtable.delete(self.next_seqnum, k);
    }

    fn scan(&mut self) -> DbIterator<K, V, impl KVIter<K, V>>
    where
        K: 'static,
        V: 'static,
    {
        let tab = self.layout.active_memtable.scan();
        let sst_merge: MergingIter<SstReader<(K, usize), Option<V>>, _, _> = MergingIter::new(
            self.layout
                .ssts
                .iter()
                .map(|fname| SstReader::load(fname.as_str()).unwrap()),
        );
        let lhs: Box<dyn KVIter<(K, usize), Option<V>>> = Box::new(sst_merge);
        let merged = MergingIter::new([Box::new(tab), lhs]);
        let scan = SeqnumIter::new(self.next_seqnum, merged);
        self.next_seqnum += 1;
        DbIterator {
            iter: scan,
            _marker: PhantomData,
        }
    }

    fn flush_memtable(&mut self) -> anyhow::Result<String> {
        let scan = self.layout.active_memtable.scan();

        // TODO: use the real path join.
        // TODO: include the lower bound?
        let sst_fname = format!("{}/sst{}.sst", self.dir, self.next_seqnum);
        let writer = SstWriter::new(scan, &sst_fname);
        writer.write()?;

        self.layout.flush_memtable();
        self.layout.ssts.push(sst_fname.clone());

        Ok(sst_fname)
    }
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeMap, rc::Rc};

    use rand::Rng;

    use crate::{
        db::DBEntry,
        log::MockLog,
        memtable::{KVIter, VecIter},
        sst::{reader::SstReader, writer::SstWriter},
    };

    use super::Db;

    #[test]
    fn random_inserts() {
        let mut map = BTreeMap::new();
        let mut db: Db<String, String, MockLog<DBEntry<String, String>>> =
            Db::new("db_data/".to_owned());

        let mut rng = rand::thread_rng();

        for i in 0..1000 {
            let val: usize = rng.gen_range(0..100);
            let key = format!("key{}", val);
            let value = format!("value{}", i);
            db.insert(key.clone(), value.clone());
            map.insert(key, value);
            if rng.gen_range(0_usize..100) == 0 {
                db.flush_memtable().unwrap();
            }
        }

        let db_data: Vec<_> = db.scan().collect();
        let iter_data: Vec<_> = map.into_iter().collect();

        assert_eq!(db_data, iter_data);
    }

    #[test]
    fn test_sst_iter() {
        datadriven::walk("src/sst/testdata/", |f| {
            let mut data = Vec::new();
            let mut reader: Option<SstReader<(String, usize), Option<String>>> = None;
            f.run(|test_case| match test_case.directive.as_str() {
                "insert" => {
                    for line in test_case.input.lines() {
                        let eq_idx = line.find('=').unwrap();
                        let at_idx = line.find('@').unwrap();
                        let key = line[0..eq_idx].to_owned();
                        let val = line[eq_idx + 1..at_idx].to_owned();
                        let seqnum: usize = line[at_idx + 1..].parse().unwrap();
                        if val == "<DELETE>" {
                            data.push(((key, seqnum), None));
                        } else {
                            data.push(((key, seqnum), Some(val)));
                        }
                    }
                    data.sort();
                    "ok\n".into()
                }
                "flush" => {
                    let sst_fname = "/tmp/test_sst.sst";
                    let writer = SstWriter::new(VecIter::new(Rc::new(data.clone())), sst_fname);
                    writer.write().unwrap();
                    reader = Some(SstReader::load(sst_fname).unwrap());
                    "ok\n".into()
                }
                "scan" => {
                    let mut out = String::new();
                    for command in test_case.input.trim().chars() {
                        match command {
                            '>' => match reader.as_mut().unwrap().next() {
                                None => {
                                    out.push_str("> eof");
                                }
                                Some((k, v)) => {
                                    out.push_str(&format!("> {:?}={:?}", k, v));
                                }
                            },
                            ')' => match reader.as_mut().unwrap().peek() {
                                None => {
                                    out.push_str(") eof");
                                }
                                Some((k, v)) => {
                                    out.push_str(&format!(") {:?}={:?}", k, v));
                                }
                            },
                            '<' => match reader.as_mut().unwrap().prev() {
                                None => {
                                    out.push_str("< eof");
                                }
                                Some((k, v)) => {
                                    out.push_str(&format!("< {:?}={:?}", k, v));
                                }
                            },
                            '(' => match reader.as_mut().unwrap().peek_prev() {
                                None => {
                                    out.push_str("( eof");
                                }
                                Some((k, v)) => {
                                    out.push_str(&format!("( {:?}={:?}", k, v));
                                }
                            },

                            _ => panic!("unhandled: {}", command),
                        }
                        out.push('\n');
                    }
                    out
                }
                // TODO: oh, we'll get to you.
                // "seek-ge" => {
                //     let key = test_case
                //         .args
                //         .get("key")
                //         .expect("seek-ge requires key argument")
                //         .get(0)
                //         .unwrap();
                //     iter.as_mut().unwrap().seek_ge(key);
                //     "ok\n".into()
                // }
                _ => {
                    panic!("unhandled");
                }
            })
        })
    }

    #[test]
    fn insert() {
        let mut db: Db<String, String, MockLog<DBEntry<String, String>>> =
            Db::new("db_data/".to_owned());
        for i in 0..10 {
            db.insert(format!("sstkey{}", i), format!("bar{}", i));
        }

        let _fname = db.flush_memtable().unwrap();

        for i in 10..20 {
            db.insert(format!("memkey{}", i), format!("bar{}", i));
        }

        let iter = db.scan();

        for k in iter {
            println!("{:?}", k);
        }
    }
}
