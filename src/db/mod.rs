#![allow(dead_code)]
use std::marker::PhantomData;

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

impl<K, V> DBEntry<K, V>
where
    K: std::fmt::Debug + Clone,
    V: std::fmt::Debug + Clone,
{
    fn take_write(self) -> Option<(usize, K, V)> {
        if let DBEntry::Write(x, k, v) = self {
            Some((x, k, v))
        } else {
            None
        }
    }

    fn take_delete(self) -> Option<(usize, K)> {
        if let DBEntry::Delete(x, k) = self {
            Some((x, k))
        } else {
            None
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
        let write_entry = DBEntry::Write(self.next_seqnum, k, v);
        self.wal_set.current().write(&write_entry);
        let (seqnum, k, v) = write_entry.take_write().unwrap();
        self.layout.active_memtable.insert(seqnum, k, v);
    }

    fn delete(&mut self, k: K) {
        self.next_seqnum += 1;
        // TODO: this clone is probably not needed.
        let delete = DBEntry::Delete(self.next_seqnum, k);
        self.wal_set.current().write(&delete);
        let (seqnum, k) = delete.take_delete().unwrap();
        self.layout.active_memtable.delete(seqnum, k);
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
                "seek-ge" => {
                    let key = test_case
                        .args
                        .get("key")
                        .expect("seek-ge requires key argument");
                    let k = (key[0].clone(), key[1].parse::<usize>().unwrap());
                    reader.as_mut().unwrap().seek_ge(&k);
                    format!("{:?}\n", reader.as_mut().unwrap().next())
                }
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
