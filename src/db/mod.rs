#![allow(dead_code)]
use anyhow::bail;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::{
    encoding::{Decode, Encode},
    log::{file_log::LogReader, LogEntry, LogSet, Logger},
    memtable::{KVIter, Memtable, MergingIter, SeqnumIter},
    root::Root,
    sst::{reader::SstReader, writer::SstWriter},
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
pub enum DBCommand<K, V>
where
    K: std::fmt::Debug + Encode,
    V: std::fmt::Debug + Encode,
{
    Write(usize, K, V),
    Delete(usize, K),
}

impl<K, V> Encode for DBCommand<K, V>
where
    K: Encode,
    V: Encode,
{
    fn write_bytes(&self, kw: &mut crate::encoding::KeyWriter) {
        match self {
            DBCommand::Write(seqnum, k, v) => {
                (0_u8, (seqnum, (k, v))).write_bytes(kw);
            }
            DBCommand::Delete(seqnum, k) => {
                (1_u8, (seqnum, k)).write_bytes(kw);
            }
        }
    }
}

impl<K, V> Decode for DBCommand<K, V>
where
    K: Encode + Decode,
    V: Encode + Decode,
{
    fn decode(kr: &mut crate::encoding::KeyReader) -> anyhow::Result<Self> {
        // TODO: does this break the abstraction? god, just use a real
        // serialization scheme.
        match kr.next()[0] {
            0 => {
                let (seqnum, (k, v)) = <(usize, (K, V))>::decode(kr)?;
                Ok(DBCommand::Write(seqnum, k, v))
            }
            1 => {
                let (seqnum, k) = <(usize, K)>::decode(kr)?;
                Ok(DBCommand::Delete(seqnum, k))
            }
            _ => bail!("invalid command"),
        }
    }
}

impl<K, V> LogEntry for DBCommand<K, V>
where
    K: std::fmt::Debug + Clone + Encode + Decode,
    V: std::fmt::Debug + Clone + Encode + Decode,
{
    fn seqnum(&self) -> usize {
        match self {
            DBCommand::Write(x, _, _) => *x,
            DBCommand::Delete(x, _) => *x,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct DiskLayout {
    ssts: Vec<String>,
    wals: Vec<String>,
}

impl DiskLayout {
    fn new() -> Self {
        DiskLayout {
            ssts: Vec::new(),
            wals: Vec::new(),
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
    K: Ord + Default + Clone + std::fmt::Debug + Encode + Decode,
    V: Default + Clone + std::fmt::Debug + Encode + Decode,
{
    fn new(memtable: Memtable<K, V>, ssts: Vec<String>) -> Self {
        Layout {
            active_memtable: memtable,
            ssts,
        }
    }

    fn flush_memtable(&mut self) {
        self.active_memtable = Memtable::new();
    }
}

struct Db<K, V, L>
where
    K: std::fmt::Debug + Ord + Clone + Encode + Decode,
    V: std::fmt::Debug + Clone + Encode + Decode,
    L: Logger<DBCommand<K, V>>,
{
    root: Root<DiskLayout>,
    layout: Layout<K, V>,
    wal_set: LogSet<DBCommand<K, V>, L>,
    dir: String,
    next_seqnum: usize,
}

impl<K, V, L> Db<K, V, L>
where
    K: Ord + Default + Clone + std::fmt::Debug + Decode + Encode,
    V: Default + Clone + std::fmt::Debug + Decode + Encode,
    L: Logger<DBCommand<K, V>>,
{
    // TODO: This should be P: IntoPath like in fs.
    fn new(dir: String) -> anyhow::Result<Self> {
        let root: Root<DiskLayout> = Root::load(dir.clone())?;
        let mut memtable = Memtable::new();
        let mut next_seqnum = 0;
        for wal in root.data.wals.iter().rev() {
            for command in LogReader::<DBCommand<K, V>>::new(wal)? {
                next_seqnum = std::cmp::max(command.seqnum(), next_seqnum);
                memtable.apply_command(command)
            }
        }
        // so if we yield when we do a write, do we actually want to like, keep
        // track of the set of open writes, and keep minting new read iterators
        // just below the smallest one..? but later writes will block the
        // earliest one...
        let ssts = root.data.ssts.clone();
        Ok(Self {
            root,
            layout: Layout::new(memtable, ssts),
            wal_set: LogSet::open_dir(dir.clone())?,
            dir,
            next_seqnum,
        })
    }

    fn apply_command(&mut self, cmd: DBCommand<K, V>) {
        self.wal_set.current().write(&cmd).unwrap();
        self.apply_command_volatile(cmd);
    }

    fn apply_command_volatile(&mut self, cmd: DBCommand<K, V>) {
        self.layout.active_memtable.apply_command(cmd);
    }

    fn insert(&mut self, k: K, v: V) {
        self.next_seqnum += 1;
        self.apply_command(DBCommand::Write(self.next_seqnum, k, v));
    }

    fn delete(&mut self, k: K) {
        self.next_seqnum += 1;
        self.apply_command(DBCommand::Delete(self.next_seqnum, k));
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
        self.wal_set.fresh()?;

        let sst_fname = format!("{}/sst{}.sst", self.dir, self.next_seqnum);
        let writer = SstWriter::new(scan, &sst_fname);
        writer.write()?;

        self.wal_set.remove_old();

        self.layout.flush_memtable();
        self.layout.ssts.push(sst_fname.clone());

        self.root.write(DiskLayout {
            ssts: self.layout.ssts.clone(),
            wals: self.wal_set.fnames(),
        })?;

        Ok(sst_fname)
    }
}

#[cfg(test)]
mod test {

    use std::{collections::BTreeMap, rc::Rc};

    use rand::Rng;

    use crate::{
        db::DBCommand,
        encoding::{Decode, Encode},
        log::{file_log::Log, Logger},
        memtable::{KVIter, VecIter},
        sst::{reader::SstReader, writer::SstWriter},
    };

    use super::Db;

    fn test_db<K, V, L>() -> anyhow::Result<Db<K, V, L>>
    where
        K: Ord + Clone + Encode + Decode + Default,
        V: Clone + Encode + Default + Encode + Decode,
        L: Logger<DBCommand<K, V>>,
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().to_str().unwrap().to_owned();
        Db::new(path)
    }

    #[test]
    fn random_inserts() {
        let dir = tempfile::tempdir().unwrap();
        let mut map = BTreeMap::new();
        let mut db: Db<String, String, Log<DBCommand<String, String>>> =
            Db::new(dir.path().to_str().unwrap().to_owned()).unwrap();

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
    fn test_insert() {
        let dir = tempfile::tempdir().unwrap();
        let mut db: Db<String, String, Log<DBCommand<String, String>>> =
            Db::new(dir.path().to_str().unwrap().to_owned()).unwrap();
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

    #[test]
    fn test_recover() {
        let dir = tempfile::tempdir().unwrap();
        let mut db: Db<String, String, Log<DBCommand<String, String>>> =
            Db::new(dir.path().to_str().unwrap().to_owned()).unwrap();
        for i in 0..10 {
            db.insert(format!("sstkey{}", i), format!("bar{}", i));
        }

        let _fname = db.flush_memtable().unwrap();

        for i in 10..20 {
            db.insert(format!("memkey{}", i), format!("bar{}", i));
        }

        let prev_data: Vec<_> = db.scan().collect();

        drop(db);

        let mut db: Db<String, String, Log<DBCommand<String, String>>> =
            Db::new(dir.path().to_str().unwrap().to_owned()).unwrap();

        let post_data: Vec<_> = db.scan().collect();

        assert_eq!(prev_data, post_data);
    }
}
