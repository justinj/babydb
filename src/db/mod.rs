#![allow(dead_code)]
use anyhow::bail;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::{
    encoding::{Decode, Encode},
    fs::DbDir,
    log::{
        file_log::{Log, LogReader},
        LogEntry,
    },
    memtable::{KVIter, Memtable, MergingIter, SeqnumIter},
    root::Root,
    sst::{reader::SstReader, writer::SstWriter},
};

use self::level_iter::LevelIter;

mod level_iter;

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
    l0: Vec<String>,
    ssts: Vec<Vec<String>>,
    wals: Vec<String>,
}

impl DiskLayout {
    fn new() -> Self {
        DiskLayout {
            l0: Vec::new(),
            ssts: Vec::new(),
            wals: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct Sst {
    filename: String,
}

#[derive(Debug)]
struct Layout<K, V>
where
    K: Ord,
{
    active_memtable: Memtable<K, V>,
    l0: Vec<Sst>,
    ssts: Vec<Vec<Sst>>,
}

impl<K, V> Layout<K, V>
where
    K: Ord + Default + Clone + std::fmt::Debug + Encode + Decode,
    V: Default + Clone + std::fmt::Debug + Encode + Decode,
{
    fn new(memtable: Memtable<K, V>, l0: Vec<Sst>, ssts: Vec<Vec<Sst>>) -> Self {
        Layout {
            active_memtable: memtable,
            l0,
            ssts,
        }
    }

    fn flush_memtable(&mut self) {
        self.active_memtable = Memtable::new();
    }
}

struct Db<D, K, V>
where
    D: DbDir,
    K: std::fmt::Debug + Ord + Clone + Encode + Decode,
    V: std::fmt::Debug + Clone + Encode + Decode,
{
    root: Root<DiskLayout, D>,
    layout: Layout<K, V>,
    wal: Log<D, DBCommand<K, V>>,
    dir: D,
    next_seqnum: usize,
    // The seqnum that is used for reads.
    visible_seqnum: AtomicUsize,
}

impl<D, K, V> Db<D, K, V>
where
    D: DbDir + std::fmt::Debug + 'static,
    K: Ord + Default + Clone + std::fmt::Debug + Decode + Encode,
    V: Default + Clone + std::fmt::Debug + Decode + Encode,
{
    fn new(mut dir: D) -> anyhow::Result<Self> {
        let mut root: Root<DiskLayout, _> = Root::load(dir.clone())?;
        let mut memtable = Memtable::new();
        let mut next_seqnum = 0;
        // Compute the seqnum we are to start at. It's the max of the seqnums provided by every data source.

        let mut empty_wals = HashSet::new();
        for wal_name in root.data.wals.iter().rev() {
            let wal = dir.open(wal_name).unwrap();
            let mut any = false;
            for command in LogReader::<_, DBCommand<K, V>>::new(wal)? {
                any = true;
                next_seqnum = std::cmp::max(command.seqnum() + 1, next_seqnum);
                memtable.apply_command(command)
            }
            if !any {
                empty_wals.insert(wal_name.clone());
            }
        }

        // TODO: we should probably just declare that if we are attempting to
        // create a WAL, if one already exists with that name, we can safely
        // delete it (I believe this is true because it means that the given WAL
        // had to be empty, or else it would have contained commands that bumped
        // the seqnum).
        if !empty_wals.is_empty() {
            // If a given WAL has no commands in it, then unlink it and remove it
            // from the set of WALs.
            root.transform(|mut r| {
                r.wals.retain(|w| !empty_wals.contains(w));
                r
            })?;
            for wal in empty_wals {
                dir.unlink(&wal);
            }
        }

        let l0 = root
            .data
            .l0
            .iter()
            .map(|filename| Sst {
                filename: filename.clone(),
            })
            .collect();
        let ssts: Vec<Vec<Sst>> = root
            .data
            .ssts
            .iter()
            .map(|level| {
                level
                    .iter()
                    .map(|filename| Sst {
                        filename: filename.clone(),
                    })
                    .collect()
            })
            .collect();

        for level in &ssts {
            for sst in level {
                // SST names are of the form "sst{}.sst", where {} is one greater
                // than the largest seqnum provided by that SST.
                // TODO: this should probably be stored in the SST itself (we can
                // just stuff it at the end like the other stuff).
                let largest_seqnum: usize = sst
                    .filename
                    .strip_prefix("sst")
                    .and_then(|s| s.strip_suffix(".sst"))
                    .expect("sst filename was not as expected")
                    .parse()
                    .unwrap();
                next_seqnum = std::cmp::max(largest_seqnum, next_seqnum);
            }
        }

        let wal = Log::new(dir.clone(), next_seqnum)?;

        // When we open we create a fresh WAL, so we need to add that to the root.
        let wal_name = wal.fname().to_owned();
        root.transform(move |mut layout| {
            layout.wals.push(wal_name);
            layout
        })?;

        Ok(Self {
            root,
            layout: Layout::new(memtable, l0, ssts),
            wal,
            dir,
            next_seqnum,
            visible_seqnum: AtomicUsize::new(next_seqnum),
        })
    }

    fn apply_command(&mut self, cmd: DBCommand<K, V>) {
        self.wal.write(&cmd).unwrap();
        self.apply_command_volatile(cmd);
    }

    fn apply_command_volatile(&mut self, cmd: DBCommand<K, V>) {
        self.layout.active_memtable.apply_command(cmd);
    }

    fn ratchet_visible_seqnum(&mut self, v: usize) {
        // TODO: understand these orderings better.
        loop {
            let cur_val = self.visible_seqnum.load(Ordering::SeqCst);
            if cur_val >= v {
                // Someone else might have ratcheted above us, which is fine.
                break;
            }
            match self.visible_seqnum.compare_exchange(
                cur_val,
                v,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_v) => {
                    // We did it!
                    break;
                }
                Err(_v) => {
                    // We were unsuccessful, so try again.
                }
            }
        }
    }

    fn insert(&mut self, k: K, v: V) {
        self.next_seqnum += 1;
        self.apply_command(DBCommand::Write(self.next_seqnum, k, v));
        self.ratchet_visible_seqnum(self.next_seqnum);
    }

    fn delete(&mut self, k: K) {
        self.next_seqnum += 1;
        self.apply_command(DBCommand::Delete(self.next_seqnum, k));
        self.ratchet_visible_seqnum(self.next_seqnum);
    }

    fn get(&mut self, k: &K) -> Option<V>
    where
        K: 'static,
        V: 'static,
    {
        let scan = self.scan();
        let mut iter = scan.iter;
        iter.seek_ge(k);
        let next = iter.next()?;
        if next.0 == k {
            Some(next.1.clone())
        } else {
            None
        }
    }

    fn scan(&mut self) -> DbIterator<K, V, impl KVIter<K, V>>
    where
        K: 'static,
        V: 'static,
    {
        let tab = self.layout.active_memtable.scan();

        // Every SST in L0 is read independently, but the lower-level ones get
        // concatenated.
        let mut level_readers = Vec::new();
        for sst in &self.layout.l0 {
            level_readers.push(LevelIter::new(SstReader::load(
                self.dir
                    .open(&sst.filename)
                    .expect("sst file did not exist"),
            )))
        }

        for level in &self.layout.ssts {
            let readers = level.iter().map(|sst| {
                SstReader::load(
                    self.dir
                        .open(&sst.filename)
                        .expect("sst file did not exist"),
                )
                .unwrap()
            });
            level_readers.push(LevelIter::new(readers))
        }

        let sst_merge: MergingIter<LevelIter<_, _, SstReader<(K, usize), Option<V>, D>>, _, _> =
            MergingIter::new(level_readers);

        let lhs: Box<dyn KVIter<(K, usize), Option<V>>> = Box::new(sst_merge);
        let merged = MergingIter::new([Box::new(tab), lhs]);
        let scan = SeqnumIter::new(self.visible_seqnum.load(Ordering::SeqCst), merged);
        DbIterator {
            iter: scan,
            _marker: PhantomData,
        }
    }

    fn flush_memtable(&mut self) -> anyhow::Result<String> {
        let scan = self.layout.active_memtable.scan();

        let sst_path = format!("sst{}.sst", self.next_seqnum);

        let sst_file = self
            .dir
            .create(&sst_path)
            .expect("sst file already existed");
        let writer = SstWriter::new(scan, sst_file);
        writer.write()?;

        self.layout.flush_memtable();
        // Add it to L0.
        self.layout.l0.push(Sst {
            filename: sst_path.clone(),
        });

        // TODO: include the lower bound?
        self.wal = Log::new(self.dir.clone(), self.next_seqnum)?;
        // When we open we create a fresh WAL, so we need to add that to the root.
        let wal_name = self.wal.fname().to_owned();
        let sst_name = sst_path.clone();
        self.root.transform(move |mut layout| {
            layout.wals = vec![wal_name];
            layout.l0.push(sst_name);
            layout
        })?;

        Ok(sst_path)
    }
}

#[cfg(test)]
mod test {

    use std::{collections::BTreeMap, fmt::Write, rc::Rc};

    use rand::Rng;

    use crate::{
        fs::{DbDir, MockDir},
        memtable::{KVIter, VecIter},
        sst::{reader::SstReader, writer::SstWriter},
    };

    use super::Db;

    #[test]
    // This is really slow.
    #[ignore]
    fn random_inserts() {
        let dir = MockDir::new();
        let mut map = BTreeMap::new();
        let mut db: Db<_, String, String> = Db::new(dir).unwrap();

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

    #[test]
    fn test_sst_iter() {
        datadriven::walk("src/sst/testdata/", |f| {
            let mut dir = MockDir::new();
            let mut data = Vec::new();
            let mut reader: Option<SstReader<(String, usize), Option<String>, MockDir>> = None;
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
                    let file = dir.create(&sst_fname).unwrap();
                    let writer = SstWriter::new(VecIter::new(Rc::new(data.clone())), file);
                    writer.write().unwrap();
                    reader = Some(SstReader::load(dir.open(&sst_fname).unwrap()).unwrap());
                    "ok\n".into()
                }
                "start" => {
                    reader.as_mut().unwrap().start();
                    "ok\n".into()
                }
                "end" => {
                    reader.as_mut().unwrap().end();
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
                            '?' => write!(&mut out, "? {}", reader.as_ref().unwrap().print_state())
                                .unwrap(),
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
        let dir = MockDir::new();
        let mut db: Db<_, String, String> = Db::new(dir).unwrap();
        for i in 0..10 {
            db.insert(format!("sstkey{}", i), format!("bar{}", i));
        }

        let _fname = db.flush_memtable().unwrap();

        for i in 10..20 {
            db.insert(format!("memkey{}", i), format!("bar{}", i));
        }

        let iter = db.scan();

        assert_eq!(
            iter.collect::<Vec<_>>(),
            [
                ("memkey10", "bar10"),
                ("memkey11", "bar11"),
                ("memkey12", "bar12"),
                ("memkey13", "bar13"),
                ("memkey14", "bar14"),
                ("memkey15", "bar15"),
                ("memkey16", "bar16"),
                ("memkey17", "bar17"),
                ("memkey18", "bar18"),
                ("memkey19", "bar19"),
                ("sstkey0", "bar0"),
                ("sstkey1", "bar1"),
                ("sstkey2", "bar2"),
                ("sstkey3", "bar3"),
                ("sstkey4", "bar4"),
                ("sstkey5", "bar5"),
                ("sstkey6", "bar6"),
                ("sstkey7", "bar7"),
                ("sstkey8", "bar8"),
                ("sstkey9", "bar9")
            ]
            .into_iter()
            .map(|(a, b)| (a.into(), b.into()))
            .collect::<Vec<_>>(),
        );
    }

    #[tokio::test]
    async fn test_recover() {
        let dir = MockDir::new();

        let mut db: Db<_, String, String> = Db::new(dir.clone()).unwrap();
        for i in 0..10 {
            db.insert(format!("sstkey{}", i), format!("bar{}", i));
        }

        let _fname = db.flush_memtable().unwrap();

        for i in 10..20 {
            db.insert(format!("memkey{}", i), format!("bar{}", i));
        }

        let prev_data: Vec<_> = db.scan().collect();

        let mut db: Db<_, String, String> = Db::new(dir).unwrap();

        let post_data: Vec<_> = db.scan().collect();

        assert_eq!(prev_data, post_data);
    }
}
