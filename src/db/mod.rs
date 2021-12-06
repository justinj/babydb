use std::{fs::File, marker::PhantomData};

use crate::{
    log::{LogEntry, LogSet, Logger},
    memtable::{KVIter, Memtable, MergingIter, SeqnumIter, VecIter},
    sst::{Encode, SstWriter},
};

struct DbIterator<K, V>
where
    K: Ord,
{
    iter: SeqnumIter<MergingIter<VecIter<(K, usize), Option<V>>, (K, usize), Option<V>>, K, V>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Iterator for DbIterator<K, V>
where
    K: Ord + Default + Clone,
    V: Default + Clone,
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

struct Db<K, V, L>
where
    K: std::fmt::Debug + Ord + Clone,
    V: std::fmt::Debug + Clone,
    L: Logger<DBEntry<K, V>>,
{
    memtable: Memtable<K, V>,
    wal_set: LogSet<DBEntry<K, V>, L>,
    dir: String,
    next_seqnum: usize,
}

impl<K, V, L> Db<K, V, L>
where
    K: Ord + Default + Clone + std::fmt::Debug + Encode,
    V: Default + Clone + std::fmt::Debug + Encode,
    L: Logger<DBEntry<K, V>>,
{
    fn new(dir: String) -> Self {
        Self {
            memtable: Memtable::new(),
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
        self.memtable.insert(self.next_seqnum, k, v);
    }

    fn delete(&mut self, k: K) {
        self.next_seqnum += 1;
        // TODO: this clone is probably not needed.
        self.wal_set
            .current()
            .write(DBEntry::Delete(self.next_seqnum, k.clone()));
        self.memtable.delete(self.next_seqnum, k);
    }

    fn scan(&mut self) -> DbIterator<K, V> {
        self.next_seqnum += 1;
        DbIterator {
            iter: self.memtable.read_at(self.next_seqnum),
            _marker: PhantomData,
        }
    }

    fn flush_memtable(&mut self) -> anyhow::Result<String> {
        let scan = self.memtable.scan();

        // TODO: use the real path join.
        // TODO: include the lower bound?
        let sst_fname = format!("{}/sst{}.sst", self.dir, self.next_seqnum);
        let writer = SstWriter::new(scan, &sst_fname);
        writer.write()?;
        Ok(sst_fname)
    }
}

#[cfg(test)]
mod test {
    use crate::{db::DBEntry, log::MockLog, sst::SstReader};

    use super::Db;

    #[test]
    fn insert() {
        let mut db: Db<String, String, MockLog<DBEntry<String, String>>> =
            Db::new("db_data/".to_owned());
        for i in 0..1000 {
            db.insert("foo".into(), format!("bar{}", i));
        }

        let fname = db.flush_memtable().unwrap();

        let mut reader: SstReader<(String, usize), Option<String>> =
            SstReader::load(fname.as_str()).unwrap();
        while let Some(v) = reader.next().unwrap() {
            println!("{:?}", v);
        }
    }
}
