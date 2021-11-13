use std::marker::PhantomData;

use crate::memtable::{KVIter, Memtable, MergingIter, SeqnumIter, VecIter};

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

struct Db<K, V>
where
    K: Ord,
{
    memtable: Memtable<K, V>,
    next_seqnum: usize,
}

impl<K, V> Db<K, V>
where
    K: Ord + Default + Clone + std::fmt::Debug,
    V: Default + Clone + std::fmt::Debug,
{
    fn new() -> Self {
        Self {
            memtable: Memtable::new(),
            next_seqnum: 0,
        }
    }

    fn insert(&mut self, k: K, v: V) {
        self.next_seqnum += 1;
        self.memtable.insert(self.next_seqnum, k, v);
    }

    fn scan(&mut self) -> DbIterator<K, V> {
        self.next_seqnum += 1;
        DbIterator {
            iter: self.memtable.read_at(self.next_seqnum),
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod test {
    use super::Db;

    #[test]
    fn insert() {
        let mut db: Db<String, String> = Db::new();
        db.insert("foo".into(), "bar1".into());
        db.insert("foo".into(), "bar2".into());
        let iter1 = db.scan();
        db.insert("foo".into(), "bar3".into());

        let iter2 = db.scan();

        println!("x = {:?}", iter1.collect::<Vec<_>>());
        println!("x = {:?}", iter2.collect::<Vec<_>>());
    }
}
