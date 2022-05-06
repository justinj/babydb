use std::marker::PhantomData;

use crate::memtable::KVIter;

#[derive(Debug)]
pub struct LevelIter<K, V, I>
where
    K: Ord,
    I: KVIter<K, V>,
{
    iters: Vec<I>,
    idx: usize,
    index: Vec<(K, usize)>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V, I> LevelIter<K, V, I>
where
    K: Clone + Ord,
    I: KVIter<K, V>,
{
    pub fn new<C: IntoIterator<Item = I>>(c: C) -> Self {
        let mut index: Vec<(K, usize)> = Vec::new();
        let mut iters = Vec::new();
        for (i, mut v) in c.into_iter().enumerate() {
            if let Some(entry) = v.peek().map(|(k, _v)| (k.clone(), i)) {
                index.push(entry);
            }

            iters.push(v);
        }

        assert!(!iters.is_empty());

        LevelIter {
            iters,
            index,
            idx: 0,
            _marker: PhantomData,
        }
    }
}

impl<K, V, I> KVIter<K, V> for LevelIter<K, V, I>
where
    K: Ord + std::fmt::Debug,
    I: KVIter<K, V>,
{
    fn next(&mut self) -> Option<(&K, &V)> {
        while self.idx < self.iters.len() - 1 && self.iters[self.idx].peek().is_none() {
            self.idx += 1;
            self.iters[self.idx].start();
        }
        self.iters[self.idx].next()
    }

    fn peek(&mut self) -> Option<(&K, &V)> {
        while self.idx < self.iters.len() - 1 && self.iters[self.idx].peek().is_none() {
            self.idx += 1;
            self.iters[self.idx].start();
        }
        self.iters[self.idx].peek()
    }

    fn prev(&mut self) -> Option<(&K, &V)> {
        while self.idx > 0 && self.iters[self.idx].peek_prev().is_none() {
            self.idx += 1;
            self.iters[self.idx].end();
        }
        self.iters[self.idx].prev()
    }

    fn peek_prev(&mut self) -> Option<(&K, &V)> {
        while self.idx > 0 && self.iters[self.idx].peek_prev().is_none() {
            self.idx += 1;
            self.iters[self.idx].end();
        }
        self.iters[self.idx].peek_prev()
    }

    fn seek_ge(&mut self, key: &K) {
        self.idx = match self.index.binary_search_by_key(&key, |(k, _)| k) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        };

        self.iters[self.idx].seek_ge(key);
    }

    fn start(&mut self) {
        if !self.iters.is_empty() {
            self.idx = 0;
            self.iters[self.idx].start();
        }
    }

    fn end(&mut self) {
        if !self.iters.is_empty() {
            self.idx = self.iters.len() - 1;
            self.iters[self.idx].end();
        }
    }
}
