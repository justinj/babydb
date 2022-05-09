use std::marker::PhantomData;

use crate::memtable::KVIter;

#[derive(Debug)]
pub struct MergingIter<I, K, V>
where
    K: Ord,
    I: KVIter<K, V>,
{
    iters: Vec<I>,
    _marker: PhantomData<(K, V)>,
}

impl<I, K, V> MergingIter<I, K, V>
where
    K: Ord + std::fmt::Debug,
    V: std::fmt::Debug,
    I: KVIter<K, V>,
{
    pub fn new<J>(j: J) -> Self
    where
        J: IntoIterator<Item = I>,
    {
        Self {
            iters: j.into_iter().collect(),
            _marker: PhantomData,
        }
    }

    fn lowest(&mut self) -> Option<usize> {
        let mut lowest = None;

        for (idx, it) in self.iters.iter_mut().enumerate() {
            match lowest {
                None => {
                    lowest = it.peek().map(|(k, _v)| (idx, k));
                }
                Some((_, k)) => {
                    if let Some((k2, _)) = it.peek() {
                        if k2 < k {
                            lowest = Some((idx, k2));
                        }
                    }
                }
            }
        }

        lowest.map(|(x, _)| x)
    }

    fn highest(&mut self) -> Option<usize> {
        let mut highest = None;

        for (idx, it) in self.iters.iter_mut().enumerate() {
            match highest {
                None => {
                    highest = it.peek_prev().map(|(k, _v)| (idx, k));
                }
                Some((_, k)) => {
                    if let Some((k2, _)) = it.peek_prev() {
                        if k2 > k {
                            highest = Some((idx, k2));
                        }
                    }
                }
            }
        }

        highest.map(|(x, _)| x)
    }
}

impl<I, K, V> KVIter<K, V> for MergingIter<I, K, V>
where
    I: KVIter<K, V>,
    K: Ord + std::fmt::Debug,
    V: std::fmt::Debug,
{
    fn peek(&mut self) -> Option<(&K, &V)> {
        let lowest = self.lowest()?;
        self.iters[lowest].peek()
    }

    fn next(&mut self) -> Option<(&K, &V)> {
        let i = self.lowest()?;
        self.iters[i].next()
    }

    fn peek_prev(&mut self) -> Option<(&K, &V)> {
        let highest = self.highest()?;
        self.iters[highest].peek_prev()
    }

    fn prev(&mut self) -> Option<(&K, &V)> {
        let i = self.highest()?;
        self.iters[i].prev()
    }

    fn seek_ge(&mut self, key: &K) {
        // TODO: in the case where we're just searching for a single key _ever_,
        // we can optimize this by not looking at the lower levels.
        for it in self.iters.iter_mut() {
            it.seek_ge(key);
        }
    }

    fn start(&mut self) {
        for it in self.iters.iter_mut() {
            it.start();
        }
    }

    fn end(&mut self) {
        for it in self.iters.iter_mut() {
            it.end();
        }
    }
}
