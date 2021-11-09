#![allow(dead_code)]

use std::{marker::PhantomData, rc::Rc};

trait KVIter<K, V> {
    fn next(&mut self) -> Option<(&K, &V)>;
    fn peek(&mut self) -> Option<(&K, &V)>;
    fn prev(&mut self) -> Option<(&K, &V)>;
    fn peek_prev(&mut self) -> Option<(&K, &V)>;

    // Positions the iterator to the left of the first location the key is >=
    // to.
    fn seek_ge(&mut self, key: &K);
}

// There is a matrix of four states we can be in:

// Buffer is currently filled to the left, vs. right
// times
// Whether the logical position is behind or equal to the physical position.

#[derive(Clone, Copy)]
enum LogPhysState {
    FwdEq,
    FwdBehind,
    RevEq,
    RevBehind,
}

struct SeqnumIter<I, K, V>
where
    I: KVIter<(K, usize), V>,
{
    iter: I,
    seqnum: usize,
    state: LogPhysState,
    buf: (K, V),
}

impl<I, K, V> SeqnumIter<I, K, V>
where
    K: Default + Eq + Ord + Clone,
    V: Default + Clone,
    I: KVIter<(K, usize), V>,
{
    fn new(seqnum: usize, iter: I) -> Self {
        SeqnumIter {
            state: LogPhysState::FwdEq,
            iter,
            seqnum,
            buf: <(K, V)>::default(),
        }
    }

    fn physical_forwards(&mut self) -> Option<()> {
        let (mut ks, mut v) = self.iter.next()?;
        while ks.1 > self.seqnum {
            let (nks, nv) = self.iter.next()?;
            ks = nks;
            v = nv;
        }
        self.buf.0.clone_from(&(*ks).0);
        self.buf.1.clone_from(v);

        while let Some((nks, nv)) = self.iter.peek() {
            if nks.0 != self.buf.0 {
                break;
            }
            if nks.1 <= self.seqnum {
                self.buf.1.clone_from(nv);
            }
            self.iter.next();
        }

        Some(())
    }

    fn physical_reverse(&mut self) -> Option<()> {
        let (mut ks, mut v) = self.iter.prev()?;
        while ks.1 > self.seqnum {
            let (nks, nv) = self.iter.prev()?;
            ks = nks;
            v = nv;
        }
        self.buf.0.clone_from(&(*ks).0);
        self.buf.1.clone_from(v);

        while let Some((nks, _)) = self.iter.peek_prev() {
            if nks.0 != self.buf.0 {
                break;
            }
            self.iter.prev();
        }

        Some(())
    }
}

impl<I, K, V> KVIter<K, V> for SeqnumIter<I, K, V>
where
    K: Default + Eq + Ord + Clone,
    V: Default + Clone,
    I: KVIter<(K, usize), V>,
{
    fn next(&mut self) -> Option<(&K, &V)> {
        match self.state {
            LogPhysState::FwdEq => {
                self.physical_forwards()?;
            }
            LogPhysState::FwdBehind => {
                self.state = LogPhysState::FwdEq;
            }
            LogPhysState::RevEq => {
                self.state = LogPhysState::RevBehind;
            }
            LogPhysState::RevBehind => {
                self.physical_forwards()?;
                self.physical_forwards()?;
                self.state = LogPhysState::FwdEq;
            }
        };
        Some((&self.buf.0, &self.buf.1))
    }

    fn peek(&mut self) -> Option<(&K, &V)> {
        let state = self.state;
        match state {
            LogPhysState::FwdEq => {
                self.physical_forwards()?;
                self.state = LogPhysState::FwdBehind;
            }
            LogPhysState::FwdBehind => (),
            LogPhysState::RevEq => (),
            LogPhysState::RevBehind => {
                self.physical_forwards()?;
                self.physical_forwards()?;
                self.state = LogPhysState::FwdBehind;
            }
        };
        Some((&self.buf.0, &self.buf.1))
    }

    fn prev(&mut self) -> Option<(&K, &V)> {
        match self.state {
            LogPhysState::FwdEq => {
                self.state = LogPhysState::FwdBehind;
            }
            LogPhysState::FwdBehind => {
                self.state = LogPhysState::RevEq;
                self.physical_reverse()?;
                self.physical_reverse()?;
            }
            LogPhysState::RevEq => {
                self.physical_reverse()?;
            }
            LogPhysState::RevBehind => {
                self.state = LogPhysState::RevEq;
            }
        };
        Some((&self.buf.0, &self.buf.1))
    }

    fn peek_prev(&mut self) -> Option<(&K, &V)> {
        match self.state {
            LogPhysState::FwdBehind => {
                self.physical_reverse()?;
                self.physical_reverse()?;
                self.state = LogPhysState::RevBehind;
            }
            LogPhysState::FwdEq => (),
            LogPhysState::RevBehind => (),
            LogPhysState::RevEq => {
                self.physical_reverse()?;
                self.state = LogPhysState::RevBehind;
            }
        };
        Some((&self.buf.0, &self.buf.1))
    }

    fn seek_ge(&mut self, key: &K) {
        // TODO: we should use a buffer to clone_into the key here.
        self.iter.seek_ge(&(key.clone(), 0));
        self.physical_forwards();
        self.state = LogPhysState::FwdBehind;
    }
}

#[derive(Debug)]
struct VecIter<K, V> {
    idx: usize,
    contents: Rc<Vec<(K, V)>>,
}

impl<K, V> VecIter<K, V> {
    fn new(v: Rc<Vec<(K, V)>>) -> Self {
        Self {
            idx: 0,
            contents: v,
        }
    }
}

impl<K, V> KVIter<K, V> for VecIter<K, V>
where
    K: Ord,
{
    fn next(&mut self) -> Option<(&K, &V)> {
        if self.idx >= self.contents.len() {
            None
        } else {
            self.idx += 1;
            let v = &self.contents[self.idx - 1];
            Some((&v.0, &v.1))
        }
    }

    fn peek(&mut self) -> Option<(&K, &V)> {
        if self.idx >= self.contents.len() {
            None
        } else {
            let v = &self.contents[self.idx];
            Some((&v.0, &v.1))
        }
    }

    fn prev(&mut self) -> Option<(&K, &V)> {
        if self.idx == 0 {
            None
        } else {
            self.idx -= 1;
            let v = &self.contents[self.idx];
            Some((&v.0, &v.1))
        }
    }

    fn peek_prev(&mut self) -> Option<(&K, &V)> {
        if self.idx == 0 {
            None
        } else {
            let v = &self.contents[self.idx - 1];
            Some((&v.0, &v.1))
        }
    }

    fn seek_ge(&mut self, key: &K) {
        let idx = match self.contents.binary_search_by_key(&key, |(k, _)| k) {
            Ok(x) => x,
            Err(x) => x,
        };
        self.idx = idx;
    }
}

#[test]
fn test_seqnum_iter() {
    datadriven::walk("src/testdata/seqnum", |f| {
        let mut iter = None;
        let mut data = Vec::new();
        f.run(|test_case| match test_case.directive.as_str() {
            "insert" => {
                for line in test_case.input.lines() {
                    let eq_idx = line.find('=').unwrap();
                    let at_idx = line.find('@').unwrap();
                    let key = line[0..eq_idx].to_owned();
                    let val = line[eq_idx + 1..at_idx].to_owned();
                    let seqnum: usize = line[at_idx + 1..].parse().unwrap();
                    data.push(((key, seqnum), val));
                }
                data.sort();
                "ok\n".into()
            }
            "read" => {
                let ts = test_case
                    .args
                    .get("ts")
                    .expect("read requires ts argument")
                    .get(0)
                    .unwrap()
                    .parse()
                    .unwrap();
                iter = Some(SeqnumIter::new(ts, VecIter::new(Rc::new(data.clone()))));
                "ok\n".into()
            }
            "scan" => {
                let mut out = String::new();
                for command in test_case.input.trim().chars() {
                    match command {
                        '>' => match iter.as_mut().unwrap().next() {
                            None => {
                                out.push_str("> eof\n");
                            }
                            Some((k, v)) => {
                                out.push_str(&format!("> {}={}\n", k, v));
                            }
                        },
                        ')' => match iter.as_mut().unwrap().peek() {
                            None => {
                                out.push_str(") eof\n");
                            }
                            Some((k, v)) => {
                                out.push_str(&format!(") {}={}\n", k, v));
                            }
                        },
                        '<' => match iter.as_mut().unwrap().prev() {
                            None => {
                                out.push_str("< eof\n");
                            }
                            Some((k, v)) => {
                                out.push_str(&format!("< {}={}\n", k, v));
                            }
                        },
                        '(' => match iter.as_mut().unwrap().peek_prev() {
                            None => {
                                out.push_str("( eof\n");
                            }
                            Some((k, v)) => {
                                out.push_str(&format!("( {}={}\n", k, v));
                            }
                        },

                        _ => panic!("unhandled: {}", command),
                    }
                }
                out
            }
            "seek-ge" => {
                let key = test_case
                    .args
                    .get("key")
                    .expect("seek-ge requires key argument")
                    .get(0)
                    .unwrap();
                iter.as_mut().unwrap().seek_ge(key);
                "ok\n".into()
            }
            _ => {
                panic!("unhandled");
            }
        })
    })
}

struct MergingIter<I, K, V>
where
    I: KVIter<K, V>,
{
    iters: Vec<I>,
    _marker: PhantomData<(K, V)>,
}

impl<I, K, V> MergingIter<I, K, V>
where
    K: Ord,
    I: KVIter<K, V>,
{
    fn new<J>(j: J) -> Self
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
                    lowest = it.peek().map(|kv| (idx, kv));
                }
                Some((_, (k, v))) => {
                    if let Some((k2, _)) = it.peek() {
                        if k2 < k {
                            lowest = Some((idx, (k, v)));
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
                    highest = it.peek_prev().map(|kv| (idx, kv));
                }
                Some((_, (k, v))) => {
                    if let Some((k2, _)) = it.peek_prev() {
                        if k2 > k {
                            highest = Some((idx, (k, v)));
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
    K: Ord,
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
        self.iters[highest].peek()
    }

    fn prev(&mut self) -> Option<(&K, &V)> {
        let i = self.highest()?;
        self.iters[i].prev()
    }

    fn seek_ge(&mut self, key: &K) {
        for it in self.iters.iter_mut() {
            it.seek_ge(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use crate::{KVIter, MergingIter, SeqnumIter, VecIter};

    #[test]
    fn it_works() {
        let mut t = SeqnumIter::new(
            4,
            MergingIter::new(vec![
                VecIter::new(Rc::new(vec![
                    ((1, 0), 1),
                    ((2, 1), 2),
                    ((3, 2), 3),
                    ((5, 10), 5),
                    ((8, 3), 4),
                ])),
                VecIter::new(Rc::new(vec![
                    ((4, 2), 3),
                    ((6, 0), 1),
                    ((7, 1), 2),
                    ((9, 3), 4),
                    ((10, 4), 5),
                ])),
            ]),
        );
    }
}

#[derive(Debug)]
struct Memtable<K, V>
where
    K: Ord,
{
    prev_seqnum: usize,
    entries: Vec<Rc<Vec<((K, usize), V)>>>,
}

impl<K, V> Memtable<K, V>
where
    K: Default + Ord + Clone + std::fmt::Debug,
    V: Default + Clone + std::fmt::Debug,
{
    pub fn new() -> Self {
        Memtable {
            prev_seqnum: 0,
            entries: Vec::new(),
        }
    }

    // TODO: replace this with an iterator.
    fn merge(
        lhs: Rc<Vec<((K, usize), V)>>,
        rhs: Rc<Vec<((K, usize), V)>>,
    ) -> Rc<Vec<((K, usize), V)>> {
        let mut out = Vec::new();
        let mut lhs = (*lhs).iter();
        let mut rhs = (*rhs).iter();
        let mut left = lhs.next();
        let mut right = rhs.next();
        loop {
            match (left, right) {
                (None, None) => {
                    break;
                }
                (Some(l), None) => {
                    out.push(l.clone());
                    out.extend(lhs.cloned());
                    break;
                }
                (None, Some(r)) => {
                    out.push(r.clone());
                    out.extend(rhs.cloned());
                    break;
                }
                (Some(((k1, s1), v1)), Some(((k2, s2), v2))) => {
                    if (k1, s1) < (k2, s2) {
                        out.push(((k1.clone(), *s1), v1.clone()));
                        left = lhs.next();
                    } else {
                        // In this case, k2 must be < k1, because by
                        // construction a seqnum in a more-right slab must be
                        // greater than any in a more-left slab.
                        out.push(((k2.clone(), *s2), v2.clone()));
                        right = rhs.next();
                    }
                }
            }
        }

        Rc::new(out)
    }

    fn maybe_fix_at(&mut self, idx: usize) {
        if self.entries[idx].len() < self.entries[idx + 1].len() * 2 {
            let lhs = self.entries[idx].clone();
            let rhs = self.entries[idx + 1].clone();
            let merged = Self::merge(lhs, rhs);
            self.entries
                .splice(idx..idx + 2, vec![merged])
                .for_each(drop);
        }
    }

    pub fn insert(&mut self, s: usize, k: K, v: V) {
        if s <= self.prev_seqnum {
            panic!("seqnums must be strictly increasing")
        }
        self.prev_seqnum = s;
        self.entries.push(Rc::new(vec![((k, s), v)]));
        for i in (0..(self.entries.len() - 1)).rev() {
            self.maybe_fix_at(i);
        }
    }

    pub fn read_at(&self, seqnum: usize) -> impl KVIter<K, V> {
        SeqnumIter::new(
            seqnum,
            MergingIter::new(self.entries.iter().map(|e| VecIter::new(e.clone()))),
        )
    }
}
