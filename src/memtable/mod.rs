#![allow(dead_code)]

use crate::db::DBCommand;
use crate::encoding::{Decode, Encode};
use std::{marker::PhantomData, rc::Rc};

pub trait KVIter<K, V>
where
    K: Ord,
{
    fn next(&mut self) -> Option<(&K, &V)>;
    fn peek(&mut self) -> Option<(&K, &V)>;
    fn prev(&mut self) -> Option<(&K, &V)>;
    fn peek_prev(&mut self) -> Option<(&K, &V)>;

    // Positions the iterator to the left of the first location the key is >=
    // to.
    fn seek_ge(&mut self, key: &K);

    fn start(&mut self);
    fn end(&mut self);
}

impl<K, V, T: KVIter<K, V> + ?Sized> KVIter<K, V> for Box<T>
where
    K: Ord,
{
    fn next(&mut self) -> Option<(&K, &V)> {
        (**self).next()
    }
    fn prev(&mut self) -> Option<(&K, &V)> {
        (**self).prev()
    }
    fn peek(&mut self) -> Option<(&K, &V)> {
        (**self).peek()
    }
    fn peek_prev(&mut self) -> Option<(&K, &V)> {
        (**self).peek_prev()
    }
    fn seek_ge(&mut self, key: &K) {
        (**self).seek_ge(key)
    }
    fn start(&mut self) {
        (**self).start()
    }
    fn end(&mut self) {
        (**self).end()
    }
}

#[derive(Clone, Copy, Debug)]
enum PhysicalState {
    FwdEq,
    FwdBehind,
    RevEq,
    RevBehind,
    AtStart,
    AtEnd,
}

pub struct SeqnumIter<I, K, V>
where
    K: Ord,
    I: KVIter<(K, usize), Option<V>>,
{
    iter: I,
    seqnum: usize,
    state: PhysicalState,
    buf: (K, V),
}

impl<I, K, V> SeqnumIter<I, K, V>
where
    K: Default + Eq + Ord + Clone + std::fmt::Debug,
    V: Default + Clone + std::fmt::Debug,
    I: KVIter<(K, usize), Option<V>>,
{
    pub fn new(seqnum: usize, iter: I) -> Self {
        SeqnumIter {
            state: PhysicalState::AtStart,
            iter,
            seqnum,
            buf: <(K, V)>::default(),
        }
    }

    fn physical_forwards(&mut self) -> bool {
        let mut valid = false;
        while !valid {
            let (mut ks, mut v) = match self.iter.next() {
                Some((k, v)) => (k, v),
                None => {
                    return false;
                }
            };
            while ks.1 > self.seqnum {
                let (nks, nv) = match self.iter.next() {
                    Some((k, v)) => (k, v),
                    None => {
                        return false;
                    }
                };
                ks = nks;
                v = nv;
            }
            if let Some(v) = v {
                self.buf.0.clone_from(&(*ks).0);
                self.buf.1.clone_from(v);
                valid = true;
            }

            while let Some((nks, nv)) = self.iter.peek() {
                if nks.0 != self.buf.0 {
                    break;
                }
                if nks.1 <= self.seqnum {
                    if let Some(v) = nv {
                        self.buf.1.clone_from(v);
                        valid = true;
                    } else {
                        valid = false;
                    }
                }
                self.iter.next();
            }
        }

        true
    }

    fn physical_reverse(&mut self) -> bool {
        let mut valid = false;
        while !valid {
            let (mut ks, mut v) = match self.iter.prev() {
                Some((k, v)) => (k, v),
                None => {
                    return false;
                }
            };
            while ks.1 > self.seqnum {
                let (nks, nv) = match self.iter.prev() {
                    Some((k, v)) => (k, v),
                    None => {
                        return false;
                    }
                };
                ks = nks;
                v = nv;
            }
            self.buf.0.clone_from(&(*ks).0);
            if let Some(v) = v {
                self.buf.1.clone_from(v);
                valid = true;
            }

            while let Some((nks, _)) = self.iter.peek_prev() {
                if nks.0 != self.buf.0 {
                    break;
                }
                self.iter.prev();
            }
        }

        true
    }
}

impl<I, K, V> KVIter<K, V> for SeqnumIter<I, K, V>
where
    K: Default + Eq + Ord + Clone + std::fmt::Debug,
    V: Default + Clone + std::fmt::Debug,
    I: KVIter<(K, usize), Option<V>>,
{
    fn next(&mut self) -> Option<(&K, &V)> {
        match self.state {
            PhysicalState::AtEnd => {
                return None;
            }
            PhysicalState::RevEq => {
                self.state = PhysicalState::RevBehind;
            }
            PhysicalState::RevBehind => {
                if self.physical_forwards() && self.physical_forwards() {
                    self.state = PhysicalState::FwdEq;
                } else {
                    self.state = PhysicalState::AtEnd;
                    return None;
                }
            }
            PhysicalState::AtStart | PhysicalState::FwdEq => {
                if self.physical_forwards() {
                    self.state = PhysicalState::FwdEq;
                } else {
                    self.state = PhysicalState::AtEnd;
                    return None;
                }
            }
            PhysicalState::FwdBehind => {
                self.state = PhysicalState::FwdEq;
            }
        };
        Some((&self.buf.0, &self.buf.1))
    }

    fn prev(&mut self) -> Option<(&K, &V)> {
        match self.state {
            PhysicalState::AtStart => {
                return None;
            }
            PhysicalState::FwdEq => {
                self.state = PhysicalState::FwdBehind;
            }
            PhysicalState::FwdBehind => {
                if self.physical_reverse() && self.physical_reverse() {
                    self.state = PhysicalState::RevEq;
                } else {
                    self.state = PhysicalState::AtStart;
                    return None;
                }
            }
            PhysicalState::AtEnd | PhysicalState::RevEq => {
                if self.physical_reverse() {
                    self.state = PhysicalState::RevEq;
                } else {
                    self.state = PhysicalState::AtStart;
                    return None;
                }
            }
            PhysicalState::RevBehind => {
                self.state = PhysicalState::RevEq;
            }
        };
        Some((&self.buf.0, &self.buf.1))
    }

    fn peek(&mut self) -> Option<(&K, &V)> {
        let state = self.state;
        match state {
            PhysicalState::AtEnd => {
                return None;
            }
            PhysicalState::AtStart | PhysicalState::FwdEq => {
                if self.physical_forwards() {
                    self.state = PhysicalState::FwdBehind;
                } else {
                    self.state = PhysicalState::AtEnd;
                    return None;
                }
            }
            PhysicalState::FwdBehind => (),
            PhysicalState::RevEq => (),
            PhysicalState::RevBehind => {
                if !self.physical_forwards() || !self.physical_forwards() {
                    self.state = PhysicalState::AtEnd;
                    return None;
                }
                self.state = PhysicalState::FwdBehind;
            }
        };
        Some((&self.buf.0, &self.buf.1))
    }

    fn peek_prev(&mut self) -> Option<(&K, &V)> {
        match self.state {
            PhysicalState::AtStart => {
                return None;
            }
            PhysicalState::FwdBehind => {
                if self.physical_reverse() && self.physical_reverse() {
                    self.state = PhysicalState::RevBehind;
                } else {
                    self.state = PhysicalState::AtStart;
                    return None;
                }
            }
            PhysicalState::FwdEq => (),
            PhysicalState::RevBehind => (),
            PhysicalState::AtEnd | PhysicalState::RevEq => {
                if self.physical_reverse() {
                    self.state = PhysicalState::RevBehind;
                } else {
                    self.state = PhysicalState::AtStart;
                    return None;
                }
            }
        };
        Some((&self.buf.0, &self.buf.1))
    }

    fn seek_ge(&mut self, key: &K) {
        // TODO: we should use a buffer to clone_into the key here.
        self.iter.seek_ge(&(key.clone(), 0));
        if self.physical_forwards() {
            self.state = PhysicalState::FwdBehind;
        } else {
            self.state = PhysicalState::FwdEq;
        }
    }

    fn start(&mut self) {
        self.iter.start();
        self.state = PhysicalState::FwdBehind;
        self.physical_forwards();
    }

    fn end(&mut self) {
        self.iter.end();
        self.state = PhysicalState::RevBehind;
        self.physical_reverse();
    }
}

#[derive(Debug, Clone)]
pub struct VecIter<K, V> {
    idx: usize,
    contents: Rc<Vec<(K, V)>>,
}

impl<K, V> VecIter<K, V>
where
    K: std::fmt::Debug,
    V: std::fmt::Debug,
{
    pub fn new(v: Rc<Vec<(K, V)>>) -> Self {
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

    fn start(&mut self) {
        self.idx = 0;
    }

    fn end(&mut self) {
        self.idx = self.contents.len();
    }
}

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
    K: Ord,
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

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use crate::memtable::{KVIter, SeqnumIter, VecIter};

    #[test]
    fn test_seqnum_iter() {
        datadriven::walk("src/memtable/testdata/", |f| {
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
                        if val == "<DELETE>" {
                            data.push(((key, seqnum), None));
                        } else {
                            data.push(((key, seqnum), Some(val)));
                        }
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
                                    out.push_str("> eof");
                                }
                                Some((k, v)) => {
                                    out.push_str(&format!("> {}={}", k, v));
                                }
                            },
                            ')' => match iter.as_mut().unwrap().peek() {
                                None => {
                                    out.push_str(") eof");
                                }
                                Some((k, v)) => {
                                    out.push_str(&format!(") {}={}", k, v));
                                }
                            },
                            '<' => match iter.as_mut().unwrap().prev() {
                                None => {
                                    out.push_str("< eof");
                                }
                                Some((k, v)) => {
                                    out.push_str(&format!("< {}={}", k, v));
                                }
                            },
                            '(' => match iter.as_mut().unwrap().peek_prev() {
                                None => {
                                    out.push_str("( eof");
                                }
                                Some((k, v)) => {
                                    out.push_str(&format!("( {}={}", k, v));
                                }
                            },

                            _ => panic!("unhandled: {}", command),
                        }
                        out.push_str(&format!(" ({:?})\n", iter.as_ref().unwrap().state));
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
}

type DBEntry<K, V> = ((K, usize), Option<V>);

#[derive(Debug)]
pub struct Memtable<K, V>
where
    K: Ord,
{
    prev_seqnum: usize,
    entries: Vec<Rc<Vec<DBEntry<K, V>>>>,
}

impl<K, V> Memtable<K, V>
where
    K: Default + Ord + Clone + std::fmt::Debug + Encode + Decode,
    V: Default + Clone + std::fmt::Debug + Encode,
{
    pub fn new() -> Self {
        Memtable {
            prev_seqnum: 0,
            entries: Vec::new(),
        }
    }

    pub fn apply_command(&mut self, cmd: DBCommand<K, V>) {
        match cmd {
            DBCommand::Write(seqnum, k, v) => {
                self.insert(seqnum, k, v);
            }
            DBCommand::Delete(seqnum, k) => {
                self.delete(seqnum, k);
            }
        }
    }

    // TODO: replace this with an iterator.
    fn merge(lhs: Rc<Vec<DBEntry<K, V>>>, rhs: Rc<Vec<DBEntry<K, V>>>) -> Rc<Vec<DBEntry<K, V>>> {
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

    fn insert_val(&mut self, s: usize, k: K, v: Option<V>) {
        self.prev_seqnum = s;
        self.entries.push(Rc::new(vec![((k, s), v)]));
        for i in (0..(self.entries.len() - 1)).rev() {
            self.maybe_fix_at(i);
        }
    }

    pub fn insert(&mut self, s: usize, k: K, v: V) {
        self.insert_val(s, k, Some(v))
    }

    pub fn delete(&mut self, s: usize, k: K) {
        self.insert_val(s, k, None)
    }

    pub fn scan(&self) -> impl KVIter<(K, usize), Option<V>> {
        MergingIter::new(self.entries.iter().map(|e| VecIter::new(e.clone())))
    }

    pub fn read_at(&self, seqnum: usize) -> impl KVIter<K, V> {
        SeqnumIter::new(
            seqnum,
            MergingIter::new(self.entries.iter().map(|e| VecIter::new(e.clone()))),
        )
    }
}
