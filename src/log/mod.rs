#![allow(dead_code)]
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};

use crate::encoding::{Decode, Encode};

pub(crate) mod file_log;
pub(crate) mod mock_log;

pub trait LogEntry: std::fmt::Debug + Clone + Encode + Decode {
    fn seqnum(&self) -> usize;
}

pub struct Frozen<E, L> {
    l: L,
    _marker: PhantomData<E>,
}

impl<E, L> Frozen<E, L>
where
    E: LogEntry,
    L: Logger<E>,
{
    fn frontier(&self) -> usize {
        self.l.frontier()
    }
}

pub trait Logger<E>: std::fmt::Debug + Sized
where
    E: LogEntry,
{
    fn new<P>(dir: &P, lower_bound: usize) -> anyhow::Result<Self>
    where
        P: AsRef<Path>;
    fn write(&mut self, m: &E) -> anyhow::Result<()>;
    fn fname(&self) -> PathBuf;

    fn frontier(&self) -> usize;

    fn freeze(self) -> Frozen<E, Self> {
        Frozen {
            l: self,
            _marker: PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct LogSet<E: LogEntry, L: Logger<E>> {
    active_log: L,
    old: Vec<L>,
    dir: PathBuf,
    _marker: PhantomData<E>,
}

impl<E, L> LogSet<E, L>
where
    E: LogEntry,
    L: Logger<E>,
{
    pub fn fnames(&self) -> Vec<PathBuf> {
        let mut out = vec![self.active_log.fname()];
        out.extend(self.old.iter().map(|l| l.fname().into()));
        out
    }

    pub fn open_dir<P>(dir: &P) -> anyhow::Result<Self>
    where
        P: AsRef<Path>,
    {
        // TODO: what's the right starting seqnum?
        let cur_seqnum = 0;
        let active_log = L::new(
            &dir.as_ref().join(format!("wal-{}", cur_seqnum)),
            cur_seqnum,
        )?;

        Ok(LogSet {
            active_log,
            old: Vec::new(),
            dir: dir.as_ref().to_owned(),
            _marker: PhantomData,
        })
    }

    pub fn current(&mut self) -> &mut L {
        &mut self.active_log
    }

    pub fn fresh(&mut self) -> anyhow::Result<()> {
        let upper_bound = self.active_log.frontier();
        let active_log = L::new(&self.dir.join(format!("wal-{}", upper_bound)), upper_bound)?;
        let old_log = std::mem::replace(&mut self.active_log, active_log);
        self.old.push(old_log);
        Ok(())
    }

    pub fn remove_old(&mut self) {
        self.old.clear();
    }
}
