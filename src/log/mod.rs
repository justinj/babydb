#![allow(dead_code)]
use std::marker::PhantomData;

use crate::encoding::Encode;

pub(crate) mod file_log;
pub(crate) mod mock_log;

pub trait LogEntry: std::fmt::Debug + Clone + Encode {
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
    fn new(dir: &str, lower_bound: usize) -> anyhow::Result<Self>;
    fn write(&mut self, m: &E) -> anyhow::Result<()>;

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
    dir: String,
    _marker: PhantomData<E>,
}

impl<E, L> LogSet<E, L>
where
    E: LogEntry,
    L: Logger<E>,
{
    pub fn open_dir(dir: String) -> anyhow::Result<Self> {
        // TODO: what's the right starting seqnum?
        let cur_seqnum = 0;
        Ok(LogSet {
            // TODO: use cross-platform path join
            active_log: L::new(format!("{}/wal-{}", dir, cur_seqnum).as_str(), cur_seqnum)?,
            old: Vec::new(),
            dir,
            _marker: PhantomData,
        })
    }

    pub fn current(&mut self) -> &mut L {
        &mut self.active_log
    }

    pub fn fresh(&mut self) -> anyhow::Result<()> {
        let upper_bound = self.active_log.frontier();
        let old_log = std::mem::replace(
            &mut self.active_log,
            L::new(
                format!("{}/wal-{}", self.dir, upper_bound).as_str(),
                upper_bound,
            )?,
        );
        self.old.push(old_log);
        Ok(())
    }
}
