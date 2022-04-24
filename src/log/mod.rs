#![allow(dead_code)]
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};

use crate::{
    encoding::{Decode, Encode},
    fs::DbDir,
};

use self::file_log::Log;

pub(crate) mod file_log;

pub trait LogEntry: std::fmt::Debug + Clone + Encode + Decode {
    fn seqnum(&self) -> usize;
}

#[derive(Debug)]
pub struct LogSet<D: DbDir, E: LogEntry> {
    active_log: Log<D, E>,
    old: Vec<Log<D, E>>,
    dir: D,
    _marker: PhantomData<E>,
}

impl<D, E> LogSet<D, E>
where
    D: DbDir,
    E: LogEntry,
{
    pub fn fnames(&self) -> Vec<String> {
        let mut out = vec![self.active_log.fname().to_owned()];
        out.extend(self.old.iter().map(|l| l.fname().to_owned()));
        out
    }

    pub async fn open_dir(dir: D, cur_seqnum: usize) -> anyhow::Result<Self> {
        let active_log = Log::new(dir.clone(), cur_seqnum)?;

        Ok(LogSet {
            active_log,
            old: Vec::new(),
            dir,
            _marker: PhantomData,
        })
    }

    pub fn current(&mut self) -> &mut Log<D, E> {
        &mut self.active_log
    }

    pub async fn fresh(&mut self) -> anyhow::Result<()> {
        let upper_bound = self.active_log.frontier();
        let active_log = Log::new(self.dir.clone(), upper_bound)?;
        let old_log = std::mem::replace(&mut self.active_log, active_log);
        self.old.push(old_log);
        Ok(())
    }

    pub fn remove_old(&mut self) {
        self.old.clear();
    }
}
