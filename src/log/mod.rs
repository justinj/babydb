#![allow(dead_code)]
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};

use crate::encoding::{Decode, Encode};

use self::file_log::Log;

pub(crate) mod file_log;

pub trait LogEntry: std::fmt::Debug + Clone + Encode + Decode {
    fn seqnum(&self) -> usize;
}

#[derive(Debug)]
pub struct LogSet<E: LogEntry> {
    active_log: Log<E>,
    old: Vec<Log<E>>,
    dir: PathBuf,
    _marker: PhantomData<E>,
}

impl<E> LogSet<E>
where
    E: LogEntry,
{
    pub fn fnames(&self) -> Vec<PathBuf> {
        let mut out = vec![self.active_log.fname()];
        out.extend(self.old.iter().map(|l| l.fname()));
        out
    }

    pub async fn open_dir<P>(dir: &P) -> anyhow::Result<Self>
    where
        P: AsRef<Path>,
    {
        // TODO: what's the right starting seqnum?
        let cur_seqnum = 0;
        let active_log = Log::new(
            &dir.as_ref().join(format!("wal-{}", cur_seqnum)),
            cur_seqnum,
        )
        .await?;

        Ok(LogSet {
            active_log,
            old: Vec::new(),
            dir: dir.as_ref().to_owned(),
            _marker: PhantomData,
        })
    }

    pub fn current(&mut self) -> &mut Log<E> {
        &mut self.active_log
    }

    pub async fn fresh(&mut self) -> anyhow::Result<()> {
        let upper_bound = self.active_log.frontier();
        let active_log =
            Log::new(&self.dir.join(format!("wal-{}", upper_bound)), upper_bound).await?;
        let old_log = std::mem::replace(&mut self.active_log, active_log);
        self.old.push(old_log);
        Ok(())
    }

    pub fn remove_old(&mut self) {
        self.old.clear();
    }
}
