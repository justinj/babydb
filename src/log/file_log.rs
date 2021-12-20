use std::{fs::File, io::Write, marker::PhantomData};

use crate::encoding::KeyWriter;

use super::{LogEntry, Logger};

#[derive(Debug)]
pub struct Log<E>
where
    E: LogEntry,
{
    file: File,
    highest_seen_seqnum: usize,
    kw: KeyWriter,
    _marker: PhantomData<E>,
}

impl<E: LogEntry> Logger<E> for Log<E> {
    fn new(dir: &str, lower_bound: usize) -> anyhow::Result<Self> {
        // TODO: use real file sep
        let file = File::open(format!("{}/wal{}", dir, lower_bound))?;
        Ok(Self {
            file,
            highest_seen_seqnum: lower_bound,
            kw: KeyWriter::new(),
            _marker: PhantomData,
        })
    }

    fn write(&mut self, m: &E) -> anyhow::Result<()> {
        self.kw.clear();
        m.write_bytes(&mut self.kw);
        self.file.write_all(&self.kw.buf)?;
        self.file.flush()?;

        Ok(())
    }

    fn frontier(&self) -> usize {
        self.highest_seen_seqnum + 1
    }
}
