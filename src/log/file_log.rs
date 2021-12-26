use std::{
    fs::File,
    io::{Read, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
};

use crate::encoding::{KeyReader, KeyWriter};

use super::{LogEntry, Logger};

pub struct LogReader<E>
where
    E: LogEntry,
{
    file: File,
    reader: KeyReader,
    _marker: PhantomData<E>,
}

impl<E> LogReader<E>
where
    E: LogEntry,
{
    pub fn new<P>(fname: P) -> anyhow::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = File::open(&fname)?;
        Ok(Self {
            file,
            reader: KeyReader::new(),
            _marker: PhantomData,
        })
    }
}

impl<E> Iterator for LogReader<E>
where
    E: LogEntry,
{
    type Item = E;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: careful error handling needs to occur here.
        // First, get the u32 that denotes this entry's length.
        let mut buf = [0_u8; 4];
        // TODO: signal this error upwards somehow.
        self.file.read_exact(&mut buf).ok()?;
        let data_len = u32::from_le_bytes(buf);

        // TODO: this is probably not the right way to fill out the buffer to
        // the right size/length, maybe keep around an array of zeroes and copy
        // it over? needs benchmarking.
        let buf = self.reader.buf_mut();
        buf.clear();
        buf.extend(std::iter::repeat(0).take(data_len.try_into().unwrap()));
        self.file.read_exact(buf).ok()?;

        let v = E::decode(&mut self.reader).unwrap();

        Some(v)
    }
}

#[derive(Debug)]
pub struct Log<E>
where
    E: LogEntry,
{
    // TODO: does this do buffering or does this need to be a BufReader<File>?
    file: File,
    file_name: String,
    highest_seen_seqnum: usize,
    kw: KeyWriter,
    _marker: PhantomData<E>,
}

impl<E: LogEntry> Logger<E> for Log<E> {
    fn fname(&self) -> PathBuf {
        self.file_name.clone().into()
    }

    fn new(dir: &str, lower_bound: usize) -> anyhow::Result<Self> {
        // TODO: use real file sep
        std::fs::create_dir_all(dir)?;
        let file_name = format!("{}/wal{}", dir, lower_bound);
        let file = File::create(&file_name)?;
        // Ensure the file is created.
        file.sync_all()?;
        Ok(Self {
            file,
            file_name,
            highest_seen_seqnum: lower_bound,
            kw: KeyWriter::new(),
            _marker: PhantomData,
        })
    }

    fn write(&mut self, m: &E) -> anyhow::Result<()> {
        self.kw.clear();
        m.write_bytes(&mut self.kw);
        self.file
            .write_all(&(self.kw.buf.len() as u32).to_le_bytes())?;
        self.file.write_all(&self.kw.buf)?;
        self.file.flush()?;
        self.file.sync_all()?;

        Ok(())
    }

    fn frontier(&self) -> usize {
        self.highest_seen_seqnum + 1
    }
}
