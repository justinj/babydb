use std::{
    io::{Read, Seek},
    marker::PhantomData,
};

use crate::{
    encoding::{KeyReader, KeyWriter},
    fs::{DbDir, DbFile},
};

use super::LogEntry;

pub struct LogReader<R, E>
where
    R: Read + Seek,
    E: LogEntry,
{
    file: R,
    reader: KeyReader,
    _marker: PhantomData<E>,
}

impl<R, E> LogReader<R, E>
where
    R: Read + Seek,
    E: LogEntry,
{
    pub fn new(file: R) -> anyhow::Result<Self> {
        Ok(Self {
            file,
            reader: KeyReader::new(),
            _marker: PhantomData,
        })
    }
}

impl<R, E> Iterator for LogReader<R, E>
where
    R: Read + Seek,
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
pub struct Log<D, E>
where
    D: DbDir,
    E: LogEntry,
{
    // TODO: does this do buffering?
    file: D::DbFile,
    filename: String,
    highest_seen_seqnum: usize,
    kw: KeyWriter,
    _marker: PhantomData<E>,
}

impl<D: DbDir, E: LogEntry> Log<D, E> {
    pub fn fname(&self) -> &str {
        self.filename.as_str()
    }

    pub fn new(mut dir: D, lower_bound: usize) -> anyhow::Result<Self> {
        let filename = format!("wal{}", lower_bound);
        let mut file = dir.create(&filename).expect("WAL file already existed");
        // Ensure the file is created.
        file.sync()?;
        Ok(Self {
            filename,
            file,
            highest_seen_seqnum: lower_bound,
            kw: KeyWriter::new(),
            _marker: PhantomData,
        })
    }

    pub fn write(&mut self, m: &E) -> anyhow::Result<()> {
        self.kw.clear();
        m.write_bytes(&mut self.kw);
        self.file.write(&(self.kw.buf.len() as u32).to_le_bytes())?;
        self.file.write(&self.kw.buf)?;
        self.file.sync()?;

        Ok(())
    }

    pub fn frontier(&self) -> usize {
        self.highest_seen_seqnum + 1
    }
}
