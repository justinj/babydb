#![allow(dead_code)]
use std::{
    fs::{File, OpenOptions},
    io::{Cursor, Read, Seek, SeekFrom},
    marker::PhantomData,
    path::Path,
};

use crate::{
    encoding::{Decode, KeyReader},
    memtable::KVIter,
};

struct Reader<T: Decode, R: Seek + Read> {
    r: R,
    data_len: u64,
    idx: u64,
    buf: Vec<u8>,
    _marker: PhantomData<T>,
}

impl<T, R> Iterator for Reader<T, R>
where
    T: Decode,
    R: Seek + Read,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.next().unwrap()
    }
}

impl<T, R> Reader<T, R>
where
    T: Decode,
    R: Seek + Read,
{
    // TODO: should be u64
    fn new(r: R, data_len: u32) -> Self {
        Reader {
            r,
            data_len: data_len.into(),
            idx: 0,
            buf: Vec::with_capacity(1024),
            _marker: PhantomData,
        }
    }

    fn seek(&mut self, n: u64) -> anyhow::Result<()> {
        self.idx = n;
        self.r.seek(SeekFrom::Start(n))?;
        Ok(())
    }

    fn next(&mut self) -> anyhow::Result<Option<T>> {
        if self.idx >= self.data_len {
            return Ok(None);
        }
        let mut buf = [0_u8; 4];
        self.r.read_exact(&mut buf)?;
        let len = u32::from_le_bytes(buf);
        self.r.read_exact(&mut buf)?;
        let prefix = u32::from_le_bytes(buf);

        // TODO: Better way to do this?
        self.buf.truncate(prefix as usize);
        self.buf.extend((0..len).map(|_| 0));

        self.r.read_exact(&mut self.buf[(prefix as usize)..])?;

        // TODO: reuse this KeyReader
        let mut kr = KeyReader::new();
        kr.load(&self.buf);
        let v = T::decode(&mut kr)?;

        self.idx += 8 + len as u64;

        Ok(Some(v))
    }
}

struct Block<K, V> {
    buf: Vec<u8>,
    scratch: Vec<u8>,
    data: Vec<(K, V)>,
    idx: usize,
}

impl<K, V> Block<K, V>
where
    K: Decode + Ord + std::fmt::Debug,
    V: Decode + std::fmt::Debug,
{
    fn new() -> Self {
        Block {
            buf: Vec::new(),
            scratch: Vec::new(),
            data: Vec::new(),
            idx: 0,
        }
    }

    fn seek_ge(&mut self, seek_key: &K) {
        self.idx = self.data.partition_point(|(k, _v)| k < seek_key);
    }

    fn align_end(&mut self) {
        self.idx = self.data.len();
    }

    fn align_start(&mut self) {
        self.idx = 0;
    }

    fn next(&mut self) -> Option<(&K, &V)> {
        if self.idx < self.data.len() {
            self.idx += 1;
            let entry = &self.data[self.idx - 1];
            Some((&entry.0, &entry.1))
        } else {
            None
        }
    }

    fn peek(&mut self) -> Option<(&K, &V)> {
        if self.idx < self.data.len() {
            let entry = &self.data[self.idx];
            Some((&entry.0, &entry.1))
        } else {
            None
        }
    }

    fn prev(&mut self) -> Option<(&K, &V)> {
        if self.idx > 0 {
            self.idx -= 1;
            let entry = &self.data[self.idx];
            Some((&entry.0, &entry.1))
        } else {
            None
        }
    }

    fn peek_prev(&mut self) -> Option<(&K, &V)> {
        if self.idx > 0 {
            let entry = &self.data[self.idx - 1];
            Some((&entry.0, &entry.1))
        } else {
            None
        }
    }

    fn load<R: Read>(&mut self, data: &mut R, mut n: u32) -> anyhow::Result<()> {
        self.data.clear();
        self.scratch.clear();
        self.buf.clear();
        self.idx = 0;
        while n > 0 {
            let mut buf = [0_u8; 4];
            data.read_exact(&mut buf)?;
            let len = u32::from_le_bytes(buf);
            data.read_exact(&mut buf)?;
            let prefix = u32::from_le_bytes(buf);

            // TODO: Better way to do this?
            self.buf.truncate(prefix as usize);
            self.buf.extend((0..len).map(|_| 0));

            data.read_exact(&mut self.buf[(prefix as usize)..])?;
            let mut kr = KeyReader::new();
            kr.load(&self.buf);
            self.data.push(<(K, V)>::decode(&mut kr)?);

            n -= len + 8;
        }
        Ok(())
    }
}

pub struct SstReader<K, V>
where
    K: Decode + Default,
    V: Decode + Default,
{
    file: File,
    // (loc, len)
    index_block: Block<K, (u32, u32)>,
    current_block: Block<K, V>,
    state: ReaderState,
    _marker: PhantomData<(K, V)>,
}

enum ReaderState {
    AtStart,
    Midblock,
}

impl<K, V> KVIter<K, V> for SstReader<K, V>
where
    K: Ord + Decode + Default + std::fmt::Debug,
    V: Decode + Default + std::fmt::Debug,
{
    fn next(&mut self) -> Option<(&K, &V)> {
        match self.state {
            ReaderState::AtStart => {
                // Load in the very first block.
                if !self.next_block().unwrap() {
                    None
                } else {
                    self.state = ReaderState::Midblock;
                    self.current_block.next()
                }
            }
            ReaderState::Midblock => {
                // TODO: figure out why we can't if let here
                if self.current_block.peek().is_some() {
                    self.current_block.next()
                } else if !self.next_block().unwrap() {
                    None
                } else {
                    self.current_block.next()
                }
            }
        }
    }

    fn peek(&mut self) -> Option<(&K, &V)> {
        match self.state {
            ReaderState::AtStart => {
                // Load in the very first block.
                if !self.next_block().unwrap() {
                    None
                } else {
                    self.state = ReaderState::Midblock;
                    self.current_block.peek()
                }
            }
            ReaderState::Midblock => {
                // TODO: figure out why we can't if let here
                if self.current_block.peek().is_some() {
                    self.current_block.peek()
                } else if !self.next_block().unwrap() {
                    None
                } else {
                    self.current_block.peek()
                }
            }
        }
    }

    fn prev(&mut self) -> Option<(&K, &V)> {
        match self.state {
            ReaderState::AtStart => None,
            ReaderState::Midblock => {
                // TODO: figure out why we can't if let here
                if self.current_block.peek_prev().is_some() {
                    self.current_block.prev()
                } else if !self.prev_block().unwrap() {
                    None
                } else {
                    self.current_block.prev()
                }
            }
        }
    }

    fn peek_prev(&mut self) -> Option<(&K, &V)> {
        match self.state {
            ReaderState::AtStart => None,
            ReaderState::Midblock => {
                // TODO: figure out why we can't if let here
                if self.current_block.peek_prev().is_some() {
                    self.current_block.peek_prev()
                } else if !self.prev_block().unwrap() {
                    None
                } else {
                    self.current_block.peek_prev()
                }
            }
        }
    }

    fn seek_ge(&mut self, key: &K) {
        self.index_block.seek_ge(key);
        // TODO: how to handle errors here without infecting the nice simple traits?
        self.next_block().unwrap();
        self.current_block.seek_ge(key);
    }
}

impl<K, V> SstReader<K, V>
where
    K: Decode + Default + Ord + std::fmt::Debug,
    V: Decode + Default + std::fmt::Debug,
{
    fn next_block(&mut self) -> anyhow::Result<bool> {
        match self.index_block.next() {
            None => Ok(false),
            Some((_k, (loc, len))) => {
                // TODO: check if loc is where we already are and don't move if so.
                self.file.seek(SeekFrom::Start(*loc as u64))?;
                self.current_block.load(&mut self.file, *len as u32)?;
                self.current_block.align_start();

                Ok(true)
            }
        }
    }

    // TODO: coalesce with above.
    fn prev_block(&mut self) -> anyhow::Result<bool> {
        match self.index_block.prev() {
            None => Ok(false),
            Some((_k, (loc, len))) => {
                // TODO: check if loc is where we already are and don't move if so.
                self.file.seek(SeekFrom::Start(*loc as u64))?;
                self.current_block.load(&mut self.file, *len as u32)?;
                self.current_block.align_end();

                Ok(true)
            }
        }
    }

    pub fn load<P>(fname: P) -> anyhow::Result<Self>
    where
        P: AsRef<Path>,
    {
        // TODO: check if already exists and fail if yes.
        let mut file = OpenOptions::new().read(true).open(fname)?;

        file.seek(SeekFrom::End(-8))?;
        let mut buf = [0_u8; 4];
        file.read_exact(&mut buf)?;
        // TODO: do we need this?
        let _data_len = u32::from_le_bytes(buf);

        file.seek(SeekFrom::End(-4))?;
        file.read_exact(&mut buf)?;
        let index_len = u32::from_le_bytes(buf);

        // Load the index block into memory.
        file.seek(SeekFrom::End(-8 - (index_len as i64)))?;
        let mut index_data: Vec<_> = (0..index_len).map(|_| 0).collect();
        file.read_exact(&mut index_data)?;
        let mut index_block = Block::new();
        let len = index_data.len() as u32;
        index_block.load(&mut Cursor::new(index_data), len)?;

        file.seek(SeekFrom::Start(0))?;

        Ok(SstReader {
            file,
            current_block: Block::new(),
            index_block,
            state: ReaderState::AtStart,
            _marker: PhantomData,
        })
    }
}
