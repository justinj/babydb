use std::{
    fs::{File, OpenOptions},
    io::{Cursor, Read, Seek, SeekFrom},
    marker::PhantomData,
};

use crate::memtable::KVIter;

use super::Decode;

struct Reader<T: Decode, R: Seek + Read> {
    r: R,
    data_len: u64,
    idx: u64,
    buf: Vec<u8>,
    scratch: Vec<u8>,
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
            scratch: Vec::with_capacity(1024),
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
        let v = T::decode(&self.buf, &mut self.scratch)?;

        self.idx += 8 + len as u64;

        Ok(Some(v))
    }
}

pub struct SstReader<K, V>
where
    K: Decode + Default,
    V: Decode + Default,
{
    reader: Reader<(K, V), File>,
    index: Vec<(K, usize)>,
    buffer: (K, V),
    _marker: PhantomData<(K, V)>,
}

impl<K, V> KVIter<K, V> for SstReader<K, V>
where
    K: Ord + Decode + Default + std::fmt::Debug,
    V: Decode + Default,
{
    fn next(&mut self) -> Option<(&K, &V)> {
        if let Some(kv) = self.reader.next().unwrap() {
            self.buffer = kv;
            Some((&self.buffer.0, &self.buffer.1))
        } else {
            None
        }
    }

    fn peek(&mut self) -> Option<(&K, &V)> {
        todo!()
    }

    fn prev(&mut self) -> Option<(&K, &V)> {
        todo!()
    }

    fn peek_prev(&mut self) -> Option<(&K, &V)> {
        todo!()
    }

    fn seek_ge(&mut self, key: &K) {
        todo!()
    }
}

impl<K, V> SstReader<K, V>
where
    K: Decode + Default + std::fmt::Debug,
    V: Decode + Default,
{
    pub fn load(fname: &str) -> anyhow::Result<Self> {
        // TODO: check if already exists and fail if yes.
        let mut file = OpenOptions::new().read(true).open(fname)?;

        file.seek(SeekFrom::End(-8))?;
        let mut buf = [0_u8; 4];
        file.read_exact(&mut buf)?;
        let data_len = u32::from_le_bytes(buf);

        file.seek(SeekFrom::End(-4))?;
        file.read_exact(&mut buf)?;
        let index_len = u32::from_le_bytes(buf);

        file.seek(SeekFrom::End(-8 - (index_len as i64)))?;
        let mut index_data: Vec<_> = (0..index_len).map(|_| 0).collect();
        file.read_exact(&mut index_data)?;
        let index: Reader<(K, usize), _> = Reader::new(Cursor::new(index_data), index_len);

        let index: Vec<_> = index.collect();

        file.seek(SeekFrom::Start(0))?;

        Ok(SstReader {
            reader: Reader::new(file, data_len),
            buffer: Default::default(),
            index,
            _marker: PhantomData,
        })
    }
}
