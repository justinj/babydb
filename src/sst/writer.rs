use std::{
    fs::{File, OpenOptions},
    io::{Cursor, Write},
    marker::PhantomData,
};

use crate::memtable::KVIter;

use super::Encode;

const RESET_INTERVAL: usize = 10;

struct Writer<W>
where
    W: Write,
{
    w: W,
    prev_val: Vec<u8>,
    scratch: Vec<u8>,
    buf: Vec<u8>,
}

impl<W> Writer<W>
where
    W: Write,
{
    fn new(w: W) -> Self {
        Writer {
            w,
            prev_val: Vec::with_capacity(1024),
            buf: Vec::with_capacity(1024),
            scratch: Vec::with_capacity(1024),
        }
    }

    fn reset(&mut self) {
        self.prev_val.clear();
    }

    fn write<T: Encode>(&mut self, t: &T) -> anyhow::Result<()> {
        t.write_bytes(&mut self.buf, &mut self.scratch);

        let n = std::cmp::min(self.prev_val.len(), self.buf.len());
        let mut shared_prefix_len = n;
        for i in 0..n {
            if self.prev_val[i] != self.buf[i] {
                shared_prefix_len = i;
                break;
            }
        }

        self.w
            .write_all(&((self.buf.len() - shared_prefix_len) as u32).to_le_bytes())?;
        self.w
            .write_all(&(shared_prefix_len as u32).to_le_bytes())?;
        self.w.write_all(&self.buf[shared_prefix_len..])?;

        std::mem::swap(&mut self.buf, &mut self.prev_val);

        self.buf.clear();
        Ok(())
    }
}

pub struct SstWriter<I, K, V>
where
    I: KVIter<K, V>,
    K: Ord + Encode,
    V: Encode,
{
    file: File,
    it: I,
    _marker: PhantomData<(K, V)>,
}

impl<I, K, V> SstWriter<I, K, V>
where
    I: KVIter<K, V>,
    K: Ord + Encode,
    V: Encode,
{
    pub fn new(it: I, fname: &str) -> Self {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(fname)
            .unwrap();
        SstWriter {
            file,
            it,
            _marker: PhantomData,
        }
    }

    // TODO: reuse block handle struct?
    fn build_block(&mut self, data: &mut Vec<u8>) -> anyhow::Result<()> {
        let mut writer = Writer::new(Cursor::new(data));
        while let Some((k, v)) = self.it.next() {
            writer.write(&(k, v))?;
        }

        Ok(())
    }

    pub fn write(mut self) -> anyhow::Result<()> {
        let mut index = Vec::new();
        let mut index_writer = Writer::new(&mut index);

        let mut bytes_written = 0;

        let mut block_buffer = Vec::new();

        while let Some((header_key, _)) = self.it.peek() {
            let index_entry = (header_key, bytes_written);
            index_writer.write(&index_entry)?;

            self.build_block(&mut block_buffer)?;
            self.file.write_all(&block_buffer)?;
            bytes_written += block_buffer.len();

            block_buffer.clear();
        }

        let data_length = bytes_written;

        // Write the index block.
        self.file.write_all(&index)?;
        // Write the length of the data block.
        self.file.write_all(&(data_length as u32).to_le_bytes())?;
        // Write the length of the index block.
        self.file.write_all(&(index.len() as u32).to_le_bytes())?;

        Ok(())
    }
}
