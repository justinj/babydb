use std::{
    fs::{File, OpenOptions},
    io::{Cursor, Write},
    marker::PhantomData,
};

use crate::memtable::KVIter;

use super::{Encode, KeyWriter};

const RESET_INTERVAL: usize = 2;

struct Writer<W>
where
    W: Write,
{
    w: W,
    prev_val: Vec<u8>,
}

impl<W> Writer<W>
where
    W: Write,
{
    fn new(w: W) -> Self {
        Writer {
            w,
            prev_val: Vec::with_capacity(1024),
        }
    }

    #[allow(unused)]
    fn reset(&mut self) {
        self.prev_val.clear();
    }

    fn write<T: Encode>(&mut self, t: &T) -> anyhow::Result<()> {
        // TODO: reuse this KeyWriter.
        let mut kw = KeyWriter::new();
        t.write_bytes(&mut kw);

        // TODO: do not allocate a new vec here.
        let mut buf = kw.replace(Vec::new());

        let n = std::cmp::min(self.prev_val.len(), buf.len());
        let mut shared_prefix_len = n;
        for (i, item) in buf.iter().enumerate().take(n) {
            if self.prev_val[i] != *item {
                shared_prefix_len = i;
                break;
            }
        }

        self.w
            .write_all(&((buf.len() - shared_prefix_len) as u32).to_le_bytes())?;
        self.w
            .write_all(&(shared_prefix_len as u32).to_le_bytes())?;
        self.w.write_all(&buf[shared_prefix_len..])?;

        std::mem::swap(&mut buf, &mut self.prev_val);

        buf.clear();
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
    K: Ord + Encode + Clone,
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

    fn build_block(&mut self, data: &mut Vec<u8>) -> anyhow::Result<()> {
        let mut writer = Writer::new(Cursor::new(data));
        let mut written = 0;
        while let Some((k, v)) = self.it.next() {
            writer.write(&(k, v))?;
            written += 1;
            if written >= RESET_INTERVAL {
                break;
            }
        }

        Ok(())
    }

    pub fn write(mut self) -> anyhow::Result<()> {
        let mut index = Vec::new();
        let mut index_writer = Writer::new(&mut index);

        let mut bytes_written = 0;
        let mut block_buffer = Vec::new();

        while let Some((header_key, _)) = self.it.peek() {
            // TODO: is this clone necessary?
            let k = (*header_key).clone();

            self.build_block(&mut block_buffer)?;
            self.file.write_all(&block_buffer)?;

            let index_entry = (k, (bytes_written as u32, block_buffer.len() as u32));
            index_writer.write(&index_entry)?;

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