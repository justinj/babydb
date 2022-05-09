use std::{
    io::{Cursor, Write},
    marker::PhantomData,
};

use anyhow::bail;

use crate::{
    encoding::{Encode, KeyWriter},
    fs::DbFile,
    memtable::KVIter,
};

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
        // TODO: why can we not just slice here?
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

pub struct SstWriter<I, K, V, D>
where
    I: KVIter<K, V>,
    K: Ord + Encode,
    V: Encode,
    D: DbFile,
{
    file: D,
    it: I,
    _marker: PhantomData<(K, V)>,
}

impl<I, K, V, D> SstWriter<I, K, V, D>
where
    I: KVIter<K, V>,
    K: Ord + Encode + Clone,
    V: Encode,
    D: DbFile,
{
    pub fn new(it: I, file: D) -> Self {
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

        let min_key = if let Some((k, _)) = self.it.peek() {
            k.clone()
        } else {
            bail!("will only write non-empty SST")
        };

        while let Some((header_key, _)) = self.it.peek() {
            // TODO: is this clone necessary?
            let k = (*header_key).clone();

            self.build_block(&mut block_buffer)?;
            self.file.write(&block_buffer)?;

            let index_entry = (k, (bytes_written as u32, block_buffer.len() as u32));
            index_writer.write(&index_entry)?;

            bytes_written += block_buffer.len();

            block_buffer.clear();
        }

        self.it.end();

        let max_key = if let Some((k, _)) = self.it.peek_prev() {
            k.clone()
        } else {
            unreachable!()
        };

        // Write the index block.
        self.file.write(&index)?;

        // Write the metadata.
        let mut metadata_len = 0;

        // Write the bounds keys.
        let mut data = Vec::new();
        let mut writer = Writer::new(Cursor::new(&mut data));
        writer.write(&min_key)?;
        writer.write(&max_key)?;
        self.file.write(&data)?;
        metadata_len += data.len();

        // Write the length of the index block.
        self.file.write(&(index.len() as u32).to_le_bytes())?;
        metadata_len += 4;
        // Write the length of the metadata.
        self.file.write(&(metadata_len as u32).to_le_bytes())?;

        self.file.sync()?;

        Ok(())
    }
}
