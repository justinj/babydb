#![allow(dead_code)]
use std::{
    io::{Cursor, Read, Seek, SeekFrom},
    marker::PhantomData,
};

use crate::{
    encoding::{Decode, KeyReader},
    fs::{DbDir, DbFile},
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

#[derive(Debug)]
struct Block<K, V> {
    buf: Vec<u8>,
    scratch: Vec<u8>,
    data: Vec<(K, V)>,
    idx: usize,
}

// TODO actual description of what all these things are.

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

    fn seek_gt(&mut self, seek_key: &K) {
        self.idx = self.data.partition_point(|(k, _v)| k <= seek_key);
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

#[derive(Debug)]
pub struct SstMeta<K>
where
    K: Decode,
{
    pub min_key: K,
    pub max_key: K,
    pub num_bytes: usize,
}

#[derive(Debug)]
pub struct SstReader<K, V, D>
where
    K: Decode + Default,
    V: Decode + Default,
    D: DbDir,
{
    file: D::DbFile,
    // (loc, len)
    index_block: Block<K, (u32, u32)>,
    current_block: Block<K, V>,
    state: ReaderState,
    pub sst_meta: SstMeta<K>,
    _marker: PhantomData<(K, V)>,
}

#[derive(Debug)]
enum ReaderState {
    LeftOfLoadedBlock,
    RightOfLoadedBlock,
}

impl<K, V, D> KVIter<K, V> for SstReader<K, V, D>
where
    K: Ord + Decode + Default + Clone + std::fmt::Debug,
    V: Decode + Default + std::fmt::Debug,
    D: DbDir,
{
    fn next(&mut self) -> Option<(&K, &V)> {
        if self.current_block.peek().is_some() {
            self.current_block.next()
        } else {
            match self.state {
                ReaderState::RightOfLoadedBlock => {
                    if !self.next_block().unwrap() {
                        self.state = ReaderState::LeftOfLoadedBlock;
                        None
                    } else {
                        self.state = ReaderState::RightOfLoadedBlock;
                        self.current_block.next()
                    }
                }
                ReaderState::LeftOfLoadedBlock => {
                    if !self.next_block().unwrap() || !self.next_block().unwrap() {
                        self.state = ReaderState::LeftOfLoadedBlock;
                        None
                    } else {
                        self.state = ReaderState::RightOfLoadedBlock;
                        self.current_block.next()
                    }
                }
            }
        }
    }

    fn peek(&mut self) -> Option<(&K, &V)> {
        if self.current_block.peek().is_some() {
            self.current_block.peek()
        } else {
            match self.state {
                ReaderState::RightOfLoadedBlock => {
                    if !self.next_block().unwrap() {
                        self.state = ReaderState::LeftOfLoadedBlock;
                        None
                    } else {
                        self.state = ReaderState::RightOfLoadedBlock;
                        self.current_block.peek()
                    }
                }
                ReaderState::LeftOfLoadedBlock => {
                    if !self.next_block().unwrap() || !self.next_block().unwrap() {
                        self.state = ReaderState::LeftOfLoadedBlock;
                        None
                    } else {
                        self.state = ReaderState::RightOfLoadedBlock;
                        self.current_block.peek()
                    }
                }
            }
        }
    }

    fn prev(&mut self) -> Option<(&K, &V)> {
        if self.current_block.peek_prev().is_some() {
            self.current_block.prev()
        } else {
            match self.state {
                ReaderState::LeftOfLoadedBlock => {
                    if !self.prev_block().unwrap() {
                        self.state = ReaderState::RightOfLoadedBlock;
                        None
                    } else {
                        self.state = ReaderState::LeftOfLoadedBlock;
                        self.current_block.prev()
                    }
                }
                ReaderState::RightOfLoadedBlock => {
                    if !self.prev_block().unwrap() || !self.prev_block().unwrap() {
                        self.state = ReaderState::RightOfLoadedBlock;
                        None
                    } else {
                        self.state = ReaderState::LeftOfLoadedBlock;
                        self.current_block.prev()
                    }
                }
            }
        }
    }

    fn peek_prev(&mut self) -> Option<(&K, &V)> {
        if self.current_block.peek_prev().is_some() {
            self.current_block.peek_prev()
        } else {
            match self.state {
                ReaderState::LeftOfLoadedBlock => {
                    if !self.prev_block().unwrap() {
                        self.state = ReaderState::RightOfLoadedBlock;
                        None
                    } else {
                        self.state = ReaderState::LeftOfLoadedBlock;
                        self.current_block.peek_prev()
                    }
                }
                ReaderState::RightOfLoadedBlock => {
                    if !self.prev_block().unwrap() || !self.prev_block().unwrap() {
                        self.state = ReaderState::RightOfLoadedBlock;
                        None
                    } else {
                        self.state = ReaderState::LeftOfLoadedBlock;
                        self.current_block.peek_prev()
                    }
                }
            }
        }
    }

    fn seek_ge(&mut self, key: &K) {
        self.index_block.seek_gt(key);
        if self.index_block.idx > 0 {
            self.index_block.idx -= 1;
        }
        // TODO: how to handle errors here without infecting the nice simple traits?
        self.next_block().unwrap();
        self.current_block.seek_ge(key);
        self.state = ReaderState::RightOfLoadedBlock;
    }

    fn start(&mut self) {
        self.index_block.align_start();
        self.state = ReaderState::LeftOfLoadedBlock;
        self.next_block().unwrap();
    }

    fn end(&mut self) {
        self.index_block.align_end();
        self.state = ReaderState::RightOfLoadedBlock;
        self.prev_block().unwrap();
        self.index_block.idx += 1;
        self.current_block.align_end();
    }
}

impl<K, V, D> SstReader<K, V, D>
where
    K: Decode + Default + Ord + Clone + std::fmt::Debug,
    V: Decode + Default + std::fmt::Debug,
    D: DbDir,
{
    pub fn print_state(&self) -> String {
        format!(
            "[{} {} {:?}]",
            self.index_block.idx, self.current_block.idx, self.state
        )
    }

    fn next_block(&mut self) -> anyhow::Result<bool> {
        match self.index_block.next() {
            None => {
                // Load the empty block.
                // TODO: just blank out the memory?
                self.current_block = Block::new();
                Ok(false)
            }
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
            None => {
                // Load the empty block.
                // TODO: just blank out the memory?
                self.current_block = Block::new();
                Ok(false)
            }
            Some((_k, (loc, len))) => {
                // TODO: check if loc is where we already are and don't move if so.
                self.file.seek(SeekFrom::Start(*loc as u64))?;
                self.current_block.load(&mut self.file, *len as u32)?;
                self.current_block.align_end();

                Ok(true)
            }
        }
    }

    pub fn load(mut file: D::DbFile) -> anyhow::Result<Self> {
        // First, read the length of the metadata.
        file.seek(SeekFrom::End(-4))?;
        let mut buf = [0_u8; 4];
        file.read_exact(&mut buf)?;
        let meta_len: i64 = u32::from_le_bytes(buf).into();

        file.seek(SeekFrom::End(-4 - meta_len))?;

        // TODO: write a length-prefixed helper.

        // TODO: can we derive 12 here instead of hardcoding it?
        let mut bounds_info = (0..(meta_len - 8)).map(|_| 0u8).collect::<Vec<_>>();
        file.read_exact(&mut bounds_info)?;

        let mut reader = KeyReader::new();
        reader.load(&bounds_info);

        let mut b = Block::<K, ()>::new();
        let len = bounds_info.len().try_into()?;
        b.load(&mut Cursor::new(bounds_info), len)?;

        let min_key = (*b.next().unwrap().0).clone();
        let max_key = (*b.next().unwrap().0).clone();

        let mut buf = [0_u8; 4];
        file.read_exact(&mut buf)?;
        // TODO: do we need this?
        let _data_len = u32::from_le_bytes(buf);

        file.read_exact(&mut buf)?;
        let index_len = u32::from_le_bytes(buf);

        // Load the index block into memory.
        file.seek(SeekFrom::End(-4 - (index_len as i64) - meta_len))?;
        let mut index_data: Vec<_> = (0..index_len).map(|_| 0).collect();
        file.read_exact(&mut index_data)?;
        let mut index_block = Block::new();
        let len = index_data.len() as u32;
        index_block.load(&mut Cursor::new(index_data), len)?;

        file.seek(SeekFrom::Start(0))?;

        let num_bytes = file.len();

        Ok(SstReader {
            file,
            current_block: Block::new(),
            index_block,
            state: ReaderState::RightOfLoadedBlock,
            sst_meta: SstMeta {
                min_key,
                max_key,
                num_bytes,
            },
            _marker: PhantomData,
        })
    }
}

#[cfg(test)]
mod test {
    use std::rc::Rc;

    use rand::Rng;

    use crate::{
        fs::{DbDir, MockDir},
        memtable::{KVIter, VecIter},
        sst::writer::SstWriter,
    };

    use super::SstReader;

    #[derive(Debug, Clone)]
    enum Op {
        Next,
        Peek,
        Prev,
        PeekPrev,
        Start,
        End,
    }

    fn results_match(data: &[((String, usize), Option<String>)], ops: &[Op], show: bool) -> bool {
        let mut dir = MockDir::new();

        let mut vec_iter = VecIter::new(Rc::new(data.to_vec()));

        let sst_fname = "/tmp/test_sst.sst";
        let file = dir.create(&sst_fname).unwrap();
        let data_source = vec_iter.clone();
        let writer = SstWriter::new(data_source, file);
        writer.write().unwrap();
        let mut reader: SstReader<(String, usize), Option<String>, MockDir> =
            SstReader::load(dir.open(&sst_fname).unwrap()).unwrap();

        let mut vec_result = Vec::new();
        let mut sst_result = Vec::new();
        for op in ops {
            match op {
                Op::Next => {
                    let next = vec_iter.next();
                    vec_result.push(next.map(|x| (x.0.clone(), x.1.clone())));

                    let next = reader.next();
                    sst_result.push(next.map(|x| (x.0.clone(), x.1.clone())));
                }
                Op::Peek => {
                    let next = vec_iter.peek();
                    vec_result.push(next.map(|x| (x.0.clone(), x.1.clone())));

                    let next = reader.peek();
                    sst_result.push(next.map(|x| (x.0.clone(), x.1.clone())));
                }
                Op::Prev => {
                    let next = vec_iter.prev();
                    vec_result.push(next.map(|x| (x.0.clone(), x.1.clone())));

                    let next = reader.prev();
                    sst_result.push(next.map(|x| (x.0.clone(), x.1.clone())));
                }
                Op::PeekPrev => {
                    let next = vec_iter.peek_prev();
                    vec_result.push(next.map(|x| (x.0.clone(), x.1.clone())));

                    let next = reader.peek_prev();
                    sst_result.push(next.map(|x| (x.0.clone(), x.1.clone())));
                }
                Op::Start => {
                    vec_iter.start();
                    reader.start();
                }
                Op::End => {
                    vec_iter.end();
                    reader.end();
                }
            }
        }
        if show {
            println!("vec result = {:?}", vec_result);
            println!("sst result = {:?}", sst_result);
        }

        vec_result == sst_result
    }

    #[test]
    fn reader_test() {
        let mut data: Vec<_> = (0..500)
            .map(|i| ((format!("key{}", i), i), Some(format!("val{}", i))))
            .collect();

        let mut r = rand::thread_rng();
        let mut ops = Vec::new();
        for _ in 0..500 {
            match r.gen_range(0..4) {
                0 => ops.push(Op::Next),
                1 => ops.push(Op::Prev),
                2 => ops.push(Op::Peek),
                3 => ops.push(Op::PeekPrev),
                4 => ops.push(Op::Start),
                5 => ops.push(Op::End),
                _ => unreachable!(),
            }
        }

        if !results_match(&data, &ops, false) {
            // Simplify the results.
            loop {
                let mut better = None;
                for i in 0..data.len() {
                    let mut new_data = data.clone();
                    new_data.remove(i);
                    if !results_match(&new_data, &ops, false) {
                        better = Some(new_data);
                        break;
                    }
                }
                if let Some(d) = better {
                    data = d;
                    continue;
                }

                let mut better = None;
                for i in 0..ops.len() {
                    let mut new_ops = ops.clone();
                    new_ops.remove(i);
                    if !results_match(&data, &new_ops, false) {
                        better = Some(new_ops);
                        break;
                    }
                }
                if let Some(o) = better {
                    ops = o;
                    continue;
                }

                break;
            }

            println!("results match: {}", results_match(&data, &ops, true));
            println!("{:?}", data);
            println!("{:?}", ops);
            panic!("results did not match")
        }
    }
}
