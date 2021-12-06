use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
};

use crate::memtable::KVIter;

use self::block::BlockWriter;

mod block;

const SEPARATOR: [u8; 2] = [0x00, 0x01];
const ESCAPED_00: [u8; 2] = [0x00, 0xff];

// The SST format is a simplified, less featureful version of that of pebble.

fn copy_escaped(mut from: &[u8], to: &mut Vec<u8>) {
    while !from.is_empty() {
        // TODO: faster/more specialized way to do this?
        match from.iter().position(|x| *x == 0x00) {
            Some(idx) => {
                to.extend(&from[..idx + 1]);
                to.push(0xff);
                from = &from[(idx + 1)..];
            }
            None => {
                to.extend(from);
                return;
            }
        }
    }
}

fn copy_unescaped(mut from: &[u8], to: &mut Vec<u8>) {
    while !from.is_empty() {
        // TODO: faster/more specialized way to do this?
        match from.windows(2).position(|w| w == ESCAPED_00) {
            Some(idx) => {
                to.extend(&from[..idx + 1]);
                from = &from[(idx + 2)..];
            }
            None => {
                to.extend(from);
                from = &from[from.len()..];
            }
        }
    }
}

#[test]
fn test_escaping() {
    for str in [
        vec![0x00_u8, 0x00, 0x01, 0x02, 0x00],
        vec![0x01, 0x01, 0x00],
        vec![],
    ] {
        let mut out = Vec::new();
        copy_escaped(&str, &mut out);
        let mut out2 = Vec::new();
        copy_unescaped(&out, &mut out2);
        assert_eq!(str, out2);
    }
}

pub trait Encode {
    fn write_bytes(&self, buf: &mut Vec<u8>, scratch: &mut Vec<u8>);
}

pub trait Decode: Sized {
    fn decode(buf: &[u8], scratch: &mut Vec<u8>) -> anyhow::Result<Self>;
}

impl Encode for String {
    fn write_bytes(&self, buf: &mut Vec<u8>, _scratch: &mut Vec<u8>) {
        buf.extend(self.as_bytes())
    }
}

impl Decode for String {
    fn decode(buf: &[u8], _scratch: &mut Vec<u8>) -> anyhow::Result<Self> {
        let result = String::from_utf8(buf.to_vec())?;
        Ok(result)
    }
}

impl Encode for usize {
    fn write_bytes(&self, buf: &mut Vec<u8>, _scratch: &mut Vec<u8>) {
        buf.extend(self.to_ne_bytes())
    }
}

impl Decode for usize {
    fn decode(buf: &[u8], _scratch: &mut Vec<u8>) -> anyhow::Result<Self> {
        Ok(Self::from_le_bytes(buf.try_into()?))
    }
}

impl Encode for u32 {
    fn write_bytes(&self, buf: &mut Vec<u8>, scratch: &mut Vec<u8>) {
        buf.extend(self.to_ne_bytes())
    }
}

impl Decode for u32 {
    fn decode(buf: &[u8], _scratch: &mut Vec<u8>) -> anyhow::Result<Self> {
        Ok(Self::from_le_bytes(buf.try_into()?))
    }
}

impl<A> Encode for &A
where
    A: Encode,
{
    fn write_bytes(&self, buf: &mut Vec<u8>, scratch: &mut Vec<u8>) {
        (*self).write_bytes(buf, scratch)
    }
}

impl<A, B> Encode for (A, B)
where
    A: Encode,
    B: Encode,
{
    fn write_bytes(&self, buf: &mut Vec<u8>, scratch: &mut Vec<u8>) {
        self.0.write_bytes(buf, scratch);
        scratch.clear();
        std::mem::swap(buf, scratch);
        copy_escaped(scratch, buf);
        scratch.clear();
        buf.extend(SEPARATOR);
        self.1.write_bytes(buf, scratch);
    }
}

impl<A, B> Decode for (A, B)
where
    A: Decode,
    B: Decode,
{
    fn decode(buf: &[u8], scratch: &mut Vec<u8>) -> anyhow::Result<Self> {
        // First, find the separator.
        let split_position = buf
            .windows(2)
            .position(|x| x == SEPARATOR)
            .expect("tuple should have separator");

        scratch.clear();
        copy_unescaped(&buf[0..split_position], scratch);

        // TODO: can we get rid of the Vec::new()? As long as tuples are well-formed it shouldn't
        // actually allocate, I think...
        let fst = A::decode(scratch, &mut Vec::new())?;
        scratch.clear();
        let snd = B::decode(&buf[split_position + 2..], scratch)?;

        Ok((fst, snd))
    }
}

impl<A> Encode for Option<A>
where
    A: Encode,
{
    fn write_bytes(&self, buf: &mut Vec<u8>, scratch: &mut Vec<u8>) {
        match self {
            None => {
                buf.push(0);
            }
            Some(v) => {
                buf.push(1);
                v.write_bytes(buf, scratch);
            }
        }
    }
}

impl<A> Decode for Option<A>
where
    A: Decode,
{
    fn decode(buf: &[u8], scratch: &mut Vec<u8>) -> anyhow::Result<Self> {
        match buf[0] {
            0 => Ok(None),
            1 => Ok(Some(A::decode(&buf[1..], scratch)?)),
            _ => panic!(),
        }
    }
}

const RESET_INTERVAL: usize = 10;

struct Writer<W>
where
    W: Write,
{
    w: W,
    prev_val: Vec<u8>,
    scratch: Vec<u8>,
    buf: Vec<u8>,
    written: usize,
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
            written: 0,
        }
    }

    fn location(&self) -> usize {
        self.written
    }

    fn reset(&mut self) {
        self.prev_val.clear();
    }

    fn write_bytes_raw(&mut self, buf: &[u8]) -> anyhow::Result<()> {
        self.written += buf.len();
        self.w.write_all(buf)?;
        Ok(())
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

        self.write_bytes_raw(&((self.buf.len() - shared_prefix_len) as u32).to_le_bytes())?;
        self.write_bytes_raw(&(shared_prefix_len as u32).to_le_bytes())?;
        let to_write = &self.buf[shared_prefix_len..];
        self.written += to_write.len();
        self.w.write_all(to_write)?;

        std::mem::swap(&mut self.buf, &mut self.prev_val);

        self.buf.clear();
        Ok(())
    }
}

struct Reader<T: Decode, R: Seek + Read> {
    r: R,
    data_len: u64,
    idx: u64,
    buf: Vec<u8>,
    scratch: Vec<u8>,
    _marker: PhantomData<T>,
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
    (K, V): Decode,
{
    reader: Reader<(K, V), File>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> SstReader<K, V>
where
    (K, V): Decode,
{
    pub fn load(fname: &str) -> anyhow::Result<Self> {
        let mut file = OpenOptions::new().read(true).open(fname)?;

        file.seek(SeekFrom::End(-4))?;
        let mut buf = [0_u8; 4];
        file.read_exact(&mut buf)?;
        let data_len = u32::from_le_bytes(buf);

        file.seek(SeekFrom::End(-8))?;
        file.read_exact(&mut buf)?;
        let index_len = u32::from_le_bytes(buf);

        file.seek(SeekFrom::End(-8 - (index_len as i64)))?;
        let mut index_data: Vec<_> = (0..index_len).map(|_| 0).collect();
        file.read_exact(&mut index_data)?;

        file.seek(SeekFrom::Start(0))?;

        Ok(SstReader {
            reader: Reader::new(file, data_len),
            _marker: PhantomData,
        })
    }

    pub fn next(&mut self) -> anyhow::Result<Option<(K, V)>> {
        self.reader.next()
    }
}

pub struct SstWriter<I, K, V>
where
    I: KVIter<K, V>,
    K: Ord + Encode,
    V: Encode,
{
    writer: Writer<File>,
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
            .create(true)
            .open(fname)
            .unwrap();
        let writer = Writer::new(file);
        SstWriter {
            writer,
            it,
            _marker: PhantomData,
        }
    }

    pub fn write(mut self) -> anyhow::Result<()> {
        let mut index = Vec::new();
        let mut index_writer = Writer::new(&mut index);

        let mut written = 0;

        while let Some(kv) = self.it.next() {
            written += 1;

            if written >= RESET_INTERVAL {
                index_writer.write(&(kv.0, self.writer.location()))?;
                written = 0;
                self.writer.reset();
            }

            self.writer.write(&kv)?;
        }

        let data_length = self.writer.location() as u32;

        // Write the index block.
        self.writer.write_bytes_raw(&index)?;
        // Write the length of the data block.
        self.writer.write_bytes_raw(&data_length.to_le_bytes())?;
        // Write the length of the index block.
        self.writer
            .write_bytes_raw(&(index.len() as u32).to_le_bytes())?;
        Ok(())
    }
}
