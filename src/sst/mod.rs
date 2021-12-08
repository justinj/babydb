use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
};

use crate::memtable::KVIter;

pub mod reader;
pub mod writer;

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

pub trait Encode: std::fmt::Debug {
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
