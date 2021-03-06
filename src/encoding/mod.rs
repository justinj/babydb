const SEPARATOR: [u8; 2] = [0x00, 0x01];
const ESCAPED_00: [u8; 2] = [0x00, 0xff];

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

#[derive(Debug)]
pub struct KeyWriter {
    pub(crate) buf: Vec<u8>,
}

impl KeyWriter {
    pub fn new() -> Self {
        KeyWriter { buf: Vec::new() }
    }

    pub fn clear(&mut self) {
        self.buf.clear()
    }

    pub fn replace(&mut self, mut v: Vec<u8>) -> Vec<u8> {
        std::mem::swap(&mut v, &mut self.buf);
        v
    }

    fn write(&mut self, buf: &[u8]) {
        copy_escaped(buf, &mut self.buf);
    }

    fn write_fixed_size(&mut self, buf: &[u8]) {
        self.buf.extend(buf);
    }

    fn separator(&mut self) {
        self.buf.extend([0x00, 0x01]);
    }
}

// TODO: use bytes::bytes?
// TODO: does this need to be pub?
pub struct KeyReader {
    buf: Vec<u8>,
    from: usize,
    scratch: Vec<u8>,
}

impl KeyReader {
    pub fn new() -> Self {
        KeyReader {
            buf: Vec::new(),
            from: 0,
            scratch: Vec::new(),
        }
    }

    pub fn buf_mut(&mut self) -> &mut Vec<u8> {
        self.scratch.clear();
        self.from = 0;
        &mut self.buf
    }

    pub fn load(&mut self, buf: &[u8]) {
        self.buf.extend(buf);
        self.from = 0;
        self.scratch.clear();
    }

    pub fn next(&mut self) -> &[u8] {
        // First, find the separator.
        let split_position = self.buf[self.from..]
            .windows(2)
            .position(|x| x == SEPARATOR)
            .unwrap_or(self.buf.len() - self.from);

        self.scratch.clear();
        copy_unescaped(
            &self.buf[self.from..self.from + split_position],
            &mut self.scratch,
        );
        self.from += split_position + 2;

        &self.scratch
    }

    pub fn next_fixed_size(&mut self, n: usize) -> &[u8] {
        let from = self.from;
        self.from += n;
        &self.buf[from..from + n]
    }
}

pub trait Encode: std::fmt::Debug {
    fn write_bytes(&self, kw: &mut KeyWriter);
    fn needs_delimiter(&self) -> bool {
        true
    }
}

pub trait Decode: Sized {
    fn decode(kr: &mut KeyReader) -> anyhow::Result<Self>;
}

impl<T: Encode> Encode for Vec<T> {
    fn write_bytes(&self, kw: &mut KeyWriter) {
        // TODO: could we do this with separators somehow?
        kw.write(&self.len().to_le_bytes());
        for v in self {
            v.write_bytes(kw);
        }
    }
}

impl Encode for () {
    fn write_bytes(&self, _: &mut KeyWriter) {}
    fn needs_delimiter(&self) -> bool {
        false
    }
}

impl Decode for () {
    fn decode(_: &mut KeyReader) -> anyhow::Result<Self> {
        Ok(())
    }
}

impl Encode for String {
    fn write_bytes(&self, kw: &mut KeyWriter) {
        kw.write(self.as_bytes())
    }
}

impl Decode for String {
    fn decode(kr: &mut KeyReader) -> anyhow::Result<Self> {
        let result = String::from_utf8(kr.next().to_vec())?;
        Ok(result)
    }
}

impl Encode for usize {
    fn write_bytes(&self, kw: &mut KeyWriter) {
        kw.write_fixed_size(&self.to_le_bytes())
    }

    fn needs_delimiter(&self) -> bool {
        false
    }
}

impl Decode for usize {
    fn decode(kr: &mut KeyReader) -> anyhow::Result<Self> {
        let next = kr.next_fixed_size((usize::BITS / 8).try_into()?);
        Ok(Self::from_le_bytes(next.try_into()?))
    }
}

impl Encode for u8 {
    fn write_bytes(&self, kw: &mut KeyWriter) {
        kw.write(&self.to_le_bytes())
    }
}

impl Decode for u8 {
    fn decode(kr: &mut KeyReader) -> anyhow::Result<Self> {
        let next = kr.next();
        Ok(Self::from_le_bytes(next.try_into()?))
    }
}

impl Encode for u32 {
    fn write_bytes(&self, kw: &mut KeyWriter) {
        kw.write_fixed_size(&self.to_le_bytes())
    }

    fn needs_delimiter(&self) -> bool {
        false
    }
}

impl Decode for u32 {
    fn decode(kr: &mut KeyReader) -> anyhow::Result<Self> {
        Ok(Self::from_le_bytes(kr.next_fixed_size(4).try_into()?))
    }
}

impl<A> Encode for &A
where
    A: Encode,
{
    fn write_bytes(&self, kw: &mut KeyWriter) {
        (*self).write_bytes(kw)
    }

    fn needs_delimiter(&self) -> bool {
        (*self).needs_delimiter()
    }
}

impl<A, B> Encode for (A, B)
where
    A: Encode,
    B: Encode,
{
    fn write_bytes(&self, kw: &mut KeyWriter) {
        self.0.write_bytes(kw);
        if self.0.needs_delimiter() {
            kw.separator();
        }
        self.1.write_bytes(kw);
    }

    fn needs_delimiter(&self) -> bool {
        self.1.needs_delimiter()
    }
}

impl<A, B> Decode for (A, B)
where
    A: Decode + std::fmt::Debug,
    B: Decode + std::fmt::Debug,
{
    fn decode(kr: &mut KeyReader) -> anyhow::Result<Self> {
        let a = A::decode(kr)?;
        let b = B::decode(kr)?;
        Ok((a, b))
    }
}

impl<A> Encode for Option<A>
where
    A: Encode,
{
    fn write_bytes(&self, kw: &mut KeyWriter) {
        match self {
            None => {
                kw.write_fixed_size(&[0]);
            }
            Some(v) => {
                kw.write_fixed_size(&[1]);
                v.write_bytes(kw);
            }
        }
    }

    fn needs_delimiter(&self) -> bool {
        match self {
            Some(x) => x.needs_delimiter(),
            None => false,
        }
    }
}

impl<A> Decode for Option<A>
where
    A: Decode,
{
    fn decode(kr: &mut KeyReader) -> anyhow::Result<Self> {
        let buf = kr.next_fixed_size(1);
        match buf[0] {
            0 => Ok(None),
            1 => Ok(Some(A::decode(kr)?)),
            _ => panic!(),
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
