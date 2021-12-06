use std::io::{BufWriter, Write};

pub const BLOCK_SIZE: usize = 1 * 1024;

// TODO: for now just load this whole pupper into memory on creation. This is
// pretty bad but simple for now.

struct Reader {
    data: Vec<u8>,
    idx: usize,
}

impl Reader {
    fn new(data: Vec<u8>) -> Self {
        Reader { data, idx: 0 }
    }
    fn read_u32(&mut self) -> u32 {
        self.idx += 4;
        u32::from_le_bytes(self.data[(self.idx - 4)..self.idx].try_into().unwrap())
    }
    // TODO: this should use bytes::Bytes.
    fn read_bytes(&mut self, n: usize) -> Vec<u8> {
        self.idx += n;
        self.data[(self.idx - n)..self.idx].into()
    }
    fn at_end(&self) -> bool {
        self.idx >= self.data.len()
    }
}

pub struct BlockReader {
    // TODO(justin): when have wifi use bytes::Bytes.
    entries: Vec<Vec<u8>>,
}

impl BlockReader {
    fn load(data: Vec<u8>) -> Self {
        let mut reader = Reader::new(data);
        let mut entries = Vec::new();
        let mut buf = Vec::new();
        while !reader.at_end() {
            let shared_prefix = reader.read_u32() as usize;
            let len = reader.read_u32() as usize;

            buf.truncate(shared_prefix);
            buf.extend(reader.read_bytes(len));
            entries.push(buf.clone());
        }
        BlockReader { entries }
    }
}

pub struct BlockWriter<'a, W: Write> {
    w: &'a mut W,
    buf: Vec<u8>,
    written: usize,
}

const ENTRY_HEADER_LEN: usize = 4 + 4;

impl<'a, W: Write> BlockWriter<'a, W> {
    pub fn new(w: &'a mut W) -> Self {
        // TODO: reuse the buf across blocks.
        Self {
            w,
            buf: Vec::with_capacity(1024),
            written: 0,
        }
    }

    pub fn next_block(&mut self) -> anyhow::Result<()> {
        // TODO: buffer this?
        for _ in self.written..BLOCK_SIZE {
            self.w.write_all(&[0])?;
        }
        Ok(())
    }

    // Returns false if the new data does not fit in the block, and was not written.
    pub fn insert(&mut self, data: &[u8]) -> anyhow::Result<bool> {
        let n = std::cmp::min(data.len(), self.buf.len());
        let mut shared_prefix_len = n;
        for i in 0..n {
            if data[i] != self.buf[i] {
                shared_prefix_len = i;
                break;
            }
        }
        if self.written + ENTRY_HEADER_LEN + data.len() - shared_prefix_len >= BLOCK_SIZE {
            return Ok(false);
        }

        self.w
            .write_all(&(shared_prefix_len as u32).to_le_bytes())?;
        self.w
            .write_all(&((data.len() - shared_prefix_len) as u32).to_le_bytes())?;
        self.w.write_all(&data[shared_prefix_len..])?;

        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use std::io::BufWriter;

    use super::{BlockReader, BlockWriter};

    #[test]
    fn test_block() {
        let writes = vec![
            "bar".to_owned(),
            "barbar".to_owned(),
            "foo".to_owned(),
            "foobar".to_owned(),
        ];

        let mut data = Vec::new();
        let mut bufdata = BufWriter::new(&mut data);
        let mut writer = BlockWriter::new(&mut bufdata);
        for w in &writes {
            writer.insert(w.as_bytes()).unwrap();
        }

        drop(bufdata);

        let block = BlockReader::load(data);

        let reads = block
            .entries
            .into_iter()
            .map(|x| String::from_utf8(x).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(writes, reads);
    }
}
