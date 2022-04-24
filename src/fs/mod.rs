use std::{
    cell::RefCell,
    collections::HashMap,
    io::{self, Read, Seek},
    path::{Path, PathBuf},
    rc::Rc,
};

pub trait DbFile: std::fmt::Debug + Read + Seek {
    fn write(&mut self, buf: &[u8]) -> io::Result<()>;
    fn sync(&mut self) -> io::Result<()>;
    fn read_all(&self) -> Vec<u8>;
}

pub trait DbDir: Clone {
    type DbFile: DbFile;

    fn cd<P>(&mut self, dir_name: &P) -> Self
    where
        P: AsRef<Path>;

    fn unlink<P>(&mut self, fname: &P) -> bool
    where
        P: AsRef<Path>;

    fn ls(&mut self) -> Vec<String>;

    // TODO: these would be better as Results I think.
    fn create<P>(&mut self, fname: &P) -> Option<Self::DbFile>
    where
        P: AsRef<Path>;

    fn open<P>(&mut self, fname: &P) -> Option<Self::DbFile>
    where
        P: AsRef<Path>;

    // TODO: should this return an error?
    fn rename<P, Q>(&mut self, from: &P, to: &Q)
    where
        P: AsRef<Path>,
        Q: AsRef<Path>;
}

// Mock Implementation
#[derive(Default, Debug)]
struct MockData {
    synced: Vec<u8>,
    unsynced: Vec<(usize, Vec<u8>)>,
}

#[derive(Clone, Debug)]
pub struct MockFile {
    idx: usize,
    data: Rc<RefCell<MockData>>,
}

impl MockFile {
    #[allow(unused)]
    fn read_all_unsynced(&self) -> Vec<u8> {
        (*self.data).borrow().synced.clone()
    }
}

impl Seek for MockFile {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        match pos {
            io::SeekFrom::Start(i) => {
                self.idx = i.try_into().unwrap();
            }
            io::SeekFrom::End(i) => {
                // TODO: don't read the whole thing here
                // TODO: these numeral types are effed
                self.idx = ((self.read_all().len() as i64) + i).try_into().unwrap();
            }
            io::SeekFrom::Current(x) => {
                // TODO: What the hell how do I do this right
                if x > 0 {
                    self.idx += x as usize;
                } else {
                    self.idx -= x as usize
                }
            }
        }
        Ok(self.idx.try_into().unwrap())
    }
}

impl Read for MockFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // TODO: cache the materialized version of the full data.
        let data = self.read_all();
        let min_len = std::cmp::min(data.len() - self.idx, buf.len());
        buf[..min_len].copy_from_slice(&data[self.idx..self.idx + min_len]);
        self.idx += min_len;
        Ok(min_len)
    }
}

impl DbFile for MockFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        let data = buf.to_vec();
        self.idx += data.len();
        (*self.data)
            .borrow_mut()
            .unsynced
            .push((self.idx - data.len(), data));
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        let mut d = (*self.data).borrow_mut();
        let mut unsynced = std::mem::take(&mut d.unsynced);
        for (idx, data) in unsynced.drain(..) {
            for (i, b) in data.into_iter().enumerate() {
                while i + idx >= d.synced.len() {
                    d.synced.push(0);
                }
                d.synced[i + idx] = b;
            }
        }
        d.unsynced = unsynced;
        Ok(())
    }

    fn read_all(&self) -> Vec<u8> {
        let mut d = (*self.data).borrow_mut();
        let mut out = d.synced.clone();
        for (idx, data) in d.unsynced.drain(..) {
            for (i, b) in data.into_iter().enumerate() {
                while i + idx >= out.len() {
                    out.push(0);
                }
                out[i + idx] = b;
            }
        }
        out
    }
}

#[derive(Clone, Debug)]
pub struct MockDir {
    fs: Rc<RefCell<MockFs>>,
    prefix: Vec<String>,
}

impl MockDir {
    #[allow(unused)]
    pub fn new() -> Self {
        MockDir {
            fs: Rc::new(RefCell::new(MockFs::new())),
            prefix: Vec::new(),
        }
    }

    fn full_path<P>(&self, p: &P) -> PathBuf
    where
        P: AsRef<Path>,
    {
        self.prefix
            .iter()
            .cloned()
            .chain(p.as_ref().iter().map(|s| s.to_str().unwrap().to_owned()))
            .collect()
    }
}

impl DbDir for MockDir {
    type DbFile = MockFile;

    fn cd<P>(&mut self, dir_name: &P) -> Self
    where
        P: AsRef<Path>,
    {
        MockDir {
            fs: self.fs.clone(),
            prefix: self
                .prefix
                .iter()
                .cloned()
                .chain(
                    dir_name
                        .as_ref()
                        .iter()
                        .map(|s| s.to_str().unwrap().to_owned()),
                )
                .collect(),
        }
    }

    fn unlink<P>(&mut self, fname: &P) -> bool
    where
        P: AsRef<Path>,
    {
        (*self.fs).borrow_mut().unlink(&self.full_path(fname))
    }

    fn ls(&mut self) -> Vec<String> {
        (*self.fs)
            .borrow_mut()
            .names
            .keys()
            .filter(|f| f.starts_with(&self.prefix.join("/")))
            .cloned()
            .collect()
    }

    fn create<P>(&mut self, fname: &P) -> Option<Self::DbFile>
    where
        P: AsRef<Path>,
    {
        (*self.fs).borrow_mut().create(&self.full_path(fname))
    }

    fn open<P>(&mut self, fname: &P) -> Option<Self::DbFile>
    where
        P: AsRef<Path>,
    {
        (*self.fs).borrow_mut().open(&self.full_path(fname))
    }

    fn rename<P, Q>(&mut self, from: &P, to: &Q)
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        (*self.fs)
            .borrow_mut()
            .rename(&self.full_path(from), &self.full_path(to))
    }
}

#[derive(Debug)]
struct MockFs {
    names: HashMap<String, usize>,
    data: Vec<Rc<RefCell<MockData>>>,
}

impl MockFs {
    fn new() -> Self {
        MockFs {
            names: HashMap::new(),
            data: Vec::new(),
        }
    }
}

impl MockFs {
    // TODO: support various writing modes?
    fn create<P>(&mut self, fname: &P) -> Option<MockFile>
    where
        P: AsRef<Path>,
    {
        let path = fname.as_ref().to_str().unwrap().to_owned();
        let id = match self.names.get(&path) {
            Some(_) => {
                return None;
            }
            None => {
                let data = Rc::new(RefCell::new(MockData::default()));
                let id = self.data.len();
                self.names.insert(path, id);
                self.data.push(data);
                id
            }
        };

        Some(MockFile {
            idx: 0,
            data: self.data[id].clone(),
        })
    }

    fn unlink<P>(&mut self, fname: &P) -> bool
    where
        P: AsRef<Path>,
    {
        let path = fname.as_ref().to_str().unwrap();
        self.names.remove(path).is_some()
    }

    fn open<P>(&mut self, fname: &P) -> Option<MockFile>
    where
        P: AsRef<Path>,
    {
        let path = fname.as_ref().to_str().unwrap().to_owned();
        let id = match self.names.get(&path) {
            Some(id) => *id,
            None => {
                return None;
            }
        };

        Some(MockFile {
            idx: 0,
            data: self.data[id].clone(),
        })
    }

    fn rename<P, Q>(&mut self, from: &P, to: &Q)
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        let from = from.as_ref().to_str().unwrap().to_owned();
        let to = to.as_ref().to_str().unwrap().to_owned();

        if let Some(d) = self.names.remove(&from) {
            self.names.insert(to, d);
        }
    }
}

#[test]
fn test_mock_file() {
    let mut fs = MockFs::new();

    let mut a = fs.create(&"a").unwrap();

    a.write(&[1, 2, 3, 4]).unwrap();
    a.sync().unwrap();

    panic!("{:?} {:?}", a.read_all_unsynced(), a.read_all());
}
