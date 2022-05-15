use std::{
    cell::RefCell,
    collections::HashMap,
    io::{self, Read, Seek},
    path::{Path, PathBuf},
    rc::Rc,
};

use anyhow::bail;

pub trait DbFile: std::fmt::Debug + Read + Seek {
    fn write(&mut self, buf: &[u8]) -> anyhow::Result<()>;
    fn sync(&mut self) -> anyhow::Result<()>;
    fn read_all(&self) -> Vec<u8>;
    fn len(&self) -> usize;
}

pub trait DbDir: Clone {
    type DbFile: DbFile;

    fn cd<P>(&mut self, dir_name: &P) -> Self
    where
        P: AsRef<Path>;

    fn unlink<P>(&mut self, fname: &P) -> anyhow::Result<bool>
    where
        P: AsRef<Path>;

    fn ls(&mut self) -> Vec<String>;

    fn create<P>(&mut self, fname: &P) -> anyhow::Result<Option<Self::DbFile>>
    where
        P: AsRef<Path>;

    fn open<P>(&mut self, fname: &P) -> Option<Self::DbFile>
    where
        P: AsRef<Path>;

    fn rename<P, Q>(&mut self, from: &P, to: &Q) -> anyhow::Result<()>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>;
}

// Mock Implementation
#[derive(Default, Debug)]
struct MockData {
    synced: Vec<u8>,
    unsynced: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct MockFile {
    idx: usize,
    pub file_id: FileId,
    fs: Rc<RefCell<MockFs>>,
}

impl MockFile {
    #[allow(unused)]
    fn read_all_synced(&self) -> Vec<u8> {
        (*self.fs).borrow_mut().data[self.file_id].synced.clone()
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
    fn write(&mut self, buf: &[u8]) -> anyhow::Result<()> {
        let data = buf.to_vec();
        (*self.fs)
            .borrow_mut()
            .write(self.file_id, self.idx, data)?;
        self.idx += buf.len();

        Ok(())
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        (*self.fs).borrow_mut().sync(self.file_id)?;
        Ok(())
    }

    fn read_all(&self) -> Vec<u8> {
        (*self.fs).borrow_mut().data[self.file_id].unsynced.clone()
    }

    fn len(&self) -> usize {
        (*self.fs).borrow().stat(self.file_id).len
    }
}

struct FileMeta {
    len: usize,
}

#[derive(Clone, Debug)]
pub struct MockDir {
    pub fs: Rc<RefCell<MockFs>>,
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

    fn unlink<P>(&mut self, fname: &P) -> anyhow::Result<bool>
    where
        P: AsRef<Path>,
    {
        (*self.fs).borrow_mut().unlink(&self.full_path(fname))
    }

    fn ls(&mut self) -> Vec<String> {
        let fnames: Vec<String> = (*self.fs)
            .borrow_mut()
            .names
            .keys()
            .filter(|f| f.starts_with(&self.prefix.join("/")))
            .cloned()
            .collect();
        (*self.fs).borrow_mut().record(Event::Ls(fnames.clone()));
        fnames
    }

    fn create<P>(&mut self, fname: &P) -> anyhow::Result<Option<Self::DbFile>>
    where
        P: AsRef<Path>,
    {
        Ok((*self.fs)
            .borrow_mut()
            .create(&self.full_path(fname))?
            .map(|file_id| MockFile {
                fs: self.fs.clone(),
                file_id,
                idx: 0,
            }))
    }

    fn open<P>(&mut self, fname: &P) -> Option<Self::DbFile>
    where
        P: AsRef<Path>,
    {
        (*self.fs)
            .borrow_mut()
            .open(&self.full_path(fname))
            .map(|file_id| MockFile {
                fs: self.fs.clone(),
                file_id,
                idx: 0,
            })
    }

    fn rename<P, Q>(&mut self, from: &P, to: &Q) -> anyhow::Result<()>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        (*self.fs)
            .borrow_mut()
            .rename(&self.full_path(from), &self.full_path(to))
    }
}

type FileId = usize;

#[derive(Debug, Clone)]
pub enum Event {
    Create(String, FileId),
    Write(FileId, usize, Vec<u8>),
    Sync(FileId),
    Rename(String, String),
    Unlink(String),
    Open(String),
    Ls(Vec<String>),
}

impl Event {
    #[allow(unused)]
    pub fn write_abbrev<W: std::fmt::Write>(&self, w: &mut W) -> std::fmt::Result {
        match self {
            Event::Create(name, file_id) => {
                write!(w, "Create({}, {})", name, file_id)?;
            }
            Event::Write(file_id, idx, contents) => {
                write!(w, "Write({}, {}, ", file_id, idx)?;
                write!(
                    w,
                    "{})",
                    String::from_utf8(
                        contents
                            .iter()
                            .flat_map(|ch| std::ascii::escape_default(*ch))
                            .collect::<Vec<u8>>()
                    )
                    .unwrap()
                )?;
            }
            Event::Sync(file_id) => {
                write!(w, "Sync({})", file_id)?;
            }
            Event::Rename(from, to) => {
                write!(w, "Rename({}, {})", from, to)?;
            }
            Event::Unlink(name) => {
                write!(w, "Unlink({})", name)?;
            }
            Event::Open(name) => {
                write!(w, "Open({})", name)?;
            }
            Event::Ls(names) => {
                write!(w, "Ls() -> {:?}", names)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MockFs {
    names: HashMap<String, FileId>,
    data: Vec<MockData>,
    events: Vec<Event>,

    // After this many "things happen," "crash" the FS, meaning stop accepting
    // writes and discard any unsynced data.
    time_to_crash: Option<usize>,
}

impl MockFs {
    fn new() -> Self {
        MockFs {
            names: HashMap::new(),
            data: Vec::new(),
            events: Vec::new(),
            time_to_crash: None,
        }
    }

    fn check_crashed(&self) -> anyhow::Result<()> {
        if self.time_to_crash == Some(0) {
            bail!("filesystem is down")
        } else {
            Ok(())
        }
    }

    #[allow(unused)]
    pub fn schedule_crash(&mut self, ops: usize) {
        self.time_to_crash = Some(ops);
    }

    // Discard all unsynced state, become uncrashed.
    #[allow(unused)]
    pub fn reboot(&mut self) {
        for f in self.data.iter_mut() {
            f.unsynced.clear();
            f.unsynced.extend(&f.synced);
        }
        self.time_to_crash = None;
    }

    fn perform_op(&mut self) -> anyhow::Result<()> {
        self.check_crashed()?;
        match self.time_to_crash {
            None | Some(0) => {}
            Some(x) => {
                if x > 0 {
                    self.time_to_crash = Some(x - 1);
                }
            }
        }
        Ok(())
    }

    fn record(&mut self, e: Event) {
        self.events.push(e);
    }

    #[allow(unused)]
    pub fn iter_events(&self) -> impl Iterator<Item = &Event> {
        self.events.iter()
    }

    #[allow(unused)]
    pub fn take_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }

    fn stat(&self, file: FileId) -> FileMeta {
        FileMeta {
            len: self.data[file].unsynced.len(),
        }
    }
}

impl MockFs {
    // TODO: support various writing modes?
    fn create<P>(&mut self, fname: &P) -> anyhow::Result<Option<FileId>>
    where
        P: AsRef<Path>,
    {
        self.perform_op()?;

        let path = fname.as_ref().to_str().unwrap().to_owned();
        let id = match self.names.get(&path) {
            Some(_) => {
                return Ok(None);
            }
            None => {
                let data = MockData::default();
                let id = self.data.len();

                self.record(Event::Create(path.clone(), id));

                self.names.insert(path, id);
                self.data.push(data);
                id
            }
        };

        Ok(Some(id))
    }

    fn unlink<P>(&mut self, fname: &P) -> anyhow::Result<bool>
    where
        P: AsRef<Path>,
    {
        self.perform_op()?;

        let path = fname.as_ref().to_str().unwrap();
        self.record(Event::Unlink(path.to_owned()));
        Ok(self.names.remove(path).is_some())
    }

    fn open<P>(&mut self, fname: &P) -> Option<FileId>
    where
        P: AsRef<Path>,
    {
        let path = fname.as_ref().to_str().unwrap().to_owned();
        self.record(Event::Open(path.to_owned()));
        self.names.get(&path).cloned()
    }

    fn rename<P, Q>(&mut self, from: &P, to: &Q) -> anyhow::Result<()>
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        self.perform_op()?;

        let from = from.as_ref().to_str().unwrap().to_owned();
        let to = to.as_ref().to_str().unwrap().to_owned();

        self.record(Event::Rename(from.clone(), to.clone()));

        if let Some(d) = self.names.remove(&from) {
            self.names.insert(to, d);
        }

        Ok(())
    }

    fn write(&mut self, file: FileId, idx: usize, data: Vec<u8>) -> anyhow::Result<()> {
        self.perform_op()?;

        while self.data[file].unsynced.len() < idx + data.len() {
            self.data[file].unsynced.push(0);
        }

        self.data[file].unsynced[idx..].copy_from_slice(&data);

        self.record(Event::Write(file, idx, data));
        Ok(())
    }

    fn sync(&mut self, file: FileId) -> anyhow::Result<()> {
        self.perform_op()?;

        self.record(Event::Sync(file));
        let d = &mut self.data[file];
        d.synced = d.unsynced.clone();

        Ok(())
    }
}

#[test]
fn test_mock_file() -> anyhow::Result<()> {
    let mut dir = MockDir::new();

    let mut a = dir.create(&"a")?.unwrap();

    a.write(&[1, 2, 3, 4]).unwrap();

    assert_eq!(Vec::<u8>::new(), a.read_all_synced());
    assert_eq!(vec![1, 2, 3, 4], a.read_all());

    a.sync().unwrap();

    assert_eq!(vec![1, 2, 3, 4], a.read_all_synced());
    assert_eq!(vec![1, 2, 3, 4], a.read_all());

    Ok(())
}
