use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::prelude::AsRawFd,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub struct Root<T>
where
    T: Serialize + DeserializeOwned + Default,
{
    dir: String,
    data: T,
}

impl<T> Root<T>
where
    T: Serialize + DeserializeOwned + Default,
{
    pub fn load(dir: String) -> anyhow::Result<Self> {
        // TODO: real path separator.
        match fs::read_to_string(Self::path(dir.as_str()).as_str()) {
            Ok(contents) => Ok(Self {
                dir,
                data: serde_json::from_str(contents.as_str())?,
            }),
            Err(_) => {
                // TODO: check if this is a "did not exist" error
                let data = T::default();
                let mut result = Self { dir, data };
                result.write(T::default())?;
                Ok(result)
            }
        }
    }

    fn path(dir: &str) -> String {
        format!("{}/ROOT", dir)
    }

    fn tmp_path(dir: &str) -> String {
        format!("{}/ROOT_TMP", dir)
    }

    fn write(&mut self, t: T) -> anyhow::Result<()> {
        let tmp_path = Self::tmp_path(self.dir.as_str());
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&tmp_path)?;
        let encoded = serde_json::to_string(&t)?;
        file.write_all(encoded.as_bytes())?;
        file.flush()?;
        // fsync(file)?;
        // fsync_dir(self.dir.as_str())?;
        let path = Self::path(self.dir.as_str());

        fs::rename(tmp_path, path)?;
        self.data = t;

        Ok(())
    }
}

// extern crate nix;

// Cribbed from https://github.com/untitaker/rust-atomicwrites/blob/master/src/lib.rs.
// fn fsync<T: AsRawFd>(f: T) -> anyhow::Result<()> {
//     Ok(nix::unistd::fsync(f.as_raw_fd())?)
// }

// fn fsync_dir(x: &str) -> anyhow::Result<()> {
//     let f = fs::File::open(x)?;
//     fsync(f)
// }
