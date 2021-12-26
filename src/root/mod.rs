use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{de::DeserializeOwned, Serialize};

pub struct Root<T>
where
    T: Serialize + DeserializeOwned + Default,
{
    dir: PathBuf,
    pub(crate) data: T,
}

impl<T> Root<T>
where
    T: Serialize + DeserializeOwned + Default,
{
    pub fn load<P>(dir: P) -> anyhow::Result<Self>
    where
        P: AsRef<Path> + Into<PathBuf>,
    {
        match fs::read_to_string(Self::path(dir.as_ref())) {
            Ok(contents) => Ok(Self {
                dir: dir.into(),
                data: serde_json::from_str(contents.as_str())?,
            }),
            Err(_) => {
                // TODO: check if this is a "did not exist" error
                let data = T::default();
                let mut result = Self {
                    dir: dir.into(),
                    data,
                };
                result.write(T::default())?;
                Ok(result)
            }
        }
    }

    fn path(dir: &std::path::Path) -> PathBuf {
        dir.join("ROOT")
    }

    fn tmp_path(dir: &std::path::Path) -> PathBuf {
        dir.join("ROOT_TMP")
    }

    pub fn write(&mut self, t: T) -> anyhow::Result<()> {
        let tmp_path = Self::tmp_path(self.dir.as_path());
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&tmp_path)?;
        let encoded = serde_json::to_string(&t)?;
        file.write_all(encoded.as_bytes())?;
        // TODO: is this a no-op?
        file.flush()?;
        file.sync_all()?;
        let path = Self::path(self.dir.as_path());

        // TODO: I don't think this is guaranteed to be atomic on crash.
        fs::rename(tmp_path, path)?;
        self.data = t;

        Ok(())
    }
}
