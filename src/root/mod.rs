use serde::{de::DeserializeOwned, Serialize};

use crate::fs::{DbDir, DbFile};

pub struct Root<T, D>
where
    T: Serialize + DeserializeOwned + Default,
    D: DbDir,
{
    dir: D,
    pub(crate) data: T,
}

impl<T, D> Root<T, D>
where
    T: Serialize + DeserializeOwned + Default,
    D: DbDir,
{
    pub fn load(mut dir: D) -> anyhow::Result<Self> {
        match dir.open(&"ROOT") {
            Some(f) => Ok(Self {
                dir,
                data: serde_json::from_slice(&f.read_all())?,
            }),
            None => {
                // Didn't exist, so create it with default values.
                let data = T::default();
                let mut result = Self { dir, data };
                result.write(T::default())?;
                Ok(result)
            }
        }
    }

    pub fn write(&mut self, t: T) -> anyhow::Result<()> {
        self.dir.unlink(&"TMP_ROOT")?;
        let mut file = self.dir.create(&"TMP_ROOT")?.unwrap();
        let encoded = serde_json::to_string(&t)?;
        file.write(encoded.as_bytes())?;
        file.sync()?;

        self.dir.rename(&"TMP_ROOT", &"ROOT")?;

        self.data = t;

        Ok(())
    }

    pub fn transform<F>(&mut self, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(T) -> T,
    {
        let new_data = f(std::mem::take(&mut self.data));
        self.write(new_data)?;
        Ok(())
    }
}
