use derive_more::*;
use directories::ProjectDirs;
use std::{fmt::Display, path::PathBuf};

#[derive(Debug, Deref, DerefMut, FromStr)]

pub struct MartinezDataDir(pub PathBuf);

impl MartinezDataDir {
    pub fn chain_data_dir(&self) -> PathBuf {
        self.0.join("chaindata")
    }

    pub fn etl_temp_dir(&self) -> PathBuf {
        self.0.join("etl-temp")
    }
}

impl Default for MartinezDataDir {
    fn default() -> Self {
        Self(
            ProjectDirs::from("", "", "Martinez")
                .map(|pd| pd.data_dir().to_path_buf())
                .unwrap_or_else(|| "data".into()),
        )
    }
}

impl Display for MartinezDataDir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_os_str().to_str().unwrap())
    }
}
