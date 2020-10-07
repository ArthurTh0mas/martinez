use derive_more::*;
use directories::ProjectDirs;
use std::{fmt::Display, path::PathBuf};

#[derive(Debug, Deref, DerefMut, FromStr)]

pub struct MartinezDataDir(pub PathBuf);

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
