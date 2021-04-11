use crate::kv::{traits::NewWithSize, Table, TableEncode};
use anyhow;
use std::{
    cmp::Ord,
    fs::File,
    io::{prelude::*, BufReader, BufWriter, SeekFrom},
    marker::PhantomData,
};
use tempfile::tempfile;

#[derive(Eq, Clone, PartialEq, PartialOrd, Ord)]
pub struct Entry<T>
where
    T: Table,
{
    pub key: <T::Key as TableEncode>::Encoded,
    pub value: <T::Value as TableEncode>::Encoded,
}

impl<T> Entry<T>
where
    T: Table,
{
    pub fn new(
        key: <T::Key as TableEncode>::Encoded,
        value: <T::Value as TableEncode>::Encoded,
    ) -> Self {
        Self { key, value }
    }
}

pub struct DataProvider<T>
where
    T: Table,
{
    file: BufReader<File>,
    len: usize,
    _marker: PhantomData<T>,
}

impl<T> DataProvider<T>
where
    T: Table,
    <T::Key as TableEncode>::Encoded: NewWithSize,
    <T::Value as TableEncode>::Encoded: NewWithSize,
{
    pub fn new(buffer: Vec<Entry<T>>, id: usize) -> anyhow::Result<DataProvider<T>, std::io::Error>
    where
        Self: Sized,
    {
        let file = tempfile()?;
        let mut w = BufWriter::new(file);
        for entry in &buffer {
            let k = entry.key.as_ref();
            let v = entry.value.as_ref();

            w.write_all(&k.len().to_be_bytes())?;
            w.write_all(&v.len().to_be_bytes())?;
            w.write_all(k)?;
            w.write_all(v)?;
        }

        let mut file = BufReader::new(w.into_inner()?);
        file.seek(SeekFrom::Start(0))?;
        let len = buffer.len();
        Ok(Self {
            file,
            len,
            _marker: PhantomData,
        })
    }

    #[allow(clippy::wrong_self_convention)]
    #[allow(clippy::wrong_self_convention)]
    pub fn to_next(
        &mut self,
    ) -> anyhow::Result<
        Option<(
            <T::Key as TableEncode>::Encoded,
            <T::Value as TableEncode>::Encoded,
        )>,
    > {
        if self.len == 0 {
            return Ok(None);
        }

        let mut buffer_key_length = [0; 8];
        let mut buffer_value_length = [0; 8];

        self.file.read_exact(&mut buffer_key_length)?;
        self.file.read_exact(&mut buffer_value_length)?;

        let key_length = usize::from_be_bytes(buffer_key_length);
        let value_length = usize::from_be_bytes(buffer_value_length);
        let mut key = <T::Key as TableEncode>::Encoded::new_with_size(key_length);
        let mut value = <T::Value as TableEncode>::Encoded::new_with_size(key_length);

        self.file.read_exact(&mut key)?;
        self.file.read_exact(&mut value)?;

        self.len -= 1;

        Ok(Some((key, value)))
    }
}
