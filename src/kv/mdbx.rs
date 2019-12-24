use crate::{
    kv::{tables::*, traits, *},
    Cursor, CursorDupSort, MutableCursor, MutableCursorDupSort,
};
use ::mdbx::{
    DatabaseFlags, EnvironmentKind, Error as MdbxError, Transaction, TransactionKind, WriteFlags,
    RO, RW,
};
use anyhow::{bail, Context};
use async_trait::async_trait;
use bytes::Buf;
use std::{borrow::Cow, collections::HashMap, ops::Deref, path::Path};

pub fn table_sizes<E>(tx: &mdbx::Transaction<RO, E>) -> anyhow::Result<HashMap<String, u64>>
where
    E: EnvironmentKind,
{
    let mut out = HashMap::new();
    let main_db = tx.open_db(None)?;
    let mut cursor = tx.cursor(&main_db)?;
    while let Some((table, _)) = cursor.next_nodup()? {
        let table = String::from_utf8(table.to_vec()).unwrap();
        let db = tx
            .open_db(Some(&table))
            .with_context(|| format!("failed to open table: {}", table))?;
        let st = tx
            .db_stat(&db)
            .with_context(|| format!("failed to get stats for table: {}", table))?;

        out.insert(
            table,
            ((st.leaf_pages() + st.branch_pages() + st.overflow_pages()) * st.page_size() as usize)
                as u64,
        );

        unsafe {
            tx.close_db(db)?;
        }
    }

    Ok(out)
}

pub struct Environment<E: EnvironmentKind> {
    inner: ::mdbx::Environment<E>,
    chart: DatabaseChart,
}

impl<E: EnvironmentKind> Environment<E> {
    fn open(
        mut b: ::mdbx::EnvironmentBuilder<E>,
        path: &Path,
        chart: DatabaseChart,
        ro: bool,
    ) -> anyhow::Result<Self> {
        b.set_max_dbs(chart.len());
        if ro {
            b.set_flags(::mdbx::EnvironmentFlags {
                mode: ::mdbx::Mode::ReadOnly,
                ..Default::default()
            });
        }

        Ok(Self {
            inner: b.open(path).context("failed to open database")?,
            chart,
        })
    }

    pub fn open_ro(
        b: ::mdbx::EnvironmentBuilder<E>,
        path: &Path,
        chart: DatabaseChart,
    ) -> anyhow::Result<Self> {
        Self::open(b, path, chart, true)
    }

    pub fn open_rw(
        b: ::mdbx::EnvironmentBuilder<E>,
        path: &Path,
        chart: DatabaseChart,
    ) -> anyhow::Result<Self> {
        let s = Self::open(b, path, chart.clone(), false)?;

        let tx = s.inner.begin_rw_txn()?;
        for (table, info) in &*chart {
            tx.create_db(
                Some(table),
                if info.dup_sort.is_some() {
                    DatabaseFlags::DUP_SORT
                } else {
                    DatabaseFlags::default()
                },
            )?;
        }
        tx.commit()?;

        Ok(s)
    }
}

impl<E: EnvironmentKind> Deref for Environment<E> {
    type Target = ::mdbx::Environment<E>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl<E: EnvironmentKind> traits::KV for Environment<E> {
    type Tx<'tx> = MdbxTransaction<'tx, RO, E>;

    async fn begin(&self, _flags: u8) -> anyhow::Result<Self::Tx<'_>> {
        Ok(Self::Tx::<'_> {
            inner: self.inner.begin_ro_txn()?,
            chart: self.chart.clone(),
        })
    }
}

#[async_trait]
impl<E: EnvironmentKind> traits::MutableKV for Environment<E> {
    type MutableTx<'tx> = MdbxTransaction<'tx, RW, E>;

    async fn begin_mutable(&self) -> anyhow::Result<Self::MutableTx<'_>> {
        Ok(Self::MutableTx::<'_> {
            inner: self.inner.begin_rw_txn()?,
            chart: self.chart.clone(),
        })
    }
}

#[derive(Debug)]
pub struct MdbxTransaction<'env, K, E>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    inner: ::mdbx::Transaction<'env, K, E>,
    chart: DatabaseChart,
}

#[async_trait]
impl<'env, K, E> traits::Transaction<'env> for MdbxTransaction<'env, K, E>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    type Cursor<'tx, T: Table> = MdbxCursor<'tx, K>;
    type CursorDupSort<'tx, T: DupSort> = MdbxCursor<'tx, K>;

    async fn cursor<'tx, T>(&'tx self, table: &T) -> anyhow::Result<Self::Cursor<'tx, T>>
    where
        'env: 'tx,
        T: Table,
    {
        let table_name = table.db_name();
        Ok(MdbxCursor {
            inner: self
                .inner
                .cursor(&self.inner.open_db(Some(table_name.as_ref()))?)?,
            table_info: self
                .chart
                .get(table_name.as_ref() as &str)
                .cloned()
                .unwrap_or(TableInfo {
                    dup_sort: Some(DupSortConfig::default()),
                }),
            t: table.db_name(),
        })
    }

    async fn cursor_dup_sort<'tx, T>(&'tx self, table: &T) -> anyhow::Result<Self::Cursor<'tx, T>>
    where
        'env: 'tx,
        T: DupSort,
    {
        traits::Transaction::cursor(self, table).await
    }

    async fn get<'tx, T>(&'tx self, table: &T, key: T::Key) -> anyhow::Result<Option<T::Value>>
    where
        'env: 'tx,
        T: Table,
    {
        Ok(self.inner.get(
            &self.inner.open_db(Some(table.db_name().as_ref()))?,
            TableObject::<'tx>::encode(key),
        )?)
    }
}

#[async_trait]
impl<'env, E: EnvironmentKind> traits::MutableTransaction<'env> for MdbxTransaction<'env, RW, E> {
    type MutableCursor<'tx, T: Table> = MdbxCursor<'tx, RW>;
    type MutableCursorDupSort<'tx, T: DupSort> = MdbxCursor<'tx, RW>;

    async fn mutable_cursor<'tx, T>(
        &'tx self,
        table: &T,
    ) -> anyhow::Result<Self::MutableCursor<'tx, T>>
    where
        'env: 'tx,
        T: Table,
    {
        traits::Transaction::cursor(self, table).await
    }

    async fn mutable_cursor_dupsort<'tx, T>(
        &'tx self,
        table: &T,
    ) -> anyhow::Result<Self::MutableCursorDupSort<'tx, T>>
    where
        'env: 'tx,
        T: DupSort,
    {
        self.mutable_cursor(table).await
    }

    async fn set<'tx, T: Table>(
        &'tx self,
        table: &T,
        k: T::Key,
        v: T::Value,
    ) -> anyhow::Result<()> {
        if self
            .chart
            .get(&table.db_name().as_ref())
            .and_then(|info| info.dup_sort.as_ref())
            .is_some()
        {
            return MutableCursor::<T>::put(&mut self.mutable_cursor(table).await?, k, v).await;
        }
        Ok(self.inner.put(
            &self.inner.open_db(Some(table.db_name().as_ref()))?,
            TableObject::<'tx>::encode(k),
            TableObject::<'tx>::encode(v),
            WriteFlags::UPSERT,
        )?)
    }

    async fn commit(self) -> anyhow::Result<()> {
        self.inner.commit()?;

        Ok(())
    }
}

fn seek_autodupsort<'tx, K: TransactionKind>(
    c: &mut ::mdbx::Cursor<'tx, K>,
    dupsort_data: &AutoDupSortConfig,
    seek: &[u8],
) -> anyhow::Result<Option<(Cow<'tx, [u8]>, Cow<'tx, [u8]>)>> {
    let &AutoDupSortConfig { from, to } = dupsort_data;
    if seek.is_empty() {
        if let Some((mut k, mut v)) = c.first()? {
            if k.len() == to {
                let mut k2 = Vec::with_capacity(k.len() + from - to);
                k2.extend_from_slice(&k[..]);
                k2.extend_from_slice(&v[..from - to]);
                v.advance(from - to);
                k = k2.into();
            }
            return Ok(Some((k, v)));
        }

        return Ok(None);
    }

    let seek1;
    let mut seek2 = None;
    if seek.len() > to {
        seek1 = &seek[..to];
        seek2 = Some(&seek[to..]);
    } else {
        seek1 = seek;
    }

    let (mut k, mut v) = match c.set_range(seek1)? {
        Some(out) => out,
        None => return Ok(None),
    };

    if let Some(seek2) = seek2 {
        if seek1 == k {
            if let Some(out) = c.get_both_range(seek1, seek2)? {
                v = out;
            } else {
                (k, v) = match c.next()? {
                    Some(out) => out,
                    None => return Ok(None),
                };
            }
        }
    }

    if k.len() == to {
        let mut k2 = Vec::with_capacity(k.len() + from - to);
        k2.extend_from_slice(&k);
        k2.extend_from_slice(&v[..from - to]);
        v.advance(from - to);
        k = k2.into();
    }

    Ok(Some((k, v)))
}

fn auto_dup_sort_from_db<'tx>(
    table_info: &TableInfo,
    mut k: Cow<'tx, [u8]>,
    mut v: Cow<'tx, [u8]>,
) -> (Cow<'tx, [u8]>, Cow<'tx, [u8]>) {
    if let Some(&AutoDupSortConfig { from, to }) = table_info
        .dup_sort
        .as_ref()
        .and_then(|dup| dup.auto.as_ref())
    {
        if k.len() == to {
            let key_part = from - to;
            k = k[..].iter().chain(&v[..key_part]).copied().collect();
            v.advance(key_part);
        }
    }

    (k, v)
}

#[derive(Debug)]
pub struct MdbxCursor<'tx, K>
where
    K: TransactionKind,
{
    inner: ::mdbx::Cursor<'tx, K>,
    table_info: TableInfo,
    t: string::String<StaticBytes>,
}

impl<'tx, K> MdbxCursor<'tx, K>
where
    K: TransactionKind,
{
    fn seek_inner(
        &mut self,
        key: &[u8],
    ) -> anyhow::Result<Option<(Cow<'tx, [u8]>, Cow<'tx, [u8]>)>> {
        if let Some(info) = self
            .table_info
            .dup_sort
            .as_ref()
            .and_then(|dup| dup.auto.as_ref())
        {
            return seek_autodupsort(&mut self.inner, info, key);
        }

        Ok(if key.is_empty() {
            self.inner.first()?
        } else {
            self.inner.set_range(key)?
        })
    }
}

fn map_res_opt_decode<'tx, T: Table>(
    v: anyhow::Result<Option<(Cow<'tx, [u8]>, Cow<'tx, [u8]>)>>,
) -> anyhow::Result<Option<(T::Key, T::Value)>> {
    map_opt_decode::<'tx, T>(v?)
}

fn map_opt_decode<'tx, T: Table>(
    v: Option<(Cow<'tx, [u8]>, Cow<'tx, [u8]>)>,
) -> anyhow::Result<Option<(T::Key, T::Value)>> {
    if let Some((k, v)) = v {
        return decode(k, v).map(Some);
    }

    Ok(None)
}

fn decode<'tx, T: Table>(
    k: Cow<'tx, [u8]>,
    v: Cow<'tx, [u8]>,
) -> anyhow::Result<(T::Key, T::Value)> {
    let k = TableObject::decode::<'tx>(k)?;
    let v = TableObject::decode::<'tx>(v)?;
    Ok((k, v))
}

#[async_trait]
impl<'tx, K, T> Cursor<'tx, T> for MdbxCursor<'tx, K>
where
    K: TransactionKind,
    T: Table,
{
    async fn first(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>> {
        map_res_opt_decode(self.seek_inner(&[]))
    }

    async fn seek(&mut self, key: T::Key) -> anyhow::Result<Option<(T::Key, T::Value)>> {
        map_res_opt_decode(self.seek_inner(key.encode().as_ref()))
    }

    async fn seek_exact(&mut self, key: T::Key) -> anyhow::Result<Option<(T::Key, T::Value)>> {
        let key = key.encode();
        let key = key.as_ref();

        if let Some(&AutoDupSortConfig { from, to }) = self
            .table_info
            .dup_sort
            .as_ref()
            .and_then(|dup| dup.auto.as_ref())
        {
            return Ok(self
                .inner
                .get_both_range(&key[..to], &key[to..])?
                .and_then(|v| {
                    (key[to..] == v[..from - to])
                        .then(move || (key[..to].to_vec().into(), v.slice(from - to..)))
                })
                .map(|(k, v)| {
                    Ok::<_, anyhow::Error>(
                        TableObject::<'tx>::decode(k)?,
                        TableObject::<'tx>::decode(v)?,
                    )
                })
                .transpose()?);
        }

        map_res_opt_decode(self.inner.set_key(key))
    }

    async fn next(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>> {
        map_opt_decode(
            self.inner
                .next()?
                .map(|(k, v)| auto_dup_sort_from_db(&self.table_info, k, v)),
        )
    }

    async fn prev(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>> {
        map_opt_decode(
            self.inner
                .prev()?
                .map(|(k, v)| auto_dup_sort_from_db(&self.table_info, k, v)),
        )
    }

    async fn last(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>> {
        map_opt_decode(
            self.inner
                .last()?
                .map(|(k, v)| auto_dup_sort_from_db(&self.table_info, k, v)),
        )
    }

    async fn current(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>> {
        map_opt_decode(
            self.inner
                .get_current()?
                .map(|(k, v)| auto_dup_sort_from_db(&self.table_info, k, v)),
        )
    }
}

#[async_trait]
impl<'tx, K, T> CursorDupSort<'tx, T> for MdbxCursor<'tx, K>
where
    K: TransactionKind,
    T: DupSort,
{
    async fn seek_both_range(
        &mut self,
        key: T::Key,
        value: T::SeekBothKey,
    ) -> anyhow::Result<Option<T::Value>> {
        Ok(self
            .inner
            .get_both_range(key, value)?
            .map(T::Value::decode)
            .transpose()?)
    }

    async fn next_dup(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>> {
        map_res_opt_decode(self.inner.next_dup())
    }

    async fn next_no_dup(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>> {
        map_res_opt_decode(self.inner.next_nodup())
    }
}

fn delete_autodupsort<'tx>(
    c: &mut MdbxCursor<'tx, RW>,
    &AutoDupSortConfig { from, to }: &AutoDupSortConfig,
    key: &[u8],
) -> anyhow::Result<()> {
    if key.len() != from && key.len() >= to {
        bail!(
            "delete from dupsort table {}: can have keys of len=={} and len<{}. key: {},{}",
            c.t,
            from,
            to,
            hex::encode(key),
            key.len(),
        );
    }

    if key.len() == from {
        if let Some(v) = c.inner.get_both_range(&key[..to], &key[to..])? {
            if v[..from - to] == key[to..] {
                return Ok(c.inner.del(WriteFlags::CURRENT)?);
            }
        }

        return Ok(());
    }

    if c.inner.set(key)?.is_some() {
        c.inner.del(WriteFlags::CURRENT)?;
    }

    Ok(())
}

fn put_autodupsort<'tx>(
    c: &mut MdbxCursor<'tx, RW>,
    &AutoDupSortConfig { from, to }: &AutoDupSortConfig,
    key: &[u8],
    value: &[u8],
) -> anyhow::Result<()> {
    if key.len() != from && key.len() >= to {
        bail!(
            "put dupsort table {}: can have keys of len=={} and len<{}. key: {},{}",
            c.t,
            from,
            to,
            hex::encode(key),
            key.len(),
        );
    }

    if key.len() != from {
        match c.inner.put(key, value, WriteFlags::NO_OVERWRITE) {
            Err(MdbxError::KeyExist) => return Ok(c.inner.put(key, value, WriteFlags::CURRENT)?),
            Err(e) => {
                return Err(anyhow::Error::from(e).context(format!(
                    "key: {}, val: {}",
                    hex::encode(key),
                    hex::encode(value)
                )))
            }
            Ok(()) => return Ok(()),
        }
    }

    let value = key[to..]
        .iter()
        .chain(value.iter())
        .copied()
        .collect::<Vec<_>>();
    let key = &key[..to];
    let v = match c.inner.get_both_range(key, &value[..from - to])? {
        None => {
            return Ok(c.inner.put(key, value, WriteFlags::default())?);
        }
        Some(v) => v,
    };

    if v[..from - to] == value[..from - to] {
        if v.len() == value.len() {
            // in DupSort case mdbx.Current works only with values of same length
            return Ok(c.inner.put(key, value, WriteFlags::CURRENT)?);
        }
        c.inner.del(WriteFlags::CURRENT)?;
    }

    Ok(c.inner.put(key, value, WriteFlags::default())?)
}

#[async_trait]
impl<'tx, T> MutableCursor<'tx, T> for MdbxCursor<'tx, RW>
where
    T: Table,
{
    async fn put(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()> {
        if key.is_empty() {
            bail!("Key must not be empty");
        }

        if let Some(info) = self
            .table_info
            .dup_sort
            .as_ref()
            .and_then(|dup| dup.auto.as_ref())
            .cloned()
        {
            return put_autodupsort(self, &info, key, value);
        }

        Ok(self.inner.put(key, value, WriteFlags::default())?)
    }

    async fn append(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()> {
        Ok(self.inner.put(
            key.encode().as_ref(),
            value.encode().as_ref(),
            WriteFlags::APPEND,
        )?)
    }

    async fn delete(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()> {
        let key = key.encode();
        let value = value.encode();

        let key = key.as_ref();
        let value = value.as_ref();

        if let Some(info) = self
            .table_info
            .dup_sort
            .as_ref()
            .and_then(|dup| dup.auto.as_ref())
            .cloned()
        {
            return delete_autodupsort(self, &info, key);
        }

        if self.table_info.dup_sort.is_some() {
            if self.inner.get_both(key, value)?.is_some() {
                self.inner.del(WriteFlags::CURRENT)?;
            }

            return Ok(());
        }

        if self.inner.set(key)?.is_some() {
            self.inner.del(WriteFlags::CURRENT)?;
        }

        return Ok(());
    }

    async fn delete_current(&mut self) -> anyhow::Result<()> {
        self.inner.del(WriteFlags::CURRENT)?;

        Ok(())
    }

    async fn count(&mut self) -> anyhow::Result<usize> {
        todo!()
    }
}

#[async_trait]
impl<'tx, T> MutableCursorDupSort<'tx, T> for MdbxCursor<'tx, RW>
where
    T: DupSort,
{
    async fn delete_current_duplicates(&mut self) -> anyhow::Result<()> {
        Ok(self.inner.del(WriteFlags::NO_DUP_DATA)?)
    }
    async fn append_dup(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()> {
        Ok(self.inner.put(
            key.encode().as_ref(),
            value.encode().as_ref(),
            WriteFlags::APPEND_DUP,
        )?)
    }
}
