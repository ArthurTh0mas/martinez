use super::*;
use async_stream::try_stream;
use async_trait::async_trait;
use futures_core::Stream;
use std::fmt::Debug;

#[async_trait]
pub trait KV: Debug + Send + Sync + 'static {
    type Tx<'db>: Transaction<'db>;

    async fn begin(&self) -> anyhow::Result<Self::Tx<'_>>;
}

#[async_trait]
pub trait MutableKV: KV + 'static {
    type MutableTx<'db>: MutableTransaction<'db>;

    async fn begin_mutable(&self) -> anyhow::Result<Self::MutableTx<'_>>;
}

pub trait TableEncode: Send + Sync + Sized {
    type Encoded: AsRef<[u8]> + Send + Sync;

    fn encode(self) -> Self::Encoded;
}

pub trait TableDecode: Send + Sync + Sized {
    fn decode(b: &[u8]) -> anyhow::Result<Self>;
}

pub trait TableObject: TableEncode + TableDecode {}

impl<T> TableObject for T where T: TableEncode + TableDecode {}

pub trait Table: Send + Sync + Debug + 'static {
    type Key: TableEncode;
    type Value: TableObject;
    type SeekKey: TableEncode;

    fn db_name(&self) -> string::String<StaticBytes>;
}
pub trait DupSort: Table {
    type SeekBothKey: TableObject;
}

#[async_trait]
pub trait Transaction<'db>: Send + Sync + Debug + Sized {
    type Cursor<'tx, T: Table>: Cursor<'tx, T>
    where
        'db: 'tx,
        Self: 'tx;
    type CursorDupSort<'tx, T: DupSort>: CursorDupSort<'tx, T>
    where
        'db: 'tx,
        Self: 'tx;

    fn id(&self) -> u64;

    async fn cursor<'tx, T>(&'tx self, table: T) -> anyhow::Result<Self::Cursor<'tx, T>>
    where
        'db: 'tx,
        T: Table;
    async fn cursor_dup_sort<'tx, T>(
        &'tx self,
        table: T,
    ) -> anyhow::Result<Self::CursorDupSort<'tx, T>>
    where
        'db: 'tx,
        T: DupSort;

    async fn get<'tx, T>(&'tx self, table: T, key: T::Key) -> anyhow::Result<Option<T::Value>>
    where
        'db: 'tx,
        T: Table;
}

#[async_trait]
pub trait MutableTransaction<'db>: Transaction<'db> {
    type MutableCursor<'tx, T: Table>: MutableCursor<'tx, T>
    where
        'db: 'tx,
        Self: 'tx;
    type MutableCursorDupSort<'tx, T: DupSort>: MutableCursorDupSort<'tx, T>
    where
        'db: 'tx,
        Self: 'tx;

    async fn mutable_cursor<'tx, T>(
        &'tx self,
        table: T,
    ) -> anyhow::Result<Self::MutableCursor<'tx, T>>
    where
        'db: 'tx,
        T: Table;
    async fn mutable_cursor_dupsort<'tx, T>(
        &'tx self,
        table: T,
    ) -> anyhow::Result<Self::MutableCursorDupSort<'tx, T>>
    where
        'db: 'tx,
        T: DupSort;

    async fn set<T: Table>(&self, table: T, k: T::Key, v: T::Value) -> anyhow::Result<()>;

    async fn del<T: Table>(&self, table: T, k: T::Key, v: Option<T::Value>)
        -> anyhow::Result<bool>;

    async fn clear_table<T: Table>(&self, table: T) -> anyhow::Result<()>;

    async fn commit(self) -> anyhow::Result<()>;
}

#[async_trait]
pub trait Cursor<'tx, T>: Send + Debug
where
    T: Table,
{
    async fn first(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode;
    async fn seek(&mut self, key: T::SeekKey) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode;
    async fn seek_exact(&mut self, key: T::Key) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode;
    async fn next(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode;
    async fn prev(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode;
    async fn last(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode;
    async fn current(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode;
}

pub fn walk<'tx: 'cur, 'cur, C, T>(
    cursor: &'cur mut C,
    start_key: Option<T::SeekKey>,
) -> impl Stream<Item = anyhow::Result<(T::Key, T::Value)>> + 'cur
where
    C: Cursor<'tx, T>,
    T: Table,
    T::Key: TableDecode,
    'tx: 'cur,
{
    try_stream! {
        let start = if let Some(start_key) = start_key {
            cursor.seek(start_key).await?
        } else {
            cursor.first().await?
        };
        if let Some(mut fv) = start {
            loop {
                yield fv;

                match cursor.next().await? {
                    Some(fv1) => {
                        fv = fv1;
                    }
                    None => break,
                }
            }
        }
    }
}

pub fn walk_back<'tx: 'cur, 'cur, C, T>(
    cursor: &'cur mut C,
    start_key: Option<T::SeekKey>,
) -> impl Stream<Item = anyhow::Result<(T::Key, T::Value)>> + 'cur
where
    C: Cursor<'tx, T>,
    T: Table,
    T::Key: TableDecode,
    'tx: 'cur,
{
    try_stream! {
        let start = if let Some(start_key) = start_key {
            cursor.seek(start_key).await?
        } else {
            cursor.last().await?
        };
        if let Some(mut fv) = start {
            loop {
                yield fv;

                match cursor.prev().await? {
                    Some(fv1) => {
                        fv = fv1;
                    }
                    None => break,
                }
            }
        }
    }
}

/// Walk over duplicates for some specific key.
pub fn walk_dup<'tx: 'cur, 'cur, C, T>(
    cursor: &'cur mut C,
    start_key: T::Key,
) -> impl Stream<Item = anyhow::Result<T::Value>> + 'cur
where
    C: CursorDupSort<'tx, T>,
    T: DupSort,
    T::Key: TableDecode,
    'tx: 'cur,
{
    try_stream! {
        let start = cursor.seek_exact(start_key).await?.map(|(_, v)| v);
        if let Some(mut value) = start {
            loop {
                yield value;

                match cursor.next_dup().await? {
                    Some((_, v)) => {
                        value = v;
                    }
                    None => break,
                }
            }
        }
    }
}

/// Walk over duplicates for some specific key.
pub fn walk_back_dup<'tx: 'cur, 'cur, C, T>(
    cursor: &'cur mut C,
    start_key: T::Key,
) -> impl Stream<Item = anyhow::Result<T::Value>> + 'cur
where
    C: CursorDupSort<'tx, T>,
    T: DupSort,
    T::Key: TableDecode,
    'tx: 'cur,
{
    try_stream! {
        if cursor.seek_exact(start_key).await?.is_some() {
            if let Some(mut value) = cursor.last_dup().await? {
                loop {
                    yield value;

                    match cursor.prev_dup().await? {
                        Some((_, v)) => {
                            value = v;
                        }
                        None => break,
                    }
                }
            }
        }
    }
}

pub fn ttw<'a, T, E>(f: impl Fn(&T) -> bool + 'a) -> impl Fn(&Result<T, E>) -> bool + 'a {
    move |res| match res {
        Ok(v) => (f)(v),
        Err(_) => true,
    }
}

#[async_trait]
pub trait MutableCursor<'tx, T>: Cursor<'tx, T>
where
    T: Table,
{
    /// Put based on order
    async fn put(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()>;
    /// Upsert value
    async fn upsert(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()>;
    /// Append the given key/data pair to the end of the database.
    /// This option allows fast bulk loading when keys are already known to be in the correct order.
    async fn append(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()>;

    /// Deletes the key/data pair to which the cursor refers.
    /// This does not invalidate the cursor, so operations such as MDB_NEXT
    /// can still be used on it.
    /// Both MDB_NEXT and MDB_GET_CURRENT will return the same record after
    /// this operation.
    async fn delete_current(&mut self) -> anyhow::Result<()>;
}

#[async_trait]
pub trait CursorDupSort<'tx, T>: Cursor<'tx, T>
where
    T: DupSort,
{
    async fn seek_both_range(
        &mut self,
        key: T::Key,
        value: T::SeekBothKey,
    ) -> anyhow::Result<Option<T::Value>>
    where
        T::Key: Clone;
    async fn last_dup(&mut self) -> anyhow::Result<Option<T::Value>>
    where
        T::Key: TableDecode;
    /// Position at next data item of current key
    async fn next_dup(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode;
    /// Position at first data item of next key
    async fn next_no_dup(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode;
    async fn prev_dup(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode;
}

#[async_trait]
pub trait MutableCursorDupSort<'tx, T>: MutableCursor<'tx, T> + CursorDupSort<'tx, T>
where
    T: DupSort,
{
    /// Deletes all of the data items for the current key
    async fn delete_current_duplicates(&mut self) -> anyhow::Result<()>;
    /// Same as `Cursor::append`, but for sorted dup data
    async fn append_dup(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()>;
}
