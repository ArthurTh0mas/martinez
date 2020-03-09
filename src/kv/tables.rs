use super::*;
use crate::{models::*, zeroless_view, StageId};
use anyhow::bail;
use arrayref::array_ref;
use arrayvec::ArrayVec;
use bincode::Options;
use bytes::Bytes;
use derive_more::*;
use ethereum_types::*;
use maplit::hashmap;
use modular_bitfield::prelude::*;
use once_cell::sync::Lazy;
use roaring::RoaringTreemap;
use serde::{Deserialize, *};
use std::{collections::HashMap, fmt::Display, sync::Arc};

#[derive(Debug)]
pub struct ErasedTable<T>(pub T)
where
    T: Table;

impl<T> Table for ErasedTable<T>
where
    T: Table,
{
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type SeekKey = Vec<u8>;
    type FusedValue = (Self::Key, Self::Value);

    fn db_name(&self) -> string::String<StaticBytes> {
        self.0.db_name()
    }

    fn fuse_values(key: Self::Key, value: Self::Value) -> anyhow::Result<Self::FusedValue> {
        Ok((key, value))
    }

    fn split_fused((key, value): Self::FusedValue) -> (Self::Key, Self::Value) {
        (key, value)
    }
}

impl<T> ErasedTable<T>
where
    T: Table,
{
    pub fn encode_key(object: T::Key) -> <<T as Table>::Key as TableEncode>::Encoded {
        object.encode()
    }

    pub fn decode_key(input: &[u8]) -> anyhow::Result<T::Key>
    where
        <T as Table>::Key: TableDecode,
    {
        T::Key::decode(input)
    }

    pub fn encode_value(object: T::Value) -> <<T as Table>::Value as TableEncode>::Encoded {
        object.encode()
    }

    pub fn decode_value(input: &[u8]) -> anyhow::Result<T::Value> {
        T::Value::decode(input)
    }

    pub fn encode_seek_key(object: T::SeekKey) -> <<T as Table>::SeekKey as TableEncode>::Encoded {
        object.encode()
    }
}

macro_rules! decl_table {
    ($name:ident => $key:ty => $value:ty => $seek_key:ty) => {
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $name;

        impl crate::kv::traits::Table for $name {
            type Key = $key;
            type SeekKey = $seek_key;
            type Value = $value;
            type FusedValue = (Self::Key, Self::Value);

            fn db_name(&self) -> string::String<bytes::Bytes> {
                unsafe {
                    string::String::from_utf8_unchecked(bytes::Bytes::from_static(
                        Self::const_db_name().as_bytes(),
                    ))
                }
            }

            fn fuse_values(key: Self::Key, value: Self::Value) -> anyhow::Result<Self::FusedValue> {
                Ok((key, value))
            }

            fn split_fused((key, value): Self::FusedValue) -> (Self::Key, Self::Value) {
                (key, value)
            }
        }

        impl $name {
            pub const fn const_db_name() -> &'static str {
                stringify!($name)
            }

            pub const fn erased(self) -> ErasedTable<Self> {
                ErasedTable(self)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", Self::const_db_name())
            }
        }
    };
    ($name:ident => $key:ty => $value:ty) => {
        decl_table!($name => $key => $value => $key);
    };
}

macro_rules! decl_single_entry_table {
    ($name:ident => $value:ty) => {
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $name;

        impl crate::kv::traits::Table for $name {
            type Key = ();
            type SeekKey = !;
            type Value = $value;
            type FusedValue = Self::Value;

            fn db_name(&self) -> string::String<bytes::Bytes> {
                unsafe {
                    string::String::from_utf8_unchecked(bytes::Bytes::from_static(
                        Self::const_db_name().as_bytes(),
                    ))
                }
            }

            fn fuse_values(_: Self::Key, value: Self::Value) -> anyhow::Result<Self::FusedValue> {
                Ok(value)
            }

            fn split_fused(value: Self::FusedValue) -> (Self::Key, Self::Value) {
                ((), value)
            }
        }

        impl $name {
            pub const fn const_db_name() -> &'static str {
                stringify!($name)
            }

            pub const fn erased(self) -> ErasedTable<Self> {
                ErasedTable(self)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", Self::const_db_name())
            }
        }
    };
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TableInfo {
    pub dup_sort: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PlainState;

impl Table for PlainState {
    type Key = PlainStateKey;
    type Value = VariableVec<MAX_ACCOUNT_LEN>;
    type SeekKey = PlainStateSeekKey;
    type FusedValue = PlainStateFusedValue;

    fn db_name(&self) -> string::String<bytes::Bytes> {
        unsafe {
            string::String::from_utf8_unchecked(bytes::Bytes::from_static(
                Self::const_db_name().as_bytes(),
            ))
        }
    }

    fn fuse_values(key: Self::Key, value: Self::Value) -> anyhow::Result<Self::FusedValue> {
        Ok(match key {
            PlainStateKey::Account(address) => {
                if value.len() > MAX_ACCOUNT_LEN {
                    return Err(InvalidLength::<MAX_ACCOUNT_LEN> { got: value.len() }.into());
                }

                PlainStateFusedValue::Account {
                    address,
                    account: value,
                }
            }
            PlainStateKey::Storage(address, incarnation) => {
                if value.len() > KECCAK_LENGTH + KECCAK_LENGTH {
                    return Err(
                        TooLong::<{ KECCAK_LENGTH + KECCAK_LENGTH }> { got: value.len() }.into(),
                    );
                }

                PlainStateFusedValue::Storage {
                    address,
                    incarnation,
                    location: H256::decode(&value[..KECCAK_LENGTH])?,
                    value: ZerolessH256::decode(&value[KECCAK_LENGTH..])?.0,
                }
            }
        })
    }

    fn split_fused(fv: Self::FusedValue) -> (Self::Key, Self::Value) {
        match fv {
            PlainStateFusedValue::Account { address, account } => {
                (PlainStateKey::Account(address), account)
            }
            PlainStateFusedValue::Storage {
                address,
                incarnation,
                location,
                value,
            } => {
                let mut v = Self::Value::default();
                v.try_extend_from_slice(&location.encode()).unwrap();
                v.try_extend_from_slice(&ZerolessH256(value).encode())
                    .unwrap();
                (PlainStateKey::Storage(address, incarnation), v)
            }
        }
    }
}

impl PlainState {
    pub const fn const_db_name() -> &'static str {
        "PlainState"
    }

    pub const fn erased(self) -> ErasedTable<Self> {
        ErasedTable(self)
    }
}

impl std::fmt::Display for PlainState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", Self::const_db_name())
    }
}

decl_table!(PlainCodeHash => (Address, Incarnation) => H256);
decl_table!(AccountChangeSet => AccountChangeKey => AccountChange);
decl_table!(StorageChangeSet => StorageChangeKey => StorageChange => StorageChangeSeekKey);
decl_table!(HashedAccount => H256 => Vec<u8>);
decl_table!(HashedStorage => Vec<u8> => Vec<u8>);
decl_table!(AccountHistory => BitmapKey<Address> => RoaringTreemap);
decl_table!(StorageHistory => BitmapKey<(Address, H256)> => RoaringTreemap);
decl_table!(Code => H256 => Bytes);
decl_table!(HashedCodeHash => (H256, Incarnation) => H256);
decl_table!(IncarnationMap => Address => Incarnation);
decl_table!(TrieAccount => Vec<u8> => Vec<u8>);
decl_table!(TrieStorage => Vec<u8> => Vec<u8>);
decl_table!(SnapshotInfo => Vec<u8> => Vec<u8>);
decl_table!(BittorrentInfo => Vec<u8> => Vec<u8>);
decl_table!(HeaderNumber => H256 => BlockNumber);
decl_table!(CanonicalHeader => BlockNumber => H256);
decl_table!(Header => HeaderKey => BlockHeader);
decl_table!(HeadersTotalDifficulty => HeaderKey => U256);
decl_table!(BlockBody => HeaderKey => BodyForStorage => BlockNumber);
decl_table!(BlockTransaction => TxIndex => Transaction);
decl_table!(Receipt => BlockNumber => Vec<crate::models::Receipt>);
decl_table!(TransactionLog => (BlockNumber, TxIndex) => Vec<crate::models::Log>);
decl_table!(LogTopicIndex => Vec<u8> => RoaringTreemap);
decl_table!(LogAddressIndex => Vec<u8> => RoaringTreemap);
decl_table!(CallTraceSet => BlockNumber => CallTraceSetEntry);
decl_table!(CallFromIndex => Vec<u8> => RoaringTreemap);
decl_table!(CallToIndex => Vec<u8> => RoaringTreemap);
decl_table!(BlockTransactionLookup => H256 => TruncateStart<BlockNumber>);
decl_table!(SyncStage => StageId => BlockNumber);
decl_table!(TxSender => TxIndex => Address);
decl_table!(LastBlock => Vec<u8> => Vec<u8>);
decl_table!(Migration => Vec<u8> => Vec<u8>);
decl_table!(Sequence => Vec<u8> => Vec<u8>);
decl_table!(LastHeader => Vec<u8> => Vec<u8>);
decl_table!(Issuance => Vec<u8> => Vec<u8>);
decl_single_entry_table!(Config => CoreConfig);

impl DupSort for PlainState {
    type SeekBothKey = H256;
}
impl DupSort for AccountChangeSet {
    type SeekBothKey = Address;
}
impl DupSort for StorageChangeSet {
    type SeekBothKey = H256;
}
impl DupSort for HashedStorage {
    type SeekBothKey = Vec<u8>;
}
impl DupSort for CallTraceSet {
    type SeekBothKey = Vec<u8>;
}

pub type DatabaseChart = Arc<HashMap<&'static str, TableInfo>>;

pub static CHAINDATA_TABLES: Lazy<Arc<HashMap<&'static str, TableInfo>>> = Lazy::new(|| {
    Arc::new(hashmap! {
        PlainState::const_db_name() => TableInfo {
            dup_sort: true,
        },
        PlainCodeHash::const_db_name() => TableInfo::default(),
        AccountChangeSet::const_db_name() => TableInfo {
            dup_sort: true,
        },
        StorageChangeSet::const_db_name() => TableInfo {
            dup_sort: true,
        },
        HashedAccount::const_db_name() => TableInfo::default(),
        HashedStorage::const_db_name() => TableInfo {
            dup_sort: true,
        },
        AccountHistory::const_db_name() => TableInfo::default(),
        StorageHistory::const_db_name() => TableInfo::default(),
        Code::const_db_name() => TableInfo::default(),
        HashedCodeHash::const_db_name() => TableInfo::default(),
        IncarnationMap::const_db_name() => TableInfo::default(),
        TrieAccount::const_db_name() => TableInfo::default(),
        TrieStorage::const_db_name() => TableInfo::default(),
        SnapshotInfo::const_db_name() => TableInfo::default(),
        BittorrentInfo::const_db_name() => TableInfo::default(),
        HeaderNumber::const_db_name() => TableInfo::default(),
        CanonicalHeader::const_db_name() => TableInfo::default(),
        Header::const_db_name() => TableInfo::default(),
        HeadersTotalDifficulty::const_db_name() => TableInfo::default(),
        BlockBody::const_db_name() => TableInfo::default(),
        BlockTransaction::const_db_name() => TableInfo::default(),
        Receipt::const_db_name() => TableInfo::default(),
        TransactionLog::const_db_name() => TableInfo::default(),
        LogTopicIndex::const_db_name() => TableInfo::default(),
        LogAddressIndex::const_db_name() => TableInfo::default(),
        CallTraceSet::const_db_name() => TableInfo {
            dup_sort: true,
        },
        CallFromIndex::const_db_name() => TableInfo::default(),
        CallToIndex::const_db_name() => TableInfo::default(),
        BlockTransactionLookup::const_db_name() => TableInfo::default(),
        SyncStage::const_db_name() => TableInfo::default(),
        TxSender::const_db_name() => TableInfo::default(),
        LastBlock::const_db_name() => TableInfo::default(),
        Migration::const_db_name() => TableInfo::default(),
        Sequence::const_db_name() => TableInfo::default(),
        LastHeader::const_db_name() => TableInfo::default(),
        Issuance::const_db_name() => TableInfo::default(),
        Config::const_db_name() => TableInfo::default(),
    })
});
