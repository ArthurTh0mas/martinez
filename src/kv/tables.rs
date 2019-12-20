use super::*;
use crate::models::{BlockHeader, *};
use arrayref::array_ref;
use arrayvec::ArrayVec;
use ethereum_types::{H256, *};
use maplit::hashmap;
use once_cell::sync::Lazy;
use roaring::RoaringTreemap;
use serde::Deserialize;
use static_bytes::BytesMut;
use std::{borrow::Cow, collections::HashMap, fmt::Display, sync::Arc};

macro_rules! decl_table {
    ($name:ident => $key:ty => $value:ty) => {
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $name;

        impl crate::kv::traits::Table for $name {
            type Key = $key;
            type Value = $value;

            fn db_name(&self) -> string::String<static_bytes::Bytes> {
                unsafe {
                    string::String::from_utf8_unchecked(static_bytes::Bytes::from_static(
                        Self::const_db_name().as_bytes(),
                    ))
                }
            }
        }

        impl $name {
            const fn const_db_name() -> &'static str {
                stringify!($name)
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
    pub dup_sort: Option<DupSortConfig>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct DupSortConfig {
    pub auto: Option<AutoDupSortConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AutoDupSortConfig {
    pub from: usize,
    pub to: usize,
}

impl TableObject for Vec<u8> {
    type Encoded = Self;
    type DecodeError = !;

    fn decode<'tx>(b: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
        Ok(b.into_owned())
    }

    fn encode(self) -> Self::Encoded {
        self
    }
}

#[derive(Clone, Debug)]
pub struct InvalidLength<const EXPECTED: usize> {
    pub got: usize,
}

impl<const EXPECTED: usize> Display for InvalidLength<EXPECTED> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid length: {} != {}", EXPECTED, self.got)
    }
}

impl<const EXPECTED: usize> std::error::Error for InvalidLength<EXPECTED> {}

#[derive(Clone, Debug)]
pub struct InvalidLengthOneOf<const NUM: usize> {
    pub expected_one_of: [usize; NUM],
    pub got: usize,
}

impl<const NUM: usize> Display for InvalidLengthOneOf<NUM> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Invalid length: {} is not one of {:?}",
            self.got, self.expected_one_of
        )
    }
}

impl<const NUM: usize> std::error::Error for InvalidLengthOneOf<NUM> {}

macro_rules! u64_table_object {
    ($ty:ident) => {
        impl TableObject for $ty
        where
            InvalidLength<8>: 'static,
        {
            type Encoded = [u8; 8];
            type DecodeError = InvalidLength<8>;

            fn decode<'tx>(b: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
                match b.len() {
                    8 => Ok(Self(u64::from_be_bytes(*array_ref!(&*b, 0, 8)))),
                    other => Err(InvalidLength { got: other }),
                }
            }
            fn encode(self) -> Self::Encoded {
                self.0.to_be_bytes()
            }
        }
    };
}

u64_table_object!(BlockNumber);
u64_table_object!(Incarnation);

impl TableObject for Address
where
    InvalidLength<BLOCK_NUMBER_LENGTH>: 'static,
{
    type Encoded = [u8; ADDRESS_LENGTH];
    type DecodeError = InvalidLength<ADDRESS_LENGTH>;

    fn decode<'tx>(b: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
        match b.len() {
            ADDRESS_LENGTH => Ok(Address::from_slice(&*b)),
            other => Err(InvalidLength { got: other }),
        }
    }

    fn encode(self) -> Self::Encoded {
        self.0
    }
}

impl TableObject for H256
where
    InvalidLength<BLOCK_NUMBER_LENGTH>: 'static,
{
    type Encoded = [u8; KECCAK_LENGTH];
    type DecodeError = InvalidLength<KECCAK_LENGTH>;

    fn decode<'tx>(b: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
        match b.len() {
            KECCAK_LENGTH => Ok(H256::from_slice(&*b)),
            other => Err(InvalidLength { got: other }),
        }
    }

    fn encode(self) -> Self::Encoded {
        self.0
    }
}

impl<A, B, const A_LEN: usize, const B_LEN: usize> TableObject for (A, B)
where
    A: TableObject<Encoded = [u8; A_LEN], DecodeError = InvalidLength<A_LEN>>,
    B: TableObject<Encoded = [u8; B_LEN], DecodeError = InvalidLength<B_LEN>>,
    InvalidLength<{ A_LEN + B_LEN }>: 'static,
    [u8; A_LEN + B_LEN]: AsRef<[u8]>,
{
    type Encoded = [u8; A_LEN + B_LEN];
    type DecodeError = InvalidLength<{ A_LEN + B_LEN }>;

    fn decode<'tx>(v: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
        if v.len() != A_LEN + B_LEN {
            return Err(InvalidLength { got: v.len() });
        }
        let b_bytes = v.split_off(A_LEN);
        Ok((A::decode(v).unwrap(), B::decode(b_bytes).unwrap()))
    }

    fn encode(self) -> Self::Encoded {
        let mut v = [0; A_LEN + B_LEN];
        v[..A_LEN].copy_from_slice(&self.0.encode());
        v[A_LEN..].copy_from_slice(&self.1.encode());
        v
    }
}

impl<A, B, C, const A_LEN: usize, const B_LEN: usize, const C_LEN: usize> TableObject for (A, B, C)
where
    A: TableObject<Encoded = [u8; A_LEN], DecodeError = InvalidLength<A_LEN>>,
    B: TableObject<Encoded = [u8; B_LEN], DecodeError = InvalidLength<B_LEN>>,
    C: TableObject<Encoded = [u8; C_LEN], DecodeError = InvalidLength<C_LEN>>,
    InvalidLength<{ A_LEN + B_LEN + C_LEN }>: 'static,
    [u8; A_LEN + B_LEN + C_LEN]: AsRef<[u8]>,
{
    type Encoded = [u8; A_LEN + B_LEN + C_LEN];
    type DecodeError = InvalidLength<{ A_LEN + B_LEN + C_LEN }>;

    fn decode<'tx>(v: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
        if v.len() != A_LEN + B_LEN + C_LEN {
            return Err(InvalidLength { got: v.len() });
        }
        let b_bytes = v.split_off(A_LEN);
        let c_bytes = b_bytes.split_off(B_LEN);
        Ok((
            A::decode(v).unwrap(),
            B::decode(b_bytes).unwrap(),
            C::decode(c_bytes).unwrap(),
        ))
    }

    fn encode(self) -> Self::Encoded {
        let mut v = [0; A_LEN + B_LEN + C_LEN];
        v[..A_LEN].copy_from_slice(&self.0.encode());
        v[A_LEN..A_LEN + B_LEN].copy_from_slice(&self.1.encode());
        v[A_LEN + B_LEN..].copy_from_slice(&self.2.encode());
        v
    }
}

pub type HeaderKey = (BlockNumber, H256);

// #[derive(Debug)]
// pub struct HeaderKey {
//     pub number: BlockNumber,
//     pub hash: H256,
// }

// impl TableObject for HeaderKey {
//     type Encoded = [u8; HEADER_KEY_LEN];
//     type DecodeError = InvalidLength<HEADER_KEY_LEN>;

//     fn decode(mut b: Bytes<'tx>) -> Result<Self, Self::DecodeError> {
//         let hash_bytes = b.split_off(BLOCK_NUMBER_LENGTH);
//         Ok(Self {
//             number: BlockNumber::decode(b)?,
//             hash: H256::decode(hash_bytes)?,
//         })
//     }

//     fn encode(self) -> Self::Encoded {
//         let mut v = [0; HEADER_KEY_LEN];

//         v[..BLOCK_NUMBER_LENGTH].copy_from_slice(&self.number.encode());
//         v[BLOCK_NUMBER_LENGTH..].copy_from_slice(&self.hash.encode());

//         v
//     }
// }

impl TableObject for BlockHeader {
    type Encoded = BytesMut;
    type DecodeError = rlp::DecoderError;

    fn decode<'tx>(b: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
        rlp::decode(&*b)
    }

    fn encode(self) -> Self::Encoded {
        rlp::encode(&self)
    }
}

pub type BitmapKey<K> = (K, BlockNumber);

// #[derive(Debug)]
// pub struct BitmapKey<K> {
//     pub inner: K,
//     pub chunk: BlockNumber,
// }

// impl<K, const K_LEN: usize> TableObject for BitmapKey<K>
// where
//     K: TableObject<Encoded = [u8; K_LEN]>,
//     [u8; K_LEN + BLOCK_NUMBER_LENGTH]: AsRef<[u8]>,
// {
//     type Encoded = [u8; K_LEN + BLOCK_NUMBER_LENGTH];
//     type DecodeError = !;

//     fn decode<'tx>(b: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
//         Ok(Self {
//             inner: K::decode(&b[..K_LEN]).unwrap(),
//             chunk: BlockNumber::decode(&b[K_LEN..]).unwrap(),
//         })
//     }

//     fn encode(self) -> Self::Encoded {
//         let mut out = [0; K_LEN + BLOCK_NUMBER_LENGTH];
//         out[..K_LEN].copy_from_slice(&self.inner.encode());
//         out[K_LEN..].copy_from_slice(&self.chunk.encode());
//         out
//     }
// }

impl TableObject for ChainConfig {
    type Encoded = Vec<u8>;
    type DecodeError = serde_json::Error;

    fn decode<'tx>(b: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
        serde_json::from_slice(&b)
    }

    fn encode(self) -> Self::Encoded {
        serde_json::to_vec(&self).unwrap()
    }
}

impl TableObject for RoaringTreemap {
    type Encoded = Vec<u8>;
    type DecodeError = std::io::Error;

    fn decode<'tx>(b: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
        RoaringTreemap::deserialize_from(b)
    }

    fn encode(self) -> Self::Encoded {
        let mut out = vec![];
        self.serialize_into(&mut out).unwrap();
        out
    }
}

impl TableObject for Account {
    type Encoded = Vec<u8>;
    type DecodeError = !;

    fn decode<'tx>(enc: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
        Self::decode_from_storage(&enc[..]).unwrap()
    }

    fn encode(self) -> Self::Encoded {
        self.encode_for_storage(false)
    }
}

pub struct AccountChange {
    pub address: Address,
    pub account: ArrayVec<u8, { Account::MAX_ENCODED_LEN }>,
}

impl TableObject for AccountChange {
    type Encoded = Vec<u8>;
    type DecodeError = !;

    fn decode<'tx>(b: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
        if (ADDRESS_LENGTH..ADDRESS_LENGTH + Account::MAX_ENCODED_LEN).contains(&b.len()) {
            todo!()
        }
        Ok(Self {
            address: Address::decode(Cow::Borrowed(&b[..ADDRESS_LENGTH])).unwrap(),
            account: ArrayVec::from_slice(&b[ADDRESS_LENGTH..]),
        })
    }

    fn encode(self) -> Self::Encoded {
        let mut v = Vec::with_capacity(ADDRESS_LENGTH + self.account.encoding_length_for_storage());
        v.extend_from_slice(&self.address.encode());
        v.extend_from_slice(&self.account.encode());
        v
    }
}

pub type StorageChange = (H256, H256);

pub enum MultiLenKeyEncoded<const LEFT: usize, const RIGHT: usize> {
    Left([u8; LEFT]),
    Right([u8; RIGHT]),
}

impl<const LEFT: usize, const RIGHT: usize> AsRef<[u8]> for MultiLenKeyEncoded<LEFT, RIGHT> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Left(arr) => arr.as_ref(),
            Self::Right(arr) => arr.as_ref(),
        }
    }
}

pub enum PlainStateKey {
    Account(Address),
    Storage((Address, Incarnation, H256)),
}

const PLAIN_STORAGE_KEY_LENGTH: usize = ADDRESS_LENGTH + INCARNATION_LENGTH + KECCAK_LENGTH;

impl TableObject for PlainStateKey {
    type Encoded = MultiLenKeyEncoded<ADDRESS_LENGTH, PLAIN_STORAGE_KEY_LENGTH>;
    type DecodeError = InvalidLengthOneOf<2>;

    fn decode<'tx>(b: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
        match b.len() {
            ADDRESS_LENGTH => Ok(Self::Account(TableObject::<'tx>::decode(b).unwrap())),
            PLAIN_STORAGE_KEY_LENGTH => Ok(Self::Storage(TableObject::<'tx>::decode(b).unwrap())),
            other => Err(InvalidLengthOneOf {
                expected_one_of: [ADDRESS_LENGTH, PLAIN_STORAGE_KEY_LENGTH],
                got: other,
            }),
        }
    }

    fn encode(self) -> Self::Encoded {
        match self {
            PlainStateKey::Account(obj) => MultiLenKeyEncoded::Left(obj.encode()),
            PlainStateKey::Storage(obj) => MultiLenKeyEncoded::Right(obj.encode()),
        }
    }
}

impl<const MAX_LEN: usize> TableObject for ArrayVec<u8, MAX_LEN> {
    type Encoded = Self;
    type DecodeError = !;

    fn decode<'tx>(b: Cow<'tx, [u8]>) -> Result<Self, Self::DecodeError> {
        Self::from_slice(&b[..])
    }

    fn encode(self) -> Self::Encoded {
        self
    }
}

pub type PlainStateValue =
    ArrayVec<u8, { const_utils::max(KECCAK_LENGTH, Account::MAX_ENCODED_LEN) }>;

impl DupSort for PlainState {
    type SeekBothKey = PlainStateValue;
}
impl DupSort for AccountChangeSet {
    type SeekBothKey = Address;
}
impl DupSort for StorageChangeSet {
    type SeekBothKey = H256;
}
impl DupSort for HashedStorage {
    type SeekBothKey = H256;
}
impl DupSort for CallTraceSet {
    type SeekBothKey = Vec<u8>;
}

decl_table!(PlainState => PlainStateKey => PlainStateValue);
decl_table!(PlainCodeHash => (Address, Incarnation) => H256);
decl_table!(AccountChangeSet => BlockNumber => AccountChange);
decl_table!(StorageChangeSet => (BlockNumber, Address, Incarnation) => (H256, H256));
decl_table!(HashedAccount => H256 => Vec<u8>);
decl_table!(HashedStorage => (H256, Incarnation, H256) => H256);
decl_table!(AccountHistory => BitmapKey<Address> => RoaringTreemap);
decl_table!(StorageHistory => BitmapKey<(Address, H256)> => RoaringTreemap);
decl_table!(Code => H256 => Vec<u8>);
decl_table!(HashedCodeHash => (H256, Incarnation) => H256);
decl_table!(IncarnationMap => Vec<u8> => Vec<u8>);
decl_table!(TEVMCode => H256 => Vec<u8>);
decl_table!(TrieAccount => Vec<u8> => Vec<u8>);
decl_table!(TrieStorage => Vec<u8> => Vec<u8>);
decl_table!(DbInfo => Vec<u8> => Vec<u8>);
decl_table!(SnapshotInfo => Vec<u8> => Vec<u8>);
decl_table!(BittorrentInfo => Vec<u8> => Vec<u8>);
decl_table!(HeaderNumber => H256 => BlockNumber);
decl_table!(CanonicalHeader => BlockNumber => H256);
decl_table!(Header => HeaderKey => BlockHeader);
decl_table!(HeadersTotalDifficulty => HeaderKey => Vec<u8>);
decl_table!(BlockBody => HeaderKey => BodyForStorage);
decl_table!(BlockTransaction => u64 => Transaction);
decl_table!(Receipt => Vec<u8> => Vec<u8>);
decl_table!(TransactionLog => Vec<u8> => Vec<u8>);
decl_table!(LogTopicIndex => Vec<u8> => Vec<u8>);
decl_table!(LogAddressIndex => Vec<u8> => Vec<u8>);
decl_table!(CallTraceSet => Vec<u8> => Vec<u8>);
decl_table!(CallFromIndex => Vec<u8> => Vec<u8>);
decl_table!(CallToIndex => Vec<u8> => Vec<u8>);
decl_table!(BlockTransactionLookup => H256 => Vec<u8>);
decl_table!(Config => H256 => ChainConfig);
decl_table!(SyncStage => Vec<u8> => Vec<u8>);
decl_table!(CliqueSeparate => Vec<u8> => Vec<u8>);
decl_table!(CliqueSnapshot => Vec<u8> => Vec<u8>);
decl_table!(CliqueLastSnapshot => Vec<u8> => Vec<u8>);
decl_table!(TxSender => u64 => Address);
decl_table!(LastBlock => Vec<u8> => Vec<u8>);
decl_table!(Migration => Vec<u8> => Vec<u8>);
decl_table!(Sequence => Vec<u8> => Vec<u8>);
decl_table!(LastHeader => Vec<u8> => Vec<u8>);
decl_table!(Issuance => Vec<u8> => Vec<u8>);

pub type DatabaseChart = Arc<HashMap<&'static str, TableInfo>>;

pub static CHAINDATA_TABLES: Lazy<Arc<HashMap<&'static str, TableInfo>>> = Lazy::new(|| {
    Arc::new(hashmap! {
        PlainState::const_db_name() => TableInfo {
            dup_sort: Some(DupSortConfig {
                auto: Some(AutoDupSortConfig {
                    from: 60,
                    to: 28,
                }),
            }),
        },
        PlainCodeHash::const_db_name() => TableInfo::default(),
        AccountChangeSet::const_db_name() => TableInfo {
            dup_sort: Some(DupSortConfig {
                auto: None,
            }),
        },
        StorageChangeSet::const_db_name() => TableInfo {
            dup_sort: Some(DupSortConfig {
                auto: None,
            }),
        },
        HashedAccount::const_db_name() => TableInfo::default(),
        HashedStorage::const_db_name() => TableInfo {
            dup_sort: Some(DupSortConfig {
                auto: Some(AutoDupSortConfig {
                    from: 72,
                    to: 40,
                }),
            }),
        },
        AccountHistory::const_db_name() => TableInfo::default(),
        StorageHistory::const_db_name() => TableInfo::default(),
        Code::const_db_name() => TableInfo::default(),
        HashedCodeHash::const_db_name() => TableInfo::default(),
        IncarnationMap::const_db_name() => TableInfo::default(),
        TEVMCode::const_db_name() => TableInfo::default(),
        TrieAccount::const_db_name() => TableInfo::default(),
        TrieStorage::const_db_name() => TableInfo::default(),
        DbInfo::const_db_name() => TableInfo::default(),
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
            dup_sort: Some(DupSortConfig {
                auto: None,
            }),
        },
        CallFromIndex::const_db_name() => TableInfo::default(),
        CallToIndex::const_db_name() => TableInfo::default(),
        BlockTransactionLookup::const_db_name() => TableInfo::default(),
        Config::const_db_name() => TableInfo::default(),
        SyncStage::const_db_name() => TableInfo::default(),
        CliqueSeparate::const_db_name() => TableInfo::default(),
        CliqueSnapshot::const_db_name() => TableInfo::default(),
        CliqueLastSnapshot::const_db_name() => TableInfo::default(),
        TxSender::const_db_name() => TableInfo::default(),
        LastBlock::const_db_name() => TableInfo::default(),
        Migration::const_db_name() => TableInfo::default(),
        Sequence::const_db_name() => TableInfo::default(),
        LastHeader::const_db_name() => TableInfo::default(),
        Issuance::const_db_name() => TableInfo::default(),
    })
});

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn plain_storage_key_encode() {
        let expected_addr = hex!("5A0b54D5dc17e0AadC383d2db43B0a0D3E029c4c").into();
        let expected_incarnation = 999_000_999_u64;
        let expected_key =
            hex!("58833f949125129fb8c6c93d2c6003c5bab7c0b116d695f4ca137b1debf4e472").into();

        let composite_key =
            PlainStateKey::Storage((expected_addr, expected_incarnation, expected_key)).encode();

        let (addr, incarnation, key) =
            PlainStateKey::decode(composite_key.as_ref().to_vec().into()).unwrap();

        assert_eq!(expected_addr, addr, "address should be extracted");
        assert_eq!(
            expected_incarnation, incarnation,
            "incarnation should be extracted"
        );
        assert_eq!(expected_key, key, "key should be extracted");
    }
}
