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

impl traits::TableEncode for ! {
    type Encoded = [u8; 0];

    fn encode(self) -> Self::Encoded {
        unreachable!()
    }
}

impl traits::TableEncode for Vec<u8> {
    type Encoded = Self;

    fn encode(self) -> Self::Encoded {
        self
    }
}

impl traits::TableDecode for Vec<u8> {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        Ok(b.to_vec())
    }
}

impl traits::TableEncode for Bytes {
    type Encoded = Self;

    fn encode(self) -> Self::Encoded {
        self
    }
}

impl traits::TableDecode for Bytes {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        Ok(b.to_vec().into())
    }
}

impl traits::TableEncode for () {
    type Encoded = [u8; 0];

    fn encode(self) -> Self::Encoded {
        []
    }
}

impl traits::TableDecode for () {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if !b.is_empty() {
            return Err(TooLong::<0> { got: b.len() }.into());
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deref, DerefMut, PartialEq, Eq, PartialOrd, Ord)]
pub struct VariableVec<const LEN: usize> {
    pub inner: ArrayVec<u8, LEN>,
}

impl<const LEN: usize> FromIterator<u8> for VariableVec<LEN> {
    fn from_iter<T: IntoIterator<Item = u8>>(iter: T) -> Self {
        Self {
            inner: ArrayVec::from_iter(iter),
        }
    }
}

impl<const LEN: usize> AsRef<[u8]> for VariableVec<LEN> {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl<const LEN: usize> traits::TableEncode for VariableVec<LEN> {
    type Encoded = Self;

    fn encode(self) -> Self::Encoded {
        self
    }
}

impl<const LEN: usize> traits::TableDecode for VariableVec<LEN> {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        let mut out = Self::default();
        out.try_extend_from_slice(b)?;
        Ok(out)
    }
}

impl<const LEN: usize> From<VariableVec<LEN>> for Vec<u8> {
    fn from(v: VariableVec<LEN>) -> Self {
        v.to_vec()
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
pub struct TooShort<const MINIMUM: usize> {
    pub got: usize,
}

impl<const MINIMUM: usize> Display for TooShort<MINIMUM> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Too short: {} < {}", self.got, MINIMUM)
    }
}

impl<const MINIMUM: usize> std::error::Error for TooShort<MINIMUM> {}

#[derive(Clone, Debug)]
pub struct TooLong<const MAXIMUM: usize> {
    pub got: usize,
}
impl<const MAXIMUM: usize> Display for TooLong<MAXIMUM> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Too long: {} > {}", self.got, MAXIMUM)
    }
}

impl<const MAXIMUM: usize> std::error::Error for TooLong<MAXIMUM> {}

macro_rules! u64_table_object {
    ($ty:ident) => {
        impl TableEncode for $ty {
            type Encoded = [u8; 8];

            fn encode(self) -> Self::Encoded {
                self.to_be_bytes()
            }
        }

        impl TableDecode for $ty {
            fn decode(b: &[u8]) -> anyhow::Result<Self> {
                match b.len() {
                    8 => Ok(u64::from_be_bytes(*array_ref!(&*b, 0, 8)).into()),
                    other => Err(InvalidLength::<8> { got: other }.into()),
                }
            }
        }
    };
}

u64_table_object!(u64);
u64_table_object!(BlockNumber);
u64_table_object!(Incarnation);
u64_table_object!(TxIndex);

#[derive(
    Clone,
    Copy,
    Debug,
    Deref,
    DerefMut,
    Default,
    Display,
    PartialEq,
    Eq,
    From,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct TruncateStart<T>(pub T);

impl<T, const LEN: usize> TableEncode for TruncateStart<T>
where
    T: TableEncode<Encoded = [u8; LEN]>,
{
    type Encoded = VariableVec<LEN>;

    fn encode(self) -> Self::Encoded {
        let arr = self.0.encode();

        let mut out = Self::Encoded::default();
        out.try_extend_from_slice(zeroless_view(&arr)).unwrap();
        out
    }
}

impl<T, const LEN: usize> TableDecode for TruncateStart<T>
where
    T: TableEncode<Encoded = [u8; LEN]> + TableDecode,
{
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() > LEN {
            return Err(TooLong::<LEN> { got: b.len() }.into());
        }

        let mut arr = [0; LEN];
        arr[LEN - b.len()..].copy_from_slice(b);
        T::decode(&arr).map(Self)
    }
}

macro_rules! bincode_table_object {
    ($ty:ty) => {
        impl TableEncode for $ty {
            type Encoded = Vec<u8>;

            fn encode(self) -> Self::Encoded {
                bincode::DefaultOptions::new().serialize(&self).unwrap()
            }
        }

        impl TableDecode for $ty {
            fn decode(b: &[u8]) -> anyhow::Result<Self> {
                Ok(bincode::DefaultOptions::new().deserialize(b)?)
            }
        }
    };
}

bincode_table_object!(U256);
bincode_table_object!(BodyForStorage);
bincode_table_object!(BlockHeader);
bincode_table_object!(Transaction);
bincode_table_object!(Vec<crate::models::Receipt>);
bincode_table_object!(Vec<crate::models::Log>);
bincode_table_object!(CoreConfig);

impl TableEncode for Address {
    type Encoded = [u8; ADDRESS_LENGTH];

    fn encode(self) -> Self::Encoded {
        self.0
    }
}

impl TableDecode for Address {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        match b.len() {
            ADDRESS_LENGTH => Ok(Address::from_slice(&*b)),
            other => Err(InvalidLength::<ADDRESS_LENGTH> { got: other }.into()),
        }
    }
}

impl TableEncode for H256 {
    type Encoded = [u8; KECCAK_LENGTH];

    fn encode(self) -> Self::Encoded {
        self.0
    }
}

impl TableDecode for H256
where
    InvalidLength<KECCAK_LENGTH>: 'static,
{
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        match b.len() {
            KECCAK_LENGTH => Ok(H256::from_slice(&*b)),
            other => Err(InvalidLength::<KECCAK_LENGTH> { got: other }.into()),
        }
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Deref,
    DerefMut,
    Default,
    Display,
    PartialEq,
    Eq,
    From,
    Into,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct ZerolessH256(pub H256);

impl TableEncode for ZerolessH256 {
    type Encoded = VariableVec<KECCAK_LENGTH>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        out.try_extend_from_slice(zeroless_view(&self.0)).unwrap();
        out
    }
}

impl TableDecode for ZerolessH256 {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() > KECCAK_LENGTH {
            bail!("too long: {} > {}", b.len(), KECCAK_LENGTH);
        }

        Ok(H256::from_uint(&U256::from_big_endian(b)).into())
    }
}

impl TableEncode for RoaringTreemap {
    type Encoded = Vec<u8>;

    fn encode(self) -> Self::Encoded {
        let mut out = vec![];
        self.serialize_into(&mut out).unwrap();
        out
    }
}

impl TableDecode for RoaringTreemap {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        Ok(RoaringTreemap::deserialize_from(b)?)
    }
}

pub struct BitmapKey<K> {
    pub inner: K,
    pub block_number: BlockNumber,
}

impl TableEncode for BitmapKey<Address> {
    type Encoded = [u8; ADDRESS_LENGTH + BLOCK_NUMBER_LENGTH];

    fn encode(self) -> Self::Encoded {
        let mut out = [0; ADDRESS_LENGTH + BLOCK_NUMBER_LENGTH];
        out[..ADDRESS_LENGTH].copy_from_slice(&self.inner.encode());
        out[ADDRESS_LENGTH..].copy_from_slice(&self.block_number.encode());
        out
    }
}

impl TableDecode for BitmapKey<Address> {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != ADDRESS_LENGTH + BLOCK_NUMBER_LENGTH {
            return Err(
                InvalidLength::<{ ADDRESS_LENGTH + BLOCK_NUMBER_LENGTH }> { got: b.len() }.into(),
            );
        }

        Ok(Self {
            inner: Address::decode(&b[..ADDRESS_LENGTH])?,
            block_number: BlockNumber::decode(&b[ADDRESS_LENGTH..])?,
        })
    }
}

impl TableEncode for BitmapKey<(Address, H256)> {
    type Encoded = [u8; ADDRESS_LENGTH + KECCAK_LENGTH + BLOCK_NUMBER_LENGTH];

    fn encode(self) -> Self::Encoded {
        let mut out = [0; ADDRESS_LENGTH + KECCAK_LENGTH + BLOCK_NUMBER_LENGTH];
        out[..ADDRESS_LENGTH].copy_from_slice(&self.inner.0.encode());
        out[ADDRESS_LENGTH..ADDRESS_LENGTH + KECCAK_LENGTH].copy_from_slice(&self.inner.1.encode());
        out[ADDRESS_LENGTH + KECCAK_LENGTH..].copy_from_slice(&self.block_number.encode());
        out
    }
}

impl TableDecode for BitmapKey<(Address, H256)> {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != ADDRESS_LENGTH + KECCAK_LENGTH + BLOCK_NUMBER_LENGTH {
            return Err(
                InvalidLength::<{ ADDRESS_LENGTH + KECCAK_LENGTH + BLOCK_NUMBER_LENGTH }> {
                    got: b.len(),
                }
                .into(),
            );
        }

        Ok(Self {
            inner: (
                Address::decode(&b[..ADDRESS_LENGTH])?,
                H256::decode(&b[ADDRESS_LENGTH..ADDRESS_LENGTH + KECCAK_LENGTH])?,
            ),
            block_number: BlockNumber::decode(&b[ADDRESS_LENGTH + KECCAK_LENGTH..])?,
        })
    }
}

impl TableEncode for StageId {
    type Encoded = &'static str;

    fn encode(self) -> Self::Encoded {
        self.0
    }
}

impl<A, B, const A_LEN: usize, const B_LEN: usize> TableEncode for (A, B)
where
    A: TableObject<Encoded = [u8; A_LEN]>,
    B: TableObject<Encoded = [u8; B_LEN]>,
{
    type Encoded = VariableVec<256>;

    fn encode(self) -> Self::Encoded {
        let mut v = Self::Encoded::default();
        v.try_extend_from_slice(&self.0.encode()).unwrap();
        v.try_extend_from_slice(&self.1.encode()).unwrap();
        v
    }
}

impl<A, B, const A_LEN: usize, const B_LEN: usize> TableDecode for (A, B)
where
    A: TableObject<Encoded = [u8; A_LEN]>,
    B: TableObject<Encoded = [u8; B_LEN]>,
{
    fn decode(v: &[u8]) -> anyhow::Result<Self> {
        if v.len() != A_LEN + B_LEN {
            bail!("Invalid len: {} != {} + {}", v.len(), A_LEN, B_LEN);
        }
        Ok((
            A::decode(&v[..A_LEN]).unwrap(),
            B::decode(&v[A_LEN..]).unwrap(),
        ))
    }
}

pub type AccountChangeKey = BlockNumber;

#[derive(Clone, Debug, PartialEq)]
pub struct AccountChange {
    pub address: Address,
    pub account: EncodedAccount,
}

impl TableEncode for AccountChange {
    type Encoded = VariableVec<{ ADDRESS_LENGTH + MAX_ACCOUNT_LEN }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        out.try_extend_from_slice(&self.address.encode()).unwrap();
        out.try_extend_from_slice(&self.account).unwrap();
        out
    }
}

impl TableDecode for AccountChange {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() < ADDRESS_LENGTH + 1 {
            return Err(TooShort::<{ ADDRESS_LENGTH + 1 }> { got: b.len() }.into());
        }

        Ok(Self {
            address: Address::decode(&b[..ADDRESS_LENGTH])?,
            account: EncodedAccount::decode(&b[ADDRESS_LENGTH..])?,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageChangeKey {
    pub block_number: BlockNumber,
    pub address: Address,
    pub incarnation: Incarnation,
}

impl TableEncode for StorageChangeKey {
    type Encoded = [u8; BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH + INCARNATION_LENGTH];

    fn encode(self) -> Self::Encoded {
        let mut out = [0; BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH + INCARNATION_LENGTH];
        out[..BLOCK_NUMBER_LENGTH].copy_from_slice(&self.block_number.encode());
        out[BLOCK_NUMBER_LENGTH..BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH]
            .copy_from_slice(&self.address.encode());
        out[BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH..].copy_from_slice(&self.incarnation.encode());
        out
    }
}

impl TableDecode for StorageChangeKey {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH + INCARNATION_LENGTH {
            return Err(InvalidLength::<
                { BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH + INCARNATION_LENGTH },
            > {
                got: b.len(),
            }
            .into());
        }

        Ok(Self {
            block_number: BlockNumber::decode(&b[..BLOCK_NUMBER_LENGTH])?,
            address: Address::decode(
                &b[BLOCK_NUMBER_LENGTH..BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH],
            )?,
            incarnation: Incarnation::decode(&b[BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH..])?,
        })
    }
}

pub enum StorageChangeSeekKey {
    Block(BlockNumber),
    BlockAndAddress(BlockNumber, Address),
    Full(StorageChangeKey),
}

impl TableEncode for StorageChangeSeekKey {
    type Encoded = VariableVec<{ BLOCK_NUMBER_LENGTH + ADDRESS_LENGTH + INCARNATION_LENGTH }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        match self {
            StorageChangeSeekKey::Block(block) => {
                out.try_extend_from_slice(&block.encode()).unwrap();
            }
            StorageChangeSeekKey::BlockAndAddress(block, address) => {
                out.try_extend_from_slice(&block.encode()).unwrap();
                out.try_extend_from_slice(&address.encode()).unwrap();
            }
            StorageChangeSeekKey::Full(key) => {
                out.try_extend_from_slice(&key.encode()).unwrap();
            }
        }
        out
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StorageChange {
    pub location: H256,
    pub value: H256,
}

impl TableEncode for StorageChange {
    type Encoded = VariableVec<{ KECCAK_LENGTH + KECCAK_LENGTH }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        out.try_extend_from_slice(&self.location.encode()).unwrap();
        out.try_extend_from_slice(&ZerolessH256(self.value).encode())
            .unwrap();
        out
    }
}

impl TableDecode for StorageChange {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() < KECCAK_LENGTH {
            return Err(TooShort::<KECCAK_LENGTH> { got: b.len() }.into());
        }

        Ok(Self {
            location: H256::decode(&b[..KECCAK_LENGTH])?,
            value: ZerolessH256::decode(&b[KECCAK_LENGTH..])?.0,
        })
    }
}

pub type HeaderKey = (BlockNumber, H256);

#[bitfield]
#[derive(Clone, Copy, Debug, Default)]
struct CallTraceSetFlags {
    flag_from: bool,
    flag_to: bool,
    #[skip]
    unused: B6,
}

#[derive(Clone, Copy, Debug)]
pub struct CallTraceSetEntry {
    address: Address,
    from: bool,
    to: bool,
}

impl TableEncode for CallTraceSetEntry {
    type Encoded = [u8; ADDRESS_LENGTH + 1];

    fn encode(self) -> Self::Encoded {
        let mut v = [0; ADDRESS_LENGTH + 1];
        v[..ADDRESS_LENGTH].copy_from_slice(&self.address.encode());

        let mut field_set = CallTraceSetFlags::default();
        field_set.set_flag_from(self.from);
        field_set.set_flag_to(self.to);
        v[ADDRESS_LENGTH] = field_set.into_bytes()[0];

        v
    }
}

impl TableDecode for CallTraceSetEntry {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        if b.len() != ADDRESS_LENGTH + 1 {
            return Err(InvalidLength::<{ ADDRESS_LENGTH + 1 }> { got: b.len() }.into());
        }

        let field_set = CallTraceSetFlags::from_bytes([b[ADDRESS_LENGTH]]);
        Ok(Self {
            address: Address::decode(&b[..ADDRESS_LENGTH])?,
            from: field_set.flag_from(),
            to: field_set.flag_to(),
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum PlainStateKey {
    Account(Address),
    Storage(Address, Incarnation),
}

impl TableEncode for PlainStateKey {
    type Encoded = VariableVec<{ ADDRESS_LENGTH + INCARNATION_LENGTH }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        match self {
            PlainStateKey::Account(address) => {
                out.try_extend_from_slice(&address.encode()).unwrap();
            }
            PlainStateKey::Storage(address, incarnation) => {
                out.try_extend_from_slice(&address.encode()).unwrap();
                out.try_extend_from_slice(&incarnation.encode()).unwrap();
            }
        }
        out
    }
}

impl TableDecode for PlainStateKey {
    fn decode(b: &[u8]) -> anyhow::Result<Self> {
        const STORAGE_KEY_LEN: usize = ADDRESS_LENGTH + INCARNATION_LENGTH;
        Ok(match b.len() {
            ADDRESS_LENGTH => Self::Account(Address::decode(b)?),
            STORAGE_KEY_LEN => Self::Storage(
                Address::decode(&b[..ADDRESS_LENGTH])?,
                Incarnation::decode(&b[ADDRESS_LENGTH..])?,
            ),
            _ => bail!(
                "invalid length: expected one of [{}, {}], got {}",
                ADDRESS_LENGTH,
                STORAGE_KEY_LEN,
                b.len()
            ),
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum PlainStateSeekKey {
    Account(Address),
    StorageAllIncarnations(Address),
    StorageWithIncarnation(Address, Incarnation),
}

impl TableEncode for PlainStateSeekKey {
    type Encoded = VariableVec<{ ADDRESS_LENGTH + INCARNATION_LENGTH }>;

    fn encode(self) -> Self::Encoded {
        let mut out = Self::Encoded::default();
        match self {
            Self::Account(address) | Self::StorageAllIncarnations(address) => {
                out.try_extend_from_slice(&address.encode()).unwrap();
            }
            Self::StorageWithIncarnation(address, incarnation) => {
                out.try_extend_from_slice(&address.encode()).unwrap();
                out.try_extend_from_slice(&incarnation.encode()).unwrap();
            }
        }
        out
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PlainStateFusedValue {
    Account {
        address: Address,
        account: EncodedAccount,
    },
    Storage {
        address: Address,
        incarnation: Incarnation,
        location: H256,
        value: H256,
    },
}

impl PlainStateFusedValue {
    pub fn as_account(&self) -> Option<(Address, EncodedAccount)> {
        if let PlainStateFusedValue::Account { address, account } = self {
            Some((*address, account.clone()))
        } else {
            None
        }
    }

    pub fn as_storage(&self) -> Option<(Address, Incarnation, H256, H256)> {
        if let Self::Storage {
            address,
            incarnation,
            location,
            value,
        } = self
        {
            Some((*address, *incarnation, *location, *value))
        } else {
            None
        }
    }
}
