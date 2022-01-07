use super::*;
use bytes::Bytes;
use derive_more::From;

/// All resumed data variants.
#[derive(Debug, EnumAsInner, From)]
pub(crate) enum ResumeData {
    #[from(ignore)]
    Empty,
    Account(Option<Account>),
    Storage(U256),
    Code(Bytes),
    Header(Box<Option<BlockHeader>>),
    Body(Box<Option<BlockBody>>),
    TotalDifficulty(Option<U256>),

    BodyWithSenders(Box<Option<BlockBodyWithSenders>>),
    BlockNumber(BlockNumber),
    CanonicalHash(Option<H256>),
    Hash(H256),
}

impl From<()> for ResumeData {
    fn from(_: ()) -> Self {
        Self::Empty
    }
}
