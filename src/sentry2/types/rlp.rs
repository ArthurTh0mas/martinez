use crate::sentry2::types::{
    BlockHeaders, BlockId, GetBlockHeaders, Message, MessageId, NewBlock, NewBlockHashes,
    NewPooledTransactionHashes,
};

pub fn decode_rlp_message(id: MessageId, data: &[u8]) -> anyhow::Result<Message> {
    let msg = match id {
        MessageId::NewBlockHashes => Message::NewBlockHashes(rlp::decode::<NewBlockHashes>(data)?),
        MessageId::GetBlockHeaders => {
            Message::GetBlockHeaders(rlp::decode::<GetBlockHeaders>(data)?)
        }
        MessageId::BlockHeaders => Message::BlockHeaders(rlp::decode::<BlockHeaders>(data)?),
        MessageId::NewBlock => Message::NewBlock(Box::new(rlp::decode::<NewBlock>(data)?)),
        MessageId::NewPooledTransactionHashes => {
            Message::NewPooledTransactionHashes(rlp::decode::<NewPooledTransactionHashes>(data)?)
        }
        _ => anyhow::bail!("Unknown message id: {:?}", id),
    };
    Ok(msg)
}

impl rlp::Decodable for BlockId {
    fn decode(rlp: &rlp::Rlp) -> Result<Self, rlp::DecoderError> {
        if rlp.size() == 32 {
            Ok(Self::Hash(rlp.as_val()?))
        } else {
            Ok(Self::Number(rlp.as_val()?))
        }
    }
}

impl rlp::Encodable for BlockId {
    fn rlp_append(&self, s: &mut rlp::RlpStream) {
        match self {
            Self::Hash(v) => rlp::Encodable::rlp_append(v, s),
            Self::Number(v) => rlp::Encodable::rlp_append(v, s),
        }
    }
}
