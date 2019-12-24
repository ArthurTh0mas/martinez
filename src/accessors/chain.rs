use crate::{
    kv::{
        tables,
        traits::{Cursor, MutableCursor},
    },
    models::*,
    MutableTransaction, Transaction as ReadTransaction,
};
use ethereum_types::{Address, H256, U256};
use tokio_stream::StreamExt;
use tracing::*;

pub mod canonical_hash {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: ReadTransaction<'db>>(
        tx: &'tx Tx,
        block_num: BlockNumber,
    ) -> anyhow::Result<Option<H256>> {
        tx.get(&tables::CanonicalHeader, block_num).await
    }

    pub async fn write<'db: 'tx, 'tx, RwTx: MutableTransaction<'db>>(
        tx: &'tx RwTx,
        block_num: BlockNumber,
        hash: H256,
    ) -> anyhow::Result<()> {
        trace!("Writing canonical hash of {}", block_num);

        tx.mutable_cursor(&tables::CanonicalHeader)
            .await?
            .put(block_num, hash)
            .await
    }
}

pub mod header_number {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: ReadTransaction<'db>>(
        tx: &'tx Tx,
        hash: H256,
    ) -> anyhow::Result<Option<BlockNumber>> {
        trace!("Reading block number for hash {:?}", hash);

        tx.get(&tables::HeaderNumber, hash).await
    }
}

pub mod header {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: ReadTransaction<'db>>(
        tx: &'tx Tx,
        hash: H256,
        number: BlockNumber,
    ) -> anyhow::Result<Option<BlockHeader>> {
        trace!("Reading header for block {}/{:?}", number, hash);

        tx.get(&tables::Header, (number, hash)).await
    }
}

pub mod tx {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: ReadTransaction<'db>>(
        tx: &'tx Tx,
        base_tx_id: u64,
        amount: u32,
    ) -> anyhow::Result<Vec<Transaction>> {
        trace!(
            "Reading {} transactions starting from {}",
            amount,
            base_tx_id
        );

        Ok(if amount > 0 {
            tx.cursor(&tables::BlockTransaction)
                .await?
                .walk(Some(base_tx_id), |_, _| true)
                .take(amount as usize)
                .map(|res| res.map(|(_, tx)| tx))
                .collect::<anyhow::Result<Vec<_>>>()
                .await?
        } else {
            vec![]
        })
    }

    pub async fn write<'db: 'tx, 'tx, RwTx: MutableTransaction<'db>>(
        tx: &'tx RwTx,
        base_tx_id: u64,
        txs: &[Transaction],
    ) -> anyhow::Result<()> {
        trace!(
            "Writing {} transactions starting from {}",
            txs.len(),
            base_tx_id
        );

        let mut cursor = tx.mutable_cursor(&tables::BlockTransaction).await.unwrap();

        for (i, eth_tx) in txs.iter().enumerate() {
            cursor
                .put(base_tx_id + i as u64, eth_tx.clone())
                .await
                .unwrap();
        }

        Ok(())
    }
}

pub mod tx_sender {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: ReadTransaction<'db>>(
        tx: &'tx Tx,
        base_tx_id: u64,
        amount: u32,
    ) -> anyhow::Result<Vec<Address>> {
        trace!(
            "Reading {} transaction senders starting from {}",
            amount,
            base_tx_id
        );

        Ok(if amount > 0 {
            tx.cursor(&tables::TxSender)
                .await?
                .walk(Some(base_tx_id), |_, _| true)
                .take(amount as usize)
                .map(|res| res.map(|(_, sender)| sender))
                .collect::<anyhow::Result<Vec<_>>>()
                .await?
        } else {
            vec![]
        })
    }

    pub async fn write<'db: 'tx, 'tx, RwTx: MutableTransaction<'db>>(
        tx: &'tx RwTx,
        base_tx_id: u64,
        senders: &[Address],
    ) -> anyhow::Result<()> {
        trace!(
            "Writing {} transaction senders starting from {}",
            senders.len(),
            base_tx_id
        );

        let mut cursor = tx.mutable_cursor(&tables::TxSender).await.unwrap();

        for (i, sender) in senders.iter().cloned().enumerate() {
            cursor.put(base_tx_id + i as u64, sender).await.unwrap();
        }

        Ok(())
    }
}

pub mod storage_body {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: ReadTransaction<'db>>(
        tx: &'tx Tx,
        hash: H256,
        number: BlockNumber,
    ) -> anyhow::Result<Option<BodyForStorage>> {
        trace!("Reading storage body for block {}/{:?}", number, hash);

        tx.get(&tables::BlockBody, (number, hash)).await
    }

    pub async fn has<'db: 'tx, 'tx, Tx: ReadTransaction<'db>>(
        tx: &'tx Tx,
        hash: H256,
        number: BlockNumber,
    ) -> anyhow::Result<bool> {
        Ok(read(tx, hash, number).await?.is_some())
    }

    pub async fn write<'db: 'tx, 'tx, RwTx: MutableTransaction<'db>>(
        tx: &'tx RwTx,
        hash: H256,
        number: BlockNumber,
        body: BodyForStorage,
    ) -> anyhow::Result<()> {
        trace!("Writing storage body for block {}/{:?}", number, hash);

        tx.mutable_cursor(&tables::BlockBody)
            .await?
            .put((number, hash), body)
            .await?;

        Ok(())
    }
}

pub mod td {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: ReadTransaction<'db>>(
        tx: &'tx Tx,
        hash: H256,
        number: BlockNumber,
    ) -> anyhow::Result<Option<U256>> {
        trace!("Reading total difficulty at block {}/{:?}", number, hash);

        if let Some(b) = tx
            .get(&tables::HeadersTotalDifficulty, (number, hash))
            .await?
        {
            trace!("Reading TD RLP: {}", hex::encode(&b));

            return Ok(Some(rlp::decode(&b)?));
        }

        Ok(None)
    }
}

pub mod tl {
    use super::*;

    pub async fn read<'db: 'tx, 'tx, Tx: ReadTransaction<'db>>(
        tx: &'tx Tx,
        tx_hash: H256,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        trace!("Reading Block number for a tx_hash {:?}", tx_hash);

        if let Some(b) = tx.get(&tables::BlockTransactionLookup, tx_hash).await? {
            trace!("Reading TL RLP: {}", hex::encode(&b));

            return Ok(Some(rlp::decode(&b)?));
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{new_mem_database, traits::MutableKV};
    use bytes::Bytes;

    #[tokio::test]
    async fn accessors() {
        let tx1 = Transaction {
            message: TransactionMessage::Legacy {
                chain_id: None,
                nonce: 1,
                gas_price: 20_000.into(),
                gas_limit: 3_000_000,
                action: TransactionAction::Create,
                value: 0.into(),
                input: Bytes::new(),
            },
            signature: TransactionSignature::new(false, H256::repeat_byte(2), H256::repeat_byte(3))
                .unwrap(),
        };
        let tx2 = Transaction {
            message: TransactionMessage::Legacy {
                chain_id: None,
                nonce: 2,
                gas_price: 30_000.into(),
                gas_limit: 1_000_000,
                action: TransactionAction::Create,
                value: 10.into(),
                input: Bytes::new(),
            },
            signature: TransactionSignature::new(true, H256::repeat_byte(6), H256::repeat_byte(9))
                .unwrap(),
        };
        let txs = [tx1, tx2];

        let sender1 = Address::random();
        let sender2 = Address::random();
        let senders = [sender1, sender2];

        let block1_hash = H256::random();
        let body = BodyForStorage {
            base_tx_id: 1,
            tx_amount: 2,
            uncles: vec![],
        };

        let db = new_mem_database().unwrap();
        let rwtx = db.begin_mutable().await.unwrap();
        let rwtx = &rwtx;

        storage_body::write(rwtx, block1_hash, BlockNumber(1), body)
            .await
            .unwrap();
        canonical_hash::write(rwtx, BlockNumber(1), block1_hash)
            .await
            .unwrap();
        tx::write(rwtx, 1, &txs).await.unwrap();
        tx_sender::write(rwtx, 1, &senders).await.unwrap();

        let recovered_body = storage_body::read(rwtx, block1_hash, BlockNumber(1))
            .await
            .unwrap()
            .expect("Could not recover storage body.");
        let recovered_hash = canonical_hash::read(rwtx, BlockNumber(1))
            .await
            .unwrap()
            .expect("Could not recover block hash");
        let recovered_txs = tx::read(rwtx, 1, 2).await.unwrap();
        let recovered_senders = tx_sender::read(rwtx, 1, 2).await.unwrap();

        assert_eq!(body, recovered_body);
        assert_eq!(block1_hash, recovered_hash);
        assert_eq!(txs, *recovered_txs);
        assert_eq!(senders, *recovered_senders);
    }
}
