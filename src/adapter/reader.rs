use crate::{kv::*, models::*, Transaction};
use ethereum_types::{Address, H256};
use std::{borrow::Cow, marker::PhantomData};

pub struct StateReader<'db: 'tx, 'tx, Tx: Transaction<'db> + ?Sized> {
    block_nr: BlockNumber,
    tx: &'tx Tx,
    _marker: PhantomData<&'db ()>,
}

impl<'db: 'tx, 'tx, Tx: Transaction<'db> + ?Sized> StateReader<'db, 'tx, Tx> {
    pub fn new(tx: &'tx Tx, block_nr: BlockNumber) -> Self {
        Self {
            block_nr,
            tx,
            _marker: PhantomData,
        }
    }

    pub async fn read_account_data(&mut self, address: Address) -> anyhow::Result<Option<Account>> {
        crate::state::get_account_data_as_of(self.tx, address, BlockNumber(self.block_nr.0 + 1))
            .await
    }

    pub async fn read_account_storage(
        &mut self,
        address: Address,
        incarnation: Incarnation,
        key: H256,
    ) -> anyhow::Result<Option<H256>> {
        if let Some(value) = self
            .tx
            .get(
                &tables::PlainState,
                tables::PlainStateKey::Storage((address, incarnation, key)),
            )
            .await?
        {
            return Ok(Some(H256::decode(Cow::Borrowed(&value[..]))?));
        }

        Ok(None)
    }
}
