use crate::{
    consensus::*,
    execution::{
        analysis_cache::AnalysisCache,
        continuation::{interrupt_data::InterruptData, resume_data::ResumeData},
        processor::ExecutionProcessor,
    },
    gen_await,
    models::*,
    state::*,
};
use std::{collections::HashMap, convert::TryFrom, ops::Generator, pin::Pin};

#[derive(Debug)]
pub struct Blockchain {
    config: ChainSpec,
    engine: Box<dyn Consensus>,
    bad_blocks: HashMap<H256, ValidationError>,
    receipts: Vec<Receipt>,
}

impl Blockchain {
    pub fn new(
        config: ChainSpec,
        genesis_block: Block,
    ) -> impl Generator<ResumeData, Yield = InterruptData, Return = Self> {
        Self::new_with_consensus(
            engine_factory(config.clone()).unwrap(),
            config,
            genesis_block,
        )
    }

    pub fn new_with_consensus(
        engine: Box<dyn Consensus>,
        config: ChainSpec,
        genesis_block: Block,
    ) -> impl Generator<ResumeData, Yield = InterruptData, Return = Self> {
        move |_| {
            let hash = genesis_block.header.hash();
            let number = genesis_block.header.number;
            yield InterruptData::InsertBlock {
                block: Box::new(genesis_block),
                hash,
            };
            yield InterruptData::CanonizeBlock { number, hash };

            Self {
                engine,
                config,
                bad_blocks: Default::default(),
                receipts: Default::default(),
            }
        }
    }

    pub fn insert_block(
        &mut self,
        block: Block,
        check_state_root: bool,
    ) -> impl Generator<ResumeData, Yield = InterruptData, Return = Result<(), ValidationError>> + '_
    {
        static move |_| {
            gen_await!(self.engine.validate_block_header(&block.header, true))?;
            gen_await!(self.engine.pre_validate_block(&block))?;

            let hash = block.header.hash();
            if let Some(error) = self.bad_blocks.get(&hash) {
                return Err(error.clone().into());
            }

            let b = BlockWithSenders::from(block.clone());

            let ancestor = gen_await!(self.canonical_ancestor(&b.header, hash))?;

            let current_canonical_block =
                ResumeData::into_block_number(yield InterruptData::CurrentCanonicalBlock).unwrap();

            gen_await!(self.unwind_last_changes(ancestor, current_canonical_block));

            let block_number = b.header.number;

            let mut chain = gen_await!(self.intermediate_chain(
                BlockNumber(block_number.0 - 1),
                b.header.parent_hash,
                ancestor,
            ))?;
            chain.push(WithHash { inner: b, hash });

            let mut num_of_executed_chain_blocks = 0;
            for x in chain.clone() {
                if let Err(e) = gen_await!(self.execute_block(&x.inner, check_state_root)) {
                    self.bad_blocks.insert(hash, e.clone());
                    gen_await!(self.unwind_last_changes(
                        ancestor,
                        BlockNumber(ancestor.0 + num_of_executed_chain_blocks)
                    ));
                    gen_await!(self.re_execute_canonical_chain(ancestor, current_canonical_block));

                    return Err(e);
                }

                num_of_executed_chain_blocks += 1;
            }

            yield InterruptData::InsertBlock {
                block: Box::new(block),
                hash,
            };

            let current_total_difficulty = ResumeData::into_total_difficulty(
                yield InterruptData::ReadTotalDifficulty {
                    block_number: current_canonical_block,
                    block_hash: ResumeData::into_hash(
                        yield InterruptData::CanonicalHash {
                            number: current_canonical_block,
                        },
                    )
                    .unwrap(),
                },
            )
            .unwrap();

            if ResumeData::into_total_difficulty(
                yield InterruptData::ReadTotalDifficulty {
                    block_number,
                    block_hash: hash,
                },
            )
            .unwrap()
                > current_total_difficulty
            {
                // canonize the new chain
                for i in (ancestor + 1..=current_canonical_block).rev() {
                    yield InterruptData::DecanonizeBlock { number: i };
                }

                for x in chain {
                    yield InterruptData::CanonizeBlock {
                        number: x.header.number,
                        hash: x.hash,
                    };
                }
            } else {
                gen_await!(
                    self.unwind_last_changes(ancestor, ancestor + num_of_executed_chain_blocks)
                );
                gen_await!(self.re_execute_canonical_chain(ancestor, current_canonical_block));
            }

            Ok(())
        }
    }

    fn execute_block<'a>(
        &'a mut self,
        block: &'a BlockWithSenders,
        check_state_root: bool,
    ) -> impl Generator<ResumeData, Yield = InterruptData, Return = Result<(), ValidationError>> + 'a
    {
        static move |_| {
            let body = BlockBodyWithSenders {
                transactions: block.transactions.clone(),
                ommers: block.ommers.clone(),
            };

            let block_spec = self.config.collect_block_spec(block.header.number);

            let mut analysis_cache = AnalysisCache::default();
            let processor = ExecutionProcessor::new(
                None,
                &mut analysis_cache,
                &mut *self.engine,
                &block.header,
                &body,
                &block_spec,
            );

            let _ = gen_await!(processor.execute_and_write_block());

            if check_state_root {
                let state_root = ResumeData::into_hash(yield InterruptData::StateRootHash).unwrap();
                if state_root != block.header.state_root {
                    yield InterruptData::UnwindStateChanges {
                        number: block.header.number,
                    };
                    return Err(ValidationError::WrongStateRoot {
                        expected: block.header.state_root,
                        got: state_root,
                    });
                }
            }

            Ok(())
        }
    }

    fn re_execute_canonical_chain(
        &mut self,
        ancestor: BlockNumber,
        tip: BlockNumber,
    ) -> impl Generator<ResumeData, Yield = InterruptData, Return = ()> + '_ {
        static move |_| {
            assert!(ancestor <= tip);
            for block_number in ancestor + 1..=tip {
                let hash = ResumeData::into_hash(
                    yield InterruptData::CanonicalHash {
                        number: block_number,
                    },
                )
                .unwrap();
                let body = ResumeData::into_body_with_senders(
                    yield InterruptData::ReadBodyWithSenders {
                        number: block_number,
                        hash,
                    },
                )
                .unwrap()
                .unwrap();
                let header = ResumeData::into_header(
                    yield InterruptData::ReadHeader {
                        block_number,
                        block_hash: hash,
                    },
                )
                .unwrap()
                .unwrap();

                let block = BlockWithSenders {
                    header: header.into(),
                    transactions: body.transactions,
                    ommers: body.ommers,
                };

                let _ = gen_await!(self.execute_block(&block, false)).unwrap();
            }
        }
    }

    fn unwind_last_changes(
        &mut self,
        ancestor: BlockNumber,
        tip: BlockNumber,
    ) -> impl Generator<ResumeData, Yield = InterruptData, Return = ()> + '_ {
        move |_| {
            assert!(ancestor <= tip);
            for block_number in (ancestor + 1..=tip).rev() {
                yield InterruptData::UnwindStateChanges {
                    number: block_number,
                };
            }
        }
    }

    fn intermediate_chain(
        &self,
        block_number: impl Into<BlockNumber>,
        mut hash: H256,
        canonical_ancestor: impl Into<BlockNumber>,
    ) -> impl Generator<
        ResumeData,
        Yield = InterruptData,
        Return = Result<Vec<WithHash<BlockWithSenders>>, ValidationError>,
    > {
        move |_| {
            let block_number = block_number.into();
            let canonical_ancestor = canonical_ancestor.into();
            let mut chain =
                Vec::with_capacity(usize::try_from(block_number.0 - canonical_ancestor.0).unwrap());
            for block_number in (canonical_ancestor + 1..=block_number).rev() {
                let body = ResumeData::into_body_with_senders(
                    yield InterruptData::ReadBodyWithSenders {
                        number: block_number,
                        hash,
                    },
                )
                .unwrap()
                .unwrap();
                let header = ResumeData::into_header(
                    yield InterruptData::ReadHeader {
                        block_number,
                        block_hash: hash,
                    },
                )
                .unwrap()
                .unwrap()
                .into();

                let block = WithHash {
                    inner: BlockWithSenders {
                        header,
                        transactions: body.transactions,
                        ommers: body.ommers,
                    },
                    hash,
                };

                hash = block.header.parent_hash;

                chain.push(block);
            }

            chain.reverse();

            Ok(chain)
        }
    }

    fn canonical_ancestor<'a>(
        &'a self,
        header: &'a PartialHeader,
        hash: H256,
    ) -> StateGenerator<'a, Result<BlockNumber, ValidationError>> {
        Box::pin(move |_| {
            if let Some(canonical_hash) = ResumeData::into_canonical_hash(
                yield InterruptData::CanonicalHash {
                    number: header.number,
                },
            )
            .unwrap()
            {
                if canonical_hash == hash {
                    return Ok(header.number);
                }
            }
            let parent = ResumeData::into_header(
                yield InterruptData::ReadHeader {
                    block_number: BlockNumber(header.number.0 - 1),
                    block_hash: header.parent_hash,
                },
            )
            .unwrap()
            .ok_or(ValidationError::UnknownParent)?;
            gen_await!(self.canonical_ancestor(&parent.into(), header.parent_hash))
        })
    }
}
