use crate::{
    models::{Block, BlockNumber, H256, U256},
    sentry::chain_config::ChainConfig,
    sentry2::types::*,
};
use async_trait::async_trait;
use ethereum_interfaces::sentry as grpc_sentry;
use futures_util::FutureExt;
use std::sync::Arc;
use tokio::sync::RwLock as AsyncMutex;

#[derive(Debug, Clone, Copy)]
pub struct HeadData {
    pub height: u64,
    pub hash: H256,
    pub td: u128,
}

impl HeadData {
    pub fn new(height: u64, hash: H256, td: u128) -> Self {
        Self { height, hash, td }
    }
}

impl Default for HeadData {
    fn default() -> Self {
        Self {
            height: 0,
            hash: H256::default(),
            td: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Status {
    pub network_id: u64,
    pub total_difficulty: U256,
    pub hash: H256,
    pub config: ChainConfig,
    pub block_number: BlockNumber,
}

pub struct HeaderDownloader {}

pub struct BodyDownaloder {}

pub type SentryClient = grpc_sentry::sentry_client::SentryClient<tonic::transport::Channel>;

pub struct Coordinator {
    pub sentries: Vec<SentryClient>,
    pub header_downloader: Arc<HeaderDownloader>,
    pub body_downloader: Arc<BodyDownaloder>,
    pub head_data: Arc<AsyncMutex<HeadData>>,
    pub chain_config: Option<ChainConfig>,
    pub forks: Vec<u64>,
    pub genesis_hash: H256,
    pub network_id: u64,
}

impl Coordinator {
    pub fn new(
        sentries: Vec<SentryClient>,
        header_downloader: Arc<HeaderDownloader>,
        head_data: Arc<AsyncMutex<HeadData>>,
        _chain_config: Option<ChainConfig>,
        forks: Vec<u64>,
        genesis_hash: H256,
        network_id: u64,
    ) -> Self {
        Self {
            sentries,
            header_downloader,
            body_downloader: Arc::new(BodyDownaloder {}),
            head_data,
            chain_config: None,
            forks,
            genesis_hash,
            network_id,
        }
    }
}

#[async_trait]
#[allow(unreachable_code)]
impl SentryCoordinator for Coordinator {
    async fn send_body_request(&mut self) {}
    async fn send_header_request(&mut self, _req: HeaderRequest) -> anyhow::Result<u64> {
        Ok(0)
    }
    async fn broadcast_block(
        &mut self,
        _block: Block,
        _total_difficulty: u128,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    async fn propagate_new_block_hashes(
        &mut self,
        _block_hashes: Vec<(H256, BlockNumber)>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_head(&mut self, _height: u64, _hash: H256, _total_difficultyy: u128) {}

    async fn propagate_transactions(&mut self, _transactions: Vec<H256>) -> anyhow::Result<()> {
        Ok(())
    }
    async fn penalize(&mut self, _penalties: Vec<Penalty>) {}
    async fn recv_headers(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn send_message(&mut self, msg: Message, predicate: PeerFilter) -> anyhow::Result<()> {
        let data = grpc_sentry::OutboundMessageData {
            id: grpc_sentry::MessageId::from(msg.id()) as i32,
            data: rlp::encode(&msg).into(),
        };

        let fut = async move |mut s: SentryClient,
                              filter: PeerFilter,
                              req: grpc_sentry::OutboundMessageData|
                    -> anyhow::Result<()> {
            s.hand_shake(tonic::Request::new(())).await?;
            match filter {
                PeerFilter::All => s.send_message_to_all(req).boxed(),
                PeerFilter::PeerId(peer_id) => s
                    .send_message_by_id(grpc_sentry::SendMessageByIdRequest {
                        data: Some(req),
                        peer_id: Some(peer_id).into(),
                    })
                    .boxed(),
                PeerFilter::MinBlock(min_block) => s
                    .send_message_by_min_block(grpc_sentry::SendMessageByMinBlockRequest {
                        data: Some(req),
                        min_block,
                    })
                    .boxed(),
                PeerFilter::Random(max_peers) => s
                    .send_message_to_random_peers(grpc_sentry::SendMessageToRandomPeersRequest {
                        data: Some(req),
                        max_peers,
                    })
                    .boxed(),
            }
            .await?;
            Ok(())
        };
        for s in self.sentries.iter() {
            fut(s.clone(), predicate.clone(), data.clone()).await?;
        }

        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait SentryCoordinator: Send + Sync {
    async fn send_body_request(&mut self);
    async fn send_header_request(&mut self, req: HeaderRequest) -> anyhow::Result<u64>;
    async fn propagate_new_block_hashes(
        &mut self,
        block_hashes: Vec<(H256, BlockNumber)>,
    ) -> anyhow::Result<()>;
    async fn broadcast_block(&mut self, block: Block, total_difficulty: u128)
        -> anyhow::Result<()>;
    async fn propagate_transactions(&mut self, transactions: Vec<H256>) -> anyhow::Result<()>;
    async fn update_head(&mut self, height: u64, hash: H256, total_difficulty: u128);
    async fn penalize(&mut self, penalties: Vec<Penalty>);
    async fn send_message(&mut self, message: Message, predicate: PeerFilter)
        -> anyhow::Result<()>;
    async fn recv_headers(&mut self) -> anyhow::Result<()>;
    async fn recv(&mut self) -> anyhow::Result<()>;
}

#[cfg(ignore)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(it_works_async()).unwrap();
    }

    async fn it_works_async() -> anyhow::Result<()> {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();
        let futs: Vec<SentryClient> = futures_util::future::join_all(
            (0..32)
                .into_iter()
                .map(|t| format!("http://localhost:{}", t + 8000))
                .collect::<Vec<String>>()
                .into_iter()
                .map(|clients| async move {
                    grpc_sentry::sentry_client::SentryClient::connect(clients)
                        .await
                        .unwrap()
                })
                .collect::<Vec<_>>(),
        )
        .await;

        let mut coordinator = Coordinator::new(
            futs,
            Arc::new(HeaderDownloader {}),
            Arc::new(AsyncMutex::new(HeadData::new(0, H256::default(), 0))),
            None,
            vec![],
            H256::default(),
            0,
        );

        coordinator
            .send_message(
                Message::GetBlockHeaders(GetBlockHeaders {
                    request_id: 0,
                    params: GetBlockHeadersParams {
                        limit: 192,
                        reverse: 0,
                        start: BlockId::Number(BlockNumber(0)),
                        skip: 0,
                    },
                }),
                PeerFilter::All,
            )
            .await?;

        Ok(())
    }
}
