use crate::{
    models::{Block, BlockNumber, H256},
    sentry::chain_config::ChainConfig,
    sentry2::types::*,
};
use async_trait::async_trait;
use ethereum_interfaces::sentry as grpc_sentry;
use futures_util::{FutureExt, StreamExt};
use std::{collections::HashSet, pin::Pin, sync::Arc};
use tokio::sync::RwLock as AsyncMutex;
use tracing::{debug, instrument, warn};

#[derive(Debug, Clone, Copy, Default)]
pub struct Status {
    pub height: u64,
    pub hash: H256,
    pub total_difficulty: H256,
}

impl From<Status> for grpc_sentry::StatusData {
    fn from(_status: Status) -> Self {
        todo!();
    }
}

pub struct HeaderDownloader {
    pub bad_headers: Arc<AsyncMutex<HashSet<H256>>>,
}

pub struct BodyDownaloder {}

pub type SentryClient = grpc_sentry::sentry_client::SentryClient<tonic::transport::Channel>;

#[derive(Clone)]
pub struct Coordinator {
    pub sentries: Vec<SentryClient>,
    pub header_downloader: Arc<HeaderDownloader>,
    pub body_downloader: Arc<BodyDownaloder>,
    pub status: Arc<AsyncMutex<Status>>,
    pub chain_config: Option<ChainConfig>,
    pub forks: Vec<u64>,
    pub genesis_hash: H256,
    pub network_id: u64,
}

impl Coordinator {
    pub fn new(
        sentries: Vec<SentryClient>,
        header_downloader: Arc<HeaderDownloader>,
        status: Arc<AsyncMutex<Status>>,
        _chain_config: Option<ChainConfig>,
        forks: Vec<u64>,
        genesis_hash: H256,
        network_id: u64,
    ) -> Self {
        Self {
            sentries,
            header_downloader,
            body_downloader: Arc::new(BodyDownaloder {}),
            chain_config: None,
            forks,
            genesis_hash,
            network_id,
            status,
        }
    }
}

pub type SentryInboundStream = futures_util::stream::Map<
    tonic::Streaming<grpc_sentry::InboundMessage>,
    fn(Result<grpc_sentry::InboundMessage, tonic::Status>) -> Option<InboundMessage>,
>;

#[async_trait]
#[allow(unreachable_code)]
impl SentryCoordinator for Coordinator {
    async fn set_status(&mut self) -> anyhow::Result<()> {
        let status_data: grpc_sentry::StatusData = (*self.status.read().await).into();
        let mut futs = Vec::new();
        for sentry in self.sentries.iter_mut() {
            futs.push(sentry.set_status(status_data.clone()))
        }
        futures_util::future::join_all(futs).await;

        Ok(())
    }
    async fn send_body_request(&mut self, req: BodyRequest) -> anyhow::Result<()> {
        let transform = move |_req: BodyRequest| -> anyhow::Result<Message> {
            Err(anyhow::anyhow!("Not implemented"))
        };
        let msg = transform(req).unwrap();
        let predicate =
            move || -> anyhow::Result<PeerFilter> { Err(anyhow::anyhow!("Not implemented")) };
        self.send_message(msg, predicate().unwrap()).await?;
        Ok(())
    }
    async fn send_header_request(&mut self, req: HeaderRequest) -> anyhow::Result<()> {
        let msg = Message::GetBlockHeaders(GetBlockHeaders {
            request_id: rand::Rng::gen::<u64>(&mut rand::thread_rng()),
            params: GetBlockHeadersParams {
                start: BlockId::Hash(req.hash),
                limit: req.limit,
                skip: req.skip.unwrap_or(0),
                reverse: if req.reverse { 1 } else { 0 },
            },
        });
        let predicate = PeerFilter::MinBlock(req.number.0);
        self.send_message(msg, predicate).await?;

        Ok(())
    }
    async fn recv(&mut self, msg_ids: Vec<i32>) -> anyhow::Result<CoordinatorStream> {
        Ok(futures_util::stream::select_all(
            futures_util::future::join_all(
                self.sentries
                    .iter()
                    .map(|s| recv_sentry(s, msg_ids.clone()))
                    .collect::<Vec<_>>(),
            )
            .await,
        ))
    }

    async fn recv_headers(&mut self) -> anyhow::Result<CoordinatorStream> {
        Ok(futures_util::stream::select_all(
            futures_util::future::join_all(
                self.sentries
                    .iter()
                    .map(|s| {
                        recv_sentry(
                            s,
                            vec![grpc_sentry::MessageId::from(MessageId::BlockHeaders) as i32],
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .await,
        ))
    }

    async fn broadcast_block(
        &mut self,
        _block: Block,
        _total_difficulty: u128,
    ) -> anyhow::Result<()> {
        let _fut = async move || {};
        Ok(())
    }
    async fn propagate_new_block_hashes(
        &mut self,
        _block_hashes: Vec<(H256, BlockNumber)>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn propagate_transactions(&mut self, _transactions: Vec<H256>) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_head(
        &mut self,
        height: u64,
        hash: H256,
        total_difficultyy: H256,
    ) -> anyhow::Result<()> {
        let status = Status {
            height,
            hash,
            total_difficulty: total_difficultyy,
        };
        self.status.write().await.clone_from(&status);
        self.set_status().await?;

        Ok(())
    }

    async fn penalize(&mut self, penalties: Vec<Penalty>) -> anyhow::Result<()> {
        let sentry_penalize = async move |mut s: SentryClient,
                                          penalty: Penalty|
                    -> Result<tonic::Response<()>, tonic::Status> {
            s.penalize_peer(grpc_sentry::PenalizePeerRequest {
                peer_id: Some(penalty.peer_id),
                penalty: 0,
            })
            .await
        };

        let mut futures = Vec::new();
        self.sentries.iter().for_each(|s| {
            penalties.iter().for_each(|p| {
                futures.push(sentry_penalize(s.clone(), p.clone()));
            });
        });
        futures_util::future::join_all(futures).await;
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
                        peer_id: Some(peer_id),
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

    async fn peer_count(&mut self) -> anyhow::Result<u64> {
        let peer_count: u64 = futures_util::future::join_all(
            self.sentries
                .iter()
                .map(
                    async move |s| -> anyhow::Result<grpc_sentry::PeerCountReply> {
                        let mut s = s.clone();
                        s.hand_shake(tonic::Request::new(())).await?;
                        Ok(s.peer_count(grpc_sentry::PeerCountRequest {})
                            .await?
                            .into_inner())
                    },
                )
                .collect::<Vec<_>>(),
        )
        .await
        .into_iter()
        .map(move |r| if let Ok(r) = r { r.count } else { 0 })
        .sum();

        Ok(peer_count)
    }
}
async fn recv_sentry(s: &SentryClient, ids: Vec<i32>) -> SingleSentryStream {
    let mut s = s.clone();
    s.hand_shake(tonic::Request::new(())).await.unwrap();
    debug!("Handshake with sentry {:?} done", s);

    poll_sentry_stream(
        s.messages(grpc_sentry::MessagesRequest { ids })
            .await
            .unwrap()
            .into_inner(),
    )
}

pub type SingleSentryStream =
    Pin<Box<dyn tokio_stream::Stream<Item = grpc_sentry::InboundMessage> + Send>>;

pub type CoordinatorStream = futures_util::stream::SelectAll<SingleSentryStream>;

#[instrument(level = "debug", name = "poll_sentry_stream")]
fn poll_sentry_stream(
    mut stream: tonic::Streaming<grpc_sentry::InboundMessage>,
) -> SingleSentryStream {
    Box::pin(async_stream::stream! {
        debug!("Starting to poll SingleSentryStream");
        while let Some(msg) = stream.next().await {
            debug!("Polling: Received message {:?}", msg);
            match msg {
                Ok(message) => yield message,
                _ => continue,
            }
        }
    })
}

#[async_trait]
pub trait SentryCoordinator: Send + Sync {
    async fn set_status(&mut self) -> anyhow::Result<()>;
    async fn send_body_request(&mut self, req: BodyRequest) -> anyhow::Result<()>;
    async fn send_header_request(&mut self, req: HeaderRequest) -> anyhow::Result<()>;
    async fn recv(&mut self, msg_ids: Vec<i32>) -> anyhow::Result<CoordinatorStream>;
    async fn recv_headers(&mut self) -> anyhow::Result<CoordinatorStream>;
    async fn broadcast_block(&mut self, block: Block, total_difficulty: u128)
        -> anyhow::Result<()>;
    async fn propagate_new_block_hashes(
        &mut self,
        block_hashes: Vec<(H256, BlockNumber)>,
    ) -> anyhow::Result<()>;
    async fn propagate_transactions(&mut self, transactions: Vec<H256>) -> anyhow::Result<()>;
    async fn update_head(
        &mut self,
        height: u64,
        hash: H256,
        total_difficulty: H256,
    ) -> anyhow::Result<()>;
    async fn penalize(&mut self, penalties: Vec<Penalty>) -> anyhow::Result<()>;
    async fn send_message(&mut self, message: Message, predicate: PeerFilter)
        -> anyhow::Result<()>;
    async fn peer_count(&mut self) -> anyhow::Result<u64>;
}
