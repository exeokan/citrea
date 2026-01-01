use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use citrea_network::types::PeerAction;
use citrea_network::{NetworkGlobals, NetworkRequest};
use libp2p::PeerId;
use sov_db::ledger_db::SharedLedgerOps;
use tokio::select;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

const HEAD_BLOCK_MARGIN: u64 = 5;
const SKIP_DOWNLOAD_IF_GOSSIP_WITHIN: Duration = Duration::from_secs(10);
const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(1);

pub struct DownloadInfo {
    pub peer_id: PeerId,
    pub start: u64,
    pub end: u64,
}

pub enum BatchProcessingError {
    DownloadFailed,
    ValidationError,
}
pub(crate) enum SyncManagerMessage {
    BatchProcessed(DownloadInfo, Result<(), BatchProcessingError>),
    GossipBlockProcessed(Instant),
}

enum DownloadState {
    Idle,
    Syncing(DownloadInfo),
}

pub(crate) struct SyncManager<DB>
where
    DB: SharedLedgerOps + Clone,
{
    ledger_db: DB,
    event_rx: mpsc::Receiver<SyncManagerMessage>,
    network_tx: mpsc::Sender<NetworkRequest>,
    network_globals: Arc<NetworkGlobals>,
    sync_blocks_count: u64,
    status_interval: Duration,
    sync_interval: Duration,
    download_state: DownloadState,
    gossip_block_processed_at: Option<Instant>,
}

impl<DB> SyncManager<DB>
where
    DB: SharedLedgerOps + Clone,
{
    pub fn new(
        ledger_db: DB,
        event_rx: mpsc::Receiver<SyncManagerMessage>,
        network_tx: mpsc::Sender<NetworkRequest>,
        network_globals: Arc<NetworkGlobals>,
        sync_blocks_count: u64,
        status_interval: Option<Duration>,
        sync_interval: Option<Duration>,
    ) -> Self {
        let status_interval = status_interval.unwrap_or(DEFAULT_STATUS_INTERVAL);
        let sync_interval = sync_interval.unwrap_or(DEFAULT_SYNC_INTERVAL);

        Self {
            ledger_db,
            event_rx,
            network_tx,
            network_globals,
            sync_blocks_count,
            status_interval,
            sync_interval,
            download_state: DownloadState::Idle,
            gossip_block_processed_at: None,
        }
    }

    pub async fn run(mut self) {
        let mut status_interval = tokio::time::interval(self.status_interval);
        let mut sync_interval = tokio::time::interval(self.sync_interval);

        loop {
            select! {
                Some(event) = self.event_rx.recv() => {
                    self.on_sync_manager_event(event).await;
                }
                _ = status_interval.tick() => {
                    let peers = self.network_globals.peers.read().await;
                    for peer_id in peers.keys() {
                        let request = NetworkRequest::GetPeerStatus(*peer_id);
                        self.send_network_message(request).await;
                        debug!("Requested status from peer {}", peer_id);
                    }
                }
                _ = sync_interval.tick() => {
                    if let DownloadState::Idle = self.download_state {
                        if let Err(e) = self.download_from_best_peer().await {
                            error!("Failed to download from best peer: {}", e);
                        }
                    }
                }
            }
        }
    }

    async fn on_sync_manager_event(&mut self, event: SyncManagerMessage) {
        match event {
            SyncManagerMessage::BatchProcessed(download_info, result) => {
                let DownloadState::Syncing(ds_info) = &self.download_state else {
                    unreachable!("Received BatchProcessed while not downloading");
                };
                assert_eq!(ds_info.peer_id, download_info.peer_id);
                assert_eq!(ds_info.start, download_info.start);
                // Set download state to idle regardless of success or failure
                self.download_state = DownloadState::Idle;
                if let Err(e) = result {
                    self.on_batch_processing_error(download_info.peer_id, e)
                        .await;
                } else {
                    debug!(
                        "Successfully processed L2 blocks {}-{} from peer {}",
                        download_info.start, download_info.end, download_info.peer_id
                    );
                    if let Err(e) = self.download_from_best_peer().await {
                        error!("Failed to download from best peer: {}", e);
                    }
                }
            }
            SyncManagerMessage::GossipBlockProcessed(processed_at) => {
                self.gossip_block_processed_at = Some(processed_at);
            }
        }
    }

    async fn download_from_best_peer(&mut self) -> anyhow::Result<()> {
        if let Some(last_processed) = self.gossip_block_processed_at {
            let duration_since = Instant::now().duration_since(last_processed);
            if duration_since < SKIP_DOWNLOAD_IF_GOSSIP_WITHIN {
                tracing::info!(
                    "Skipping download from best peer due to recent gossip block processing"
                );
                return Ok(());
            } else {
                debug!(
                    "Proceeding with download from best peer, last gossip block processed {} seconds ago",
                    duration_since.as_secs()
                );
            }
        }
        let head_block = self.ledger_db.get_head_l2_block_height()?.unwrap_or(0);
        let peers = self.network_globals.peers.read().await;

        // filter peers such that:
        let best_peer = peers
            .iter()
            // is connected
            .filter(|(_, info)| info.is_connected)
            // has status
            .filter_map(|(peer_id, info)| {
                info.status.as_ref().map(|status| (peer_id, info, status))
            })
            // has tx bodies
            .filter(|(_, _, status)| status.has_tx_bodies)
            // head block + HEAD_BLOCK_MARGIN > local head block
            .filter(|(_, _, status)| status.head_block + HEAD_BLOCK_MARGIN > head_block)
            // last pruned block <= local head height
            .filter(|(_, _, status)| {
                status
                    .last_pruned_block
                    .is_none_or(|pruned_height| pruned_height <= head_block)
            })
            // pick the one with highest head block
            .max_by_key(|(_, info, _)| (&info.score));

        let Some((peer_id, _, _)) = best_peer else {
            warn!("No suitable peer found for downloading L2 blocks");
            self.report_unuseful_peers().await?;
            return Ok(());
        };
        let start = head_block + 1;
        let end = start + self.sync_blocks_count - 1;
        // P2P-TODO: dynamically change sync blocks count if there is response errors
        let request = NetworkRequest::GetL2BlockRange {
            peer_id: *peer_id,
            start,
            end,
        };
        self.send_network_message(request).await;
        self.download_state = DownloadState::Syncing(DownloadInfo {
            peer_id: *peer_id,
            start,
            end,
        });
        Ok(())
    }

    async fn send_network_message(&self, request: NetworkRequest) {
        self.network_tx
            .send(request)
            .await
            .expect("Network channel closed");
    }

    async fn on_batch_processing_error(&mut self, peer_id: PeerId, error: BatchProcessingError) {
        match error {
            BatchProcessingError::DownloadFailed => {
                warn!("Download failed from peer {peer_id}");
                self.network_tx
                    .send(NetworkRequest::ReportPeer(
                        peer_id,
                        PeerAction::MidToleranceError,
                    ))
                    .await
                    .expect("Network channel closed");
            }
            BatchProcessingError::ValidationError => {
                warn!("Validation error when downloading from peer {peer_id}");
                self.network_tx
                    .send(NetworkRequest::ReportPeer(peer_id, PeerAction::Fatal))
                    .await
                    .expect("Network channel closed");
            }
        }
    }

    async fn report_unuseful_peers(&self) -> anyhow::Result<()> {
        let head_block = self
            .ledger_db
            .get_head_l2_block_height()
            .context("Failed to get head L2 block height")?
            .unwrap_or(0);
        let peers = self.network_globals.peers.read().await;

        for (peer_id, info) in peers.iter() {
            if let Some(status) = &info.status {
                if status.head_block + HEAD_BLOCK_MARGIN <= head_block
                    || status
                        .last_pruned_block
                        .is_some_and(|pruned_height| pruned_height > head_block)
                {
                    self.network_tx
                        .send(NetworkRequest::ReportPeer(
                            *peer_id,
                            PeerAction::LowToleranceError,
                        ))
                        .await
                        .expect("Network channel closed");
                }
            }
        }
        Ok(())
    }
}
