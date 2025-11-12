use std::{collections::HashMap, time::Duration};
use libp2p::PeerId;
use sov_db::ledger_db::SharedLedgerOps;
use tokio::{select, sync::mpsc};

use citrea_network::types::{NetworkRequest, StatusResponse};
use tracing::{error, warn};

pub(crate) enum SyncManagerMessage {
    // TODO: add gossip block to update known head
    // so that we can prune some peers that are not useful
    // we may also remove ledger db import and just rely on messages from l2 syncer
    BatchProcessed(PeerId, Result<(u64, u64), anyhow::Error>), // TODO change to proper result type
    NewPeer(PeerId),
    DisconnectPeer(PeerId),
    PeerStatus((PeerId, StatusResponse)),
    DownloadFailed(PeerId), // TODO: propagate from network service
}

enum DownloadState {
    Idle,
    Syncing { peer_id: PeerId, start: u64, end: u64 },
}

pub(crate) struct SyncManager<DB>
where
    DB: SharedLedgerOps + Clone,
{
    ledger_db: DB,
    event_rx: mpsc::Receiver<SyncManagerMessage>,
    network_tx: mpsc::Sender<NetworkRequest>,
    peer_states: HashMap<PeerId, Option<StatusResponse>>,
    sync_blocks_count: u64,
    status_interval: Duration,
    sync_interval: Duration,
    download_state: DownloadState,
}

impl<DB> SyncManager<DB>
where
    DB: SharedLedgerOps + Clone,
{
    pub fn new(
        ledger_db: DB,
        event_rx: mpsc::Receiver<SyncManagerMessage>,
        network_tx: mpsc::Sender<NetworkRequest>,
        sync_blocks_count: u64,
        status_interval: Duration,
        sync_interval: Duration,
    ) -> Self {
        Self {
            ledger_db,
            event_rx,
            network_tx,
            peer_states: HashMap::new(),
            sync_blocks_count,
            status_interval,
            sync_interval,
            download_state: DownloadState::Idle,
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
                    for peer_id in self.peer_states.keys() {
                        let request = NetworkRequest::GetPeerStatus(*peer_id);
                        if let Err(e) = self.send_network_message(request).await {
                            error!("Failed to request status from peer {}: {}", peer_id, e);
                        }
                    }
                }
                // TODO: If no response from download,
                // error will be handled on the network side, and error will be sent back to here
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
            SyncManagerMessage::BatchProcessed(_peer_id, result) => {
                match result {
                    Ok((start, end)) => {
                        match self.download_state {
                            DownloadState::Syncing { peer_id: _ds_peer_id, start: ds_start, end: ds_end } => {
                                // TODO: alt check ds_end > end: ok
                                if start == ds_start && end == ds_end {
                                    self.download_state = DownloadState::Idle;
                                } else {
                                    // TODO: handle unexpected range
                                }
                                // TODO: handle unexpected peer_id
                            }
                            _ => {} // TODO: handle unexpected state
                        }
                        
                    }
                    Err(_e) => {
                        // Handle error
                    }
                }
            }
            SyncManagerMessage::NewPeer(peer_id) => {
                self.peer_states.insert(peer_id, None);
            }
            SyncManagerMessage::DisconnectPeer(peer_id) => {
                self.peer_states.remove(&peer_id);
            }
            SyncManagerMessage::PeerStatus((peer_id, status)) => {
                self.peer_states.insert(peer_id, Some(status));
            }
            SyncManagerMessage::DownloadFailed(_peer_id) => {
                // Handle download failure
            }
        }
    }

    async fn download_from_best_peer(&self) -> anyhow::Result<()> {
        let head_block = self.ledger_db.get_head_l2_block_height()?.unwrap_or(0);
        // TODO: handle pruned blocks

        // filter peers such that:
        // - have status
        // - have head block > local head block
        let best_peer = self.peer_states.iter()
            .filter_map(|(peer_id, status_opt)| 
                status_opt.as_ref().map(|status| (*peer_id, status)))
            .filter(|(_, status)| status.head_block > head_block)
            .max_by_key(|(_, status)| status.head_block);

        let Some((peer_id, status)) = best_peer else {
            warn!("No suitable peer found for downloading L2 blocks");
            // TODO: slash some peers here
            return Ok(());
        };
        let start = head_block + 1;
        let end = (start + self.sync_blocks_count - 1).min(status.head_block); 
        // TODO: dynamically change sync blocks count if there is response errors

        let request = NetworkRequest::GetL2BlockRange { peer_id, start, end };
        if let Err(e) = self.send_network_message(request).await {
            error!("Failed to request L2 block range from peer {}: {}", peer_id, e);
        }
        Ok(())
    }

    async fn send_network_message(&self, request: NetworkRequest) -> anyhow::Result<()> {
        self.network_tx
            .send(request)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send network request: {}", e))
    }
}