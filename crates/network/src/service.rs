use citrea_common::NetworkConfig;
use reth_tasks::shutdown::GracefulShutdown;
use sov_db::ledger_db::SharedLedgerOps;
use sov_db::schema::types::l2_block::StoredL2Block;
use tokio::sync::mpsc;
use tracing::info;
use anyhow::{Context, Result};
use sov_db::schema::types::L2BlockNumber;

use crate::types::{L2SyncMessage, NetworkRequest, PeerStatus, NetworkEvent}; 
use crate::Network;

pub struct NetworkService<DB> 
    where DB: SharedLedgerOps
{
    network: Network,
    ledger_db: DB,
    request_rx: mpsc::Receiver<NetworkRequest>,
    _l2_sync_tx: Option<mpsc::Sender<L2SyncMessage>>,
    event_rx: mpsc::Receiver<NetworkEvent>,
}

impl<DB> NetworkService<DB>
where
    DB: SharedLedgerOps,
{
    pub fn build(
        network_config: NetworkConfig,
        ledger_db: DB,
        request_rx: mpsc::Receiver<NetworkRequest>,
        _l2_sync_tx: Option<mpsc::Sender<L2SyncMessage>>,
    ) -> Result<Self> {
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(100);

        let network = Network::build(network_config, event_tx).context("Failed to build network")?;
        Ok(Self {
            network,
            ledger_db,
            request_rx,
            _l2_sync_tx,
            event_rx,
        })
    }

    pub async fn run(mut self, mut shutdown_signal: GracefulShutdown) {
        let cloned_signal = shutdown_signal.clone();
        tokio::spawn(async move {
            self.network.run(cloned_signal).await
        });

        loop {
            tokio::select! {
                Some(_request) = self.request_rx.recv() => {
                    // Handle incoming network requests here
                }
                Some(_event) = self.event_rx.recv() => {
                    // Handle incoming network events here
                }
                _ = &mut shutdown_signal => {
                    info!("Shutting down NetworkService");
                    return;
                }
            }
        }
    }

    // TODO: Fix error/response type
    pub async fn l2_blocks_by_range(&self, start: u64, end: u64) -> Result<Vec<StoredL2Block>> {
        if end < start {
            return Err(anyhow::anyhow!("End block number must be greater than or equal to start block number"));
        }
        let diff = end - start;

        // TODO: Make this configurable
        if diff > 1000 {
            return Err(anyhow::anyhow!("Requested block range too large. Max range is 1000 blocks"));
        }

        // TODO: handle the case where the full node doesnt save transactions
        self.ledger_db
            .get_l2_block_range(&(L2BlockNumber(start)..=L2BlockNumber(end)))
            .map_err(|_| anyhow::anyhow!("Failed to get L2 blocks from ledger DB"))
    }

    pub async fn peer_status(&self) -> Result<PeerStatus> {
        unimplemented!()
    }
}
