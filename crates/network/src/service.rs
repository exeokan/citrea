use anyhow::{Context, Result};
use citrea_common::NetworkConfig;
use libp2p::request_response::InboundRequestId;
use reth_tasks::shutdown::GracefulShutdown;
use sov_db::ledger_db::{LedgerDB, SharedLedgerOps};
use sov_rollup_interface::rpc::block::L2BlockResponse;
use sov_rollup_interface::rpc::LedgerRpcProvider;
use tokio::sync::mpsc;
use tracing::info;

use crate::rpc::{Eth2Request, Eth2Response, StatusResponse};
use crate::types::{L2SyncMessage, NetworkEvent, NetworkRequest, PeerStatus};
use crate::Network;

pub struct NetworkService
{
    network: Network,
    ledger_db: LedgerDB,
    request_rx: mpsc::Receiver<NetworkRequest>,
    _l2_sync_tx: Option<mpsc::Sender<L2SyncMessage>>,
}

impl NetworkService {
    pub fn build(
        network_config: NetworkConfig,
        ledger_db: LedgerDB,
        request_rx: mpsc::Receiver<NetworkRequest>,
        _l2_sync_tx: Option<mpsc::Sender<L2SyncMessage>>,
    ) -> Result<Self> {

        let network =
            Network::build(network_config).context("Failed to build network")?;
        Ok(Self {
            network,
            ledger_db,
            request_rx,
            _l2_sync_tx,
        })
    }

    pub async fn run(mut self, mut shutdown_signal: GracefulShutdown) {
        let (response_tx, mut response_rx) = mpsc::channel::<(InboundRequestId, Result<Eth2Response>)>(100);
        let mut rpc_request_interval = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            tokio::select! {
                Some(_request) = self.request_rx.recv() => {
                    // Handle incoming network requests here
                }
                network_event = self.network.next_event() => {
                    let event = network_event.expect("Failed to get network event");
                    match event {
                        NetworkEvent::RequestReceived { request_id, request } => {
                            // don't block the event loop
                            let ledger_db = self.ledger_db.clone();
                            let tx = response_tx.clone();
                            tokio::spawn(async move {
                                let result = Self::handle_incoming_rpc_request(&ledger_db, request);
                                let _ = tx.send((request_id, result)).await;
                            });
                        }
                        NetworkEvent::ResponseReceived { peer_id, response } => {
                            info!("Response received from peer {peer_id}: {response:?}");
                        }
                        _ => {
                            info!("Received other network event");
                        }
                    }
                }
                Some((request_id, result)) = response_rx.recv() => {
                    match result {
                        // TODO: propagate the error to the caller peer
                        Ok(response) => {
                            if let Err(e) = self.network.send_rpc_response(request_id, response) {
                                tracing::error!("Error sending RPC response: {:?}", e);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error handling incoming RPC request: {:?}", e);
                        }
                    }
                }
                _ = rpc_request_interval.tick() => {
                    self.network.send_rpc_request(None, Eth2Request::BlocksByRange(
                        crate::rpc::BlocksByRangeRequest {
                            start: 1,
                            end: 3,
                        }
                    ));
                }

                _ = &mut shutdown_signal => {
                    info!("Shutting down NetworkService");
                    return;
                }
            }
        }
    }

    fn handle_incoming_rpc_request(
        ledger_db: &LedgerDB,
        request: Eth2Request,
    ) -> Result<Eth2Response> {
        match request {
            Eth2Request::Status => {
                // Handle status request
                // TODO: Implement proper status response
                let last_pruned_block = ledger_db.get_last_pruned_l2_height()?;
                let head_block = LedgerRpcProvider::get_head_l2_block_height(ledger_db)?;
                Ok(Eth2Response::Status(StatusResponse{
                    head_block,
                    last_pruned_block,
                }))
            }
            Eth2Request::BlocksByRange(blocks_request) => {
                let blocks = Self::l2_blocks_by_range(ledger_db, blocks_request.start, blocks_request.end)?;
                Ok(Eth2Response::BlocksByRange(blocks))
            }
        }
    }

    // TODO: Fix error/response type
    pub fn l2_blocks_by_range(ledger_db: &LedgerDB, start: u64, end: u64) -> Result<Vec<L2BlockResponse>> {
        let diff = end - start;

        // TODO: Make this configurable
        if diff > 1000 {
            return Err(anyhow::anyhow!(
                "Requested block range too large. Max range is 1000 blocks"
            ));
        }

        ledger_db.get_l2_blocks_range(start, end)?
            .into_iter()
            .map(|block_opt| {
                block_opt.ok_or_else(|| anyhow::anyhow!("Block not found"))
            })
            .collect()
    }

    pub async fn peer_status(&self) -> Result<PeerStatus> {
        unimplemented!()
    }
}
