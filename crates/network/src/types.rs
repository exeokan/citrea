use libp2p::{request_response::InboundRequestId, PeerId};
use serde::{Deserialize, Serialize};
use sov_rollup_interface::rpc::block::L2BlockResponse;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BlocksByRangeRequest {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Eth2Request {
    Status,
    BlocksByRange(BlocksByRangeRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub head_block: u64,
    pub last_pruned_block: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Eth2Response {
    Status(StatusResponse),
    BlocksByRange(Vec<L2BlockResponse>),
}

pub enum NetworkRequest {
    PublishMessage { topic: String, message: Vec<u8> },
    AddPeer { peer_id: String },
    RemovePeer { peer_id: String },
    GetL2BlockRange { start: u64, end: u64 },
    ReportPeer { peer_id: String },
    GetPeerStatus { peer_id: String },
}

pub enum L2SyncMessage {
    GossipBlock,
    BlockBatch(Vec<()>),
    NewPeer(PeerId),
    DisconnectPeer(PeerId),
    PeerStatus(StatusResponse),
}

#[allow(dead_code)] // TODO: remove when all events are handled
pub(crate) enum NetworkEvent {
    GossipBlock,
    RequestReceived {
        request_id: InboundRequestId,
        request: Eth2Request,
    },
    ResponseReceived {
        peer_id: PeerId,
        response: Eth2Response,
    },
    NewPeers(Vec<PeerId>),
    DisconnectPeer(PeerId),
}
