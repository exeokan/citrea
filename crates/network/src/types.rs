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
    AddPeer(PeerId),
    RemovePeer(PeerId),
    GetL2BlockRange { peer_id: PeerId,start: u64, end: u64 },
    ReportPeer(PeerId), // TODO: add degree/reason
    GetPeerStatus(PeerId),
}

pub enum L2SyncMessage {
    GossipBlock(PeerId, L2BlockResponse),
    BlockBatch(PeerId, Vec<L2BlockResponse>),
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
