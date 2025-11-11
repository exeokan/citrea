use libp2p::{request_response::InboundRequestId, PeerId};

use crate::rpc::{Eth2Request, Eth2Response};

pub enum NetworkRequest {
    PublishMessage { topic: String, message: Vec<u8> },
    AddPeer { peer_id: String },
    RemovePeer { peer_id: String },
    GetL2BlockRange { start: u64, end: u64 },
    ReportPeer { peer_id: String },
    GetPeerStatus { peer_id: String },
}

pub struct PeerStatus;

pub enum L2SyncMessage {
    GossipBlock,
    BlockBatch(Vec<()>),
    NewPeer(String),
    DisconnectPeer(String),
    PeerStatus(PeerStatus),
}

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
    PeerStatus(PeerStatus),
}
