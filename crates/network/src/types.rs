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

#[allow(dead_code)] // TODO: remove when used
pub(crate) enum NetworkEvent {
    GossipBlock,
    RpcResponse,
    RpcRequest,
    NewPeer(String),
    DisconnectPeer(String),
    PeerStatus(PeerStatus),
}