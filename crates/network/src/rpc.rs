use libp2p::{
    request_response::{self, ProtocolSupport}, StreamProtocol,
};
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
pub(crate) struct StatusResponse {
    pub head_block: u64,
    pub last_pruned_block: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Eth2Response {
    Status(StatusResponse),
    BlocksByRange(Vec<L2BlockResponse>),
}

// 2. Create the behaviour using built-in JSON codec
// TODO: consider cbor codec
pub(crate) fn create_eth2_behaviour() -> request_response::json::Behaviour<Eth2Request, Eth2Response> {
    let protocols = vec![
        (StreamProtocol::new("/eth2/beacon_chain/req/status/1/json"), ProtocolSupport::Full),
        (StreamProtocol::new("/eth2/beacon_chain/req/beacon_blocks_by_range/2/json"), ProtocolSupport::Full),
    ];
    request_response::json::Behaviour::new(
        protocols.into_iter(),
        request_response::Config::default(),
    )
}

pub(crate) type Eth2Behaviour = request_response::json::Behaviour<Eth2Request, Eth2Response>;