use anyhow::Result;
use citrea_common::NetworkConfig;
use futures::stream::StreamExt;
use gossipsub::Message as GossipsubMessage;
use libp2p::gossipsub::{MessageAcceptance, MessageId};
use libp2p::request_response::{InboundRequestId, OutboundRequestId, ResponseChannel};
use libp2p::swarm::{ConnectionId, NetworkBehaviour, SwarmEvent};
use libp2p::{
    gossipsub, mdns, noise, request_response, tcp, yamux, Multiaddr, PeerId, Swarm, SwarmBuilder,
};
use sov_rollup_interface::rpc::block::L2BlockResponse;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::config::build_gossipsub_config;
use crate::peer_manager::{HeartbeatResult, PeerManager, ReportPeerResult};
use crate::types::{Eth2Request, Eth2Response, NetworkEvent, PeerAction, SCORE_HALFLIFE};

mod config;
mod peer_manager;
mod rpc;
pub mod service;
pub mod types;

pub use service::NetworkService;
pub use types::{NetworkRequest, PeerInfo, PeerStatus};

#[derive(Default)]
pub struct NetworkGlobals {
    pub peers: RwLock<HashMap<PeerId, PeerInfo>>,
}
impl NetworkGlobals {
    pub fn new() -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
        }
    }
}

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
    eth2_rpc: rpc::Eth2Behaviour,
}

struct Network {
    swarm: Swarm<MyBehaviour>,
    peer_manager: PeerManager,
    pending_inbound_requests: HashMap<InboundRequestId, ResponseChannel<Eth2Response>>,
    pending_outbound_requests: HashMap<OutboundRequestId, Eth2Request>,
    connection_id_by_peer_id: HashMap<PeerId, Vec<ConnectionId>>,
    discovery_enabled: bool,
}

impl Network {
    fn build(network_config: NetworkConfig, network_globals: Arc<NetworkGlobals>) -> Result<Self> {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_quic()
            .with_behaviour(|key| {
                // To content-address message, we can take the hash of message and use it as an ID.
                // build a gossipsub network behaviour
                let gossipsub: gossipsub::Behaviour = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    build_gossipsub_config(&network_config.gossipsub_config)?,
                )?;
                let mdns = mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    key.public().to_peer_id(),
                )?;

                Ok(MyBehaviour {
                    gossipsub,
                    mdns,
                    eth2_rpc: rpc::create_eth2_behaviour(),
                })
            })?
            .build();

        let topic = gossipsub::IdentTopic::new("new-head");
        swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

        // Listen on all interfaces and whatever port the OS assigns
        swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
        
        for addr in network_config.dial_addresses {
            let addr: Multiaddr = addr.parse()?;
            info!("Dialing peer at {addr}");
            swarm.dial(addr)?;
        }

        let peer_manager = PeerManager::new(
            network_globals.clone(),
            network_config.target_peers,
            SCORE_HALFLIFE,
        );
        Ok(Self {
            swarm,
            peer_manager,
            pending_inbound_requests: HashMap::new(),
            pending_outbound_requests: HashMap::new(),
            connection_id_by_peer_id: HashMap::new(),
            discovery_enabled: network_config.discovery_enabled,
        })
    }

    pub async fn next_event(&mut self) -> Result<NetworkEvent> {
        loop {
            match self.swarm.select_next_some().await {
                SwarmEvent::Behaviour(event) => {
                    let network_event = match event {
                        MyBehaviourEvent::Eth2Rpc(event) => self.on_eth2_rpc_event(event).await,
                        MyBehaviourEvent::Mdns(event) => self.on_mdns_event(event).await,
                        MyBehaviourEvent::Gossipsub(event) => self.on_gossipsub_event(event).await,
                    };
                    if let Some(event) = network_event {
                        return Ok(event);
                    }
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    info!("Local node is listening on {address}");
                }
                SwarmEvent::ConnectionEstablished {
                    peer_id,
                    connection_id,
                    ..
                } => {
                    info!("Connection established with peer {peer_id} (connection id: {connection_id})");
                    self.connection_id_by_peer_id
                        .entry(peer_id)
                        .or_default()
                        .push(connection_id);
                    self.peer_manager.connected_peer(&peer_id).await;
                }
                SwarmEvent::ConnectionClosed {
                    peer_id,
                    connection_id,
                    ..
                } => {
                    info!("Connection closed with peer {peer_id} (connection id: {connection_id})");
                    if let Some(connections) = self.connection_id_by_peer_id.get_mut(&peer_id) {
                        connections.retain(|&id| id != connection_id);
                        if connections.is_empty() {
                            self.connection_id_by_peer_id.remove(&peer_id);
                            self.peer_manager.disconnected_peer(&peer_id).await;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    pub fn send_rpc_request(&mut self, peer_id: PeerId, request: Eth2Request) {
        let request_id = self
            .swarm
            .behaviour_mut()
            .eth2_rpc
            .send_request(&peer_id, request.clone());

        self.pending_outbound_requests.insert(request_id, request);
    }

    // P2P-TODO: what happens when the response is too large?
    pub fn send_rpc_response(
        &mut self,
        request_id: InboundRequestId,
        response: Eth2Response,
    ) -> anyhow::Result<()> {
        let channel = self
            .pending_inbound_requests
            .remove(&request_id)
            .ok_or_else(|| {
                anyhow::anyhow!("No pending inbound request found for the request id: {request_id}")
            })?;

        if let Err(_failed_response) = self
            .swarm
            .behaviour_mut()
            .eth2_rpc
            .send_response(channel, response)
        {
            error!("Failed to send response, request id: {request_id}");
        };
        Ok(())
    }

    pub fn publish_message(&mut self, topic: &str, message: Vec<u8>) {
        let gossipsub_topic = gossipsub::IdentTopic::new(topic);
        if let Err(e) = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(gossipsub_topic, message)
        {
            error!("Failed to publish message: {:?}", e);
        }
    }

    async fn on_mdns_event(&mut self, event: mdns::Event) -> Option<NetworkEvent> {
        match event {
            mdns::Event::Discovered(list) => {
                if !self.discovery_enabled {
                    return None;
                }
                tracing::debug!("mDNS discovered {} new peers", list.len());
                let to_dial = self.peer_manager.discovered_peers(list).await;
                for (peer_id, multiaddr) in to_dial {
                    if let Err(e) = self.swarm.dial(multiaddr.clone()) {
                        error!("Failed to dial discovered peer {peer_id} at {multiaddr}: {e}");
                    } else {
                        info!("Dialed discovered peer {peer_id} at {multiaddr}");
                    }
                }
            }
            mdns::Event::Expired(_) => {}
        }
        None
    }

    async fn on_gossipsub_event(&mut self, event: gossipsub::Event) -> Option<NetworkEvent> {
        match event {
            gossipsub::Event::Message {
                propagation_source,
                message_id,
                message,
            } => {
                let GossipsubMessage { data, .. } = message;
                let l2_block_response: L2BlockResponse = match serde_json::from_slice(&data) {
                    Ok(msg) => msg,
                    Err(_) => {
                        self.report_message_validation_result(
                            &propagation_source,
                            message_id,
                            MessageAcceptance::Reject,
                        );
                        return None;
                    }
                };
                Some(NetworkEvent::GossipBlock {
                    peer_id: propagation_source,
                    l2_block_response,
                    message_id,
                })
            }
            gossipsub::Event::GossipsubNotSupported { peer_id } => {
                self.report_peer(&peer_id, PeerAction::Fatal).await;
                None
            }
            gossipsub::Event::SlowPeer { peer_id, .. } => {
                self.report_peer(&peer_id, PeerAction::HighToleranceError)
                    .await;
                None
            }
            gossipsub::Event::Subscribed { .. } | gossipsub::Event::Unsubscribed { .. } => None,
        }
    }

    async fn on_eth2_rpc_event(
        &mut self,
        event: request_response::Event<Eth2Request, Eth2Response>,
    ) -> Option<NetworkEvent> {
        match event {
            request_response::Event::Message { peer, message, .. } => {
                match message {
                    request_response::Message::Request {
                        request_id,
                        request,
                        channel,
                    } => {
                        self.pending_inbound_requests.insert(request_id, channel);
                        // send the request to the upper layer, which will call send_rpc_response once ready
                        // P2P-TODO: consider using peer_id here
                        Some(NetworkEvent::RequestReceived {
                            request_id,
                            request,
                        })
                    }
                    request_response::Message::Response {
                        request_id,
                        response,
                    } => {
                        self.pending_outbound_requests.remove(&request_id);
                        Some(NetworkEvent::ResponseReceived {
                            peer_id: peer,
                            response,
                        })
                    }
                }
            }
            request_response::Event::OutboundFailure {
                peer, request_id, ..
            } => {
                let failed_request = self
                    .pending_outbound_requests
                    .remove(&request_id)
                    .expect("Failed outbound request must be tracked");
                Some(NetworkEvent::RPCFailed {
                    peer_id: peer,
                    request: failed_request,
                })
            }
            request_response::Event::InboundFailure { request_id, .. } => {
                // P2P-TODO: Consider taking action on inbound failures based on error,
                // including disconnection, and unsupported protocol
                self.pending_inbound_requests.remove(&request_id);
                None
            }
            request_response::Event::ResponseSent { .. } => None,
        }
    }

    /// Informs the gossipsub about the result of a message validation.
    /// If the message is valid it will get propagated by gossipsub.
    pub fn report_message_validation_result(
        &mut self,
        propagation_source: &PeerId,
        message_id: MessageId,
        validation_result: MessageAcceptance,
    ) {
        self.swarm
            .behaviour_mut()
            .gossipsub
            .report_message_validation_result(&message_id, propagation_source, validation_result);
    }

    pub fn disconnect_peer(&mut self, peer_id: &PeerId) {
        let Some(connections) = self.connection_id_by_peer_id.get(peer_id) else {
            error!("No connections found for peer {peer_id}, cannot disconnect");
            return;
        };
        for connection_id in connections {
            if !self.swarm.close_connection(*connection_id) {
                error!(
                    "Failed to close connection to peer {peer_id}, connection id: {connection_id}"
                );
            }
        }
    }

    pub async fn peer_manager_heartbeat(&mut self) -> HeartbeatResult {
        self.peer_manager.heartbeat().await
    }

    pub async fn report_peer(&mut self, peer_id: &PeerId, action: PeerAction) -> ReportPeerResult {
        self.peer_manager.report_peer(peer_id, action).await
    }
}
