use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use citrea_common::NetworkConfig;
use discv5::enr::CombinedKey;
use discv5::Event as Discv5Event;
use futures::future::Either;
use futures::stream::StreamExt;
use gossipsub::Message as GossipsubMessage;
use libp2p::request_response::{InboundRequestId, OutboundRequestId, ResponseChannel};
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{
    gossipsub, noise, request_response, tcp, yamux, Multiaddr, PeerId, Swarm, SwarmBuilder,
};
pub use service::NetworkService;
use sov_rollup_interface::rpc::block::L2BlockResponse;
use tokio::{
    io,
    runtime::Handle,
    select,
    sync::mpsc,
    time::{Interval, MissedTickBehavior},
};
use tracing::{error, info, warn};

mod discovery;
use self::discovery::{
    enr_multiaddrs, enr_peer_id, prepare_identity, start_service, DiscoveryComponents,
    DiscoveryService,
};
use crate::types::{Eth2Request, Eth2Response, NetworkEvent};
mod rpc;
pub mod service;
pub mod types;

pub use service::NetworkService;

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    gossipsub: gossipsub::Behaviour,
    eth2_rpc: rpc::Eth2Behaviour,
}

struct Network {
    swarm: Swarm<MyBehaviour>,
    pending_inbound_requests: HashMap<InboundRequestId, ResponseChannel<Eth2Response>>,
    pending_outbound_requests: HashMap<OutboundRequestId, Eth2Request>,
    discovery: Option<DiscoveryService>,
    discovery_events: Option<mpsc::Receiver<Discv5Event>>,
    discovery_interval: Option<Interval>,
}

impl Network {
    fn build(network_config: NetworkConfig) -> Result<Self> {
        let heartbeat_interval =
            Duration::from_secs(network_config.gossipsub_config.heartbeat_interval_secs);

        let (identity_keypair, discovery_key) = prepare_identity(&network_config.discovery)?;

        let mut swarm = SwarmBuilder::with_existing_identity(identity_keypair)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_quic()
            .with_behaviour(|key| {
                // To content-address message, we can take the hash of message and use it as an ID.
                let message_id_fn = |message: &gossipsub::Message| {
                    let mut s = DefaultHasher::new();
                    message.data.hash(&mut s);
                    gossipsub::MessageId::from(s.finish().to_string())
                };
                // Set a custom gossipsub configuration
                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(heartbeat_interval) // This is set to aid debugging by not cluttering the log space
                    .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message
                    // signing)
                    .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
                    .build()
                    .map_err(tokio::io::Error::other)?; // Temporary hack because `build` does not return a proper `std::error::Error`.

                // build a gossipsub network behaviour
                let gossipsub: gossipsub::Behaviour = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )?;
                Ok(MyBehaviour {
                    gossipsub,
                    eth2_rpc: rpc::create_eth2_behaviour(),
                })
            })?
            .build();

        // Create a Gossipsub topic
        let topic = gossipsub::IdentTopic::new("new-head");
        // subscribes to our topic
        swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

        // Listen on all interfaces and whatever port the OS assigns
        swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

        let dial_addr = network_config.dial_addr;

        // P2P-TODO: implement for multiple addresses
        // Dial the peer identified by the multi-address given as the second
        // command-line argument, if any.
        if let Some(addr) = dial_addr.as_ref() {
            let remote: Multiaddr = addr.parse()?;
            swarm.dial(remote)?;
            info!("Dialed {addr}");
        }


        let (discovery, discovery_events, discovery_interval) = if let Some(enr_key) = discovery_key
        {
            let handle = Handle::try_current()
                .map_err(|_| anyhow!("Tokio runtime is required for discv5 discovery"))?;
            let DiscoveryComponents {
                service, event_rx, ..
            } = handle.block_on(start_service(&network_config.discovery, enr_key))?;
            let mut interval = tokio::time::interval(Duration::from_secs(
                network_config.discovery.query_interval_secs,
            ));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            (Some(service), Some(event_rx), Some(interval))
        } else {
            (None, None, None)
        };

        Ok(Self {
            swarm,
            pending_inbound_requests: HashMap::new(),
            pending_outbound_requests: HashMap::new(),
            discovery,
            discovery_events,
            discovery_interval,
        })
    }

    pub async fn next_event(&mut self) -> Result<NetworkEvent> {
        loop {
            let swarm_future = self.swarm.select_next_some();
            tokio::pin!(swarm_future);

            let discovery_future = if let Some(mut rx) = self.discovery_events.take() {
                Either::Left(async move {
                    let event = rx.recv().await;
                    (event, Some(rx))
                })
            } else {
                Either::Right(async { (None, None) })
            };
            tokio::pin!(discovery_future);

            let interval_future = if let Some(mut interval) = self.discovery_interval.take() {
                Either::Left(async move {
                    interval.tick().await;
                    Some(interval)
                })
            } else {
                Either::Right(async { None })
            };
            tokio::pin!(interval_future);

            select! {
                event = &mut swarm_future => {
                    match event {
                        SwarmEvent::Behaviour(event) => {
                            let network_event = match event {
                                MyBehaviourEvent::Eth2Rpc(event) => self.on_eth2_rpc_event(event).await,
                                MyBehaviourEvent::Gossipsub(event) => self.on_gossipsub_event(event).await,
                            };
                            if let Some(event) = network_event {
                                return Ok(event);
                            }
                        }
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!("Local node is listening on {address}");
                        }
                        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                            return Ok(NetworkEvent::NewPeer(peer_id));
                        }
                        SwarmEvent::ConnectionClosed { peer_id, .. } => {
                            return Ok(NetworkEvent::DisconnectedPeer(peer_id));
                        }
                        _ => {}
                    }
                
                }
                (event, receiver) = &mut discovery_future => {
                    if let Some(rx) = receiver {
                        if event.is_some() {
                            self.discovery_events = Some(rx);
                        }
                    }
                    if let Some(event) = event {
                        if let Some(network_event) = self.handle_discovery_event(event) {
                            return Ok(network_event);
                        }
                    }
                }
                interval = &mut interval_future => {
                    if let Some(interval) = interval {
                        self.discovery_interval = Some(interval);
                    }
                    if let Some(discovery) = &self.discovery {
                        if self.swarm.connected_peers().count() < discovery.target_peers() {
                            if let Err(err) = discovery.random_lookup().await {
                                warn!("discv5 random lookup failed: {err:?}");
                            }
                        }
                    }
                }
            }
        }
    }

    fn handle_discovery_event(&mut self, event: Discv5Event) -> Option<NetworkEvent> {
        match event {
            Discv5Event::Discovered(enr) => self.handle_discovered_enr(enr),
            Discv5Event::SessionEstablished(enr, _) => self.handle_discovered_enr(enr),
            Discv5Event::NodeInserted { node_id, .. } => {
                info!("discv5 inserted node {node_id}");
                None
            }
            Discv5Event::SocketUpdated(addr) => {
                info!("discv5 updated external socket to {addr}");
                None
            }
            Discv5Event::UnverifiableEnr { node_id, .. } => {
                warn!("Received unverifiable ENR for node {node_id}");
                None
            }
            Discv5Event::TalkRequest(_) => None,
            _ => None,
        }
    }

    fn handle_discovered_enr(
        &mut self,
        enr: discv5::enr::Enr<CombinedKey>,
    ) -> Option<NetworkEvent> {
        let peer_id = enr_peer_id(&enr)?;
        let addresses = enr_multiaddrs(&enr);
        if addresses.is_empty() {
            warn!("Discovered peer {peer_id} without reachable multiaddrs");
        }
        for addr in addresses {
            if let Err(err) = self.swarm.dial(addr.clone()) {
                warn!("Failed to dial peer {peer_id} via {addr}: {err:?}");
            }
        }
        self.swarm
            .behaviour_mut()
            .gossipsub
            .add_explicit_peer(&peer_id);
        Some(NetworkEvent::NewPeers(vec![peer_id]))
    }

    // TODO: what happens when the response is too large?
    pub fn send_rpc_response(
        &mut self,
        request_id: InboundRequestId,
        response: Eth2Response,
    ) -> anyhow::Result<()> {
        let channel = self
            .pending_inbound_requests
            .remove(&request_id)
            .ok_or_else(|| {
                anyhow::anyhow!("No pending inbound request found for the given request ID")
            })?;

        if let Err(failed_response) = self
            .swarm
            .behaviour_mut()
            .eth2_rpc
            .send_response(channel, response)
        {
            // TODO: handle this error
            error!("Failed to send status response: {:?}", failed_response);
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

    async fn on_gossipsub_event(&mut self, event: gossipsub::Event) -> Option<NetworkEvent> {
        match event {
            gossipsub::Event::Message {
                propagation_source: peer_id,
                message_id: _id,
                message,
            } => {
                // P2P-TODO: research gossipsub broadcast guarantees
                let GossipsubMessage { data, .. } = message; // P2P-TODO: consider handling topic/peer_id/sequence_number
                let l2_block_response: L2BlockResponse = match serde_json::from_slice(&data) {
                    Ok(msg) => msg,
                    // P2P-TODO: who to slash for bad messages, propagation source or the original sender?
                    Err(_) => return None,
                };
                Some(NetworkEvent::GossipBlock(peer_id, l2_block_response))
            }
            gossipsub::Event::GossipsubNotSupported { .. } => {
                // P2P-TODO: ban peer
                None
            }
            gossipsub::Event::SlowPeer { .. } => {
                // P2P-TODO: slash peer
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
                // P2P-TODO: slashing based on error here?
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
}
