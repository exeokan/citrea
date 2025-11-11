use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use anyhow::Result;
use citrea_common::NetworkConfig;
use futures::stream::StreamExt;
use libp2p::request_response::{InboundRequestId, OutboundRequestId, ResponseChannel};
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{gossipsub, mdns, noise, request_response, tcp, yamux, Multiaddr, PeerId, Swarm, SwarmBuilder};
pub use service::NetworkService;
use tokio::{io, select};
use tracing::{error, info};

use crate::rpc::Eth2Request;
use crate::types::NetworkEvent;
pub mod service;
pub mod types;
mod rpc;

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
    eth2_rpc: rpc::Eth2Behaviour,
}

struct OutboundRequest{
    peer_id: PeerId,
    timestamp: Instant,
}

struct Network {
    swarm: Swarm<MyBehaviour>,
    pending_inbound_requests: HashMap<InboundRequestId, ResponseChannel<rpc::Eth2Response>>,
    pending_outbound_requests: HashMap<OutboundRequestId, OutboundRequest>,
}

impl Network {
    fn build(network_config: NetworkConfig) -> Result<Self> {
        let heartbeat_interval =
            Duration::from_secs(network_config.gossipsub_config.heartbeat_interval_secs);

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
                    .map_err(io::Error::other)?; // Temporary hack because `build` does not return a proper `std::error::Error`.

                // build a gossipsub network behaviour
                let gossipsub: gossipsub::Behaviour = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )?;
                let mdns = mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    key.public().to_peer_id(),
                )?;

                Ok(MyBehaviour { gossipsub, mdns, eth2_rpc: rpc::create_eth2_behaviour() })
            })?
            .build();

        // Create a Gossipsub topic
        let topic = gossipsub::IdentTopic::new("test-net");
        // subscribes to our topic
        swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

        // Listen on all interfaces and whatever port the OS assigns
        swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

        let dial_addr = network_config.dial_addr;
        // Dial the peer identified by the multi-address given as the second
        // command-line argument, if any.
        if let Some(addr) = dial_addr.as_ref() {
            let remote: Multiaddr = addr.parse()?;
            swarm.dial(remote)?;
            info!("Dialed {addr}");
        }

        Ok(Self {
            swarm,
            pending_inbound_requests: HashMap::new(),
            pending_outbound_requests: HashMap::new(),
        })
    }

    pub async fn next_event(&mut self) -> Result<NetworkEvent> {
        let swarm = &mut self.swarm;

        loop {
            select! {
                event = swarm.select_next_some() => match event {
                    SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                        let mut ids = vec![];
                        for (peer_id, _multiaddr) in list {
                            info!("mDNS discovered a new peer: {peer_id}");
                            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                            ids.push(peer_id);
                        }
                        return Ok(NetworkEvent::NewPeers(ids));
                    },
                    SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                        for (peer_id, _multiaddr) in list {
                            info!("mDNS discover peer has expired: {peer_id}");
                            swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        }
                    },
                    SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                        propagation_source: peer_id,
                        message_id: id,
                        message,
                    })) => info!(
                            "Got message: '{}' with id: {id} from peer: {peer_id}",
                            String::from_utf8_lossy(&message.data),
                        ),

                    SwarmEvent::Behaviour(MyBehaviourEvent::Eth2Rpc(request_response::Event::Message {peer, message, .. })) => {
                        match message {
                            request_response::Message::Request { request_id, request, channel } => {
                                self.pending_inbound_requests.insert(request_id, channel);
                                // send the request to the upper layer, which will call send_rpc_response once ready
                                // TODO: consider using peer_id here
                                return Ok(NetworkEvent::RequestReceived {
                                    request_id,
                                    request,
                                });
                            },
                            request_response::Message::Response { request_id, response } => {
                                self.pending_outbound_requests.remove(&request_id);
                                return Ok(NetworkEvent::ResponseReceived {
                                    peer_id: peer,
                                    response,
                                });
                            }
                        }
                    }
                    // TODO handle other eth2rpc events
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!("Local node is listening on {address}");
                    }
                    _ => {}
                }
            }
        }
    }

    pub fn send_rpc_request(&mut self, peer_id: Option<PeerId>, request: Eth2Request) {
        // REMOVE ME: for testing only
        let peer_id = if let Some(peer_id) = peer_id {
            peer_id
        } else {
            // pick a random peer from the connected peers
            match self.swarm.connected_peers().next() {
                Some(p) => p.to_owned(),
                None => {
                    error!("No connected peers to send RPC request");
                    return;
                }
            }
        };

        let request_id = self
            .swarm
            .behaviour_mut()
            .eth2_rpc
            .send_request(&peer_id, request);

        let timestamp = Instant::now();
        let outbound_request = OutboundRequest {
            peer_id,
            timestamp,
        };
        self.pending_outbound_requests.insert(request_id, outbound_request);
    }

    // TODO: what happens when the response is too large?
    pub fn send_rpc_response(&mut self, request_id: InboundRequestId, response: rpc::Eth2Response) -> anyhow::Result<()> {
        let channel = self.pending_inbound_requests
            .remove(&request_id)
            .ok_or_else(|| anyhow::anyhow!("No pending inbound request found for the given request ID"))?;

        if let Err(failed_response) = self
            .swarm
            .behaviour_mut()
            .eth2_rpc
            .send_response(channel, response) {
            // TODO: handle this error
            error!("Failed to send status response: {:?}", failed_response);
        };
        Ok(())
    }
}
