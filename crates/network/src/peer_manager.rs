use std::{sync::Arc, time::{Duration, Instant}};
use rand::seq::SliceRandom;
use libp2p::{PeerId, Multiaddr};

use crate::{types::PeerAction, NetworkGlobals};

pub enum ReportPeerResult {
    Ban,
    NoAction,
}

pub enum HeartbeatResult {
    WantedPeers(usize),
    ExcessPeers(Vec<PeerId>),
    NoAction,
}

pub(crate) struct PeerManager {
    network_globals: Arc<NetworkGlobals>,
    target_peers: usize,
    score_halflife: Duration,
    last_decay: Instant,
}

impl PeerManager {
    pub fn new(
        network_globals: Arc<NetworkGlobals>,
        target_peers: usize,
        score_halflife: Duration,
    ) -> Self {
        Self { network_globals, target_peers, score_halflife, last_decay: Instant::now() }
    }

    pub async fn report_peer(&self, peer_id: &PeerId, action: PeerAction) -> ReportPeerResult {
        let mut peers = self.network_globals.peers.write().await;
        if let Some(peer_info) = peers.get_mut(peer_id) {
            let was_banned = peer_info.score.is_banned();
            peer_info.score.apply_peer_action(action);
            if !was_banned && peer_info.score.is_banned() {
                return ReportPeerResult::Ban;
            }
        }
        ReportPeerResult::NoAction
    }

    pub async fn heartbeat(&mut self) -> HeartbeatResult {
        // Decay scores if score halflife has passed
        if self.last_decay.elapsed() >= self.score_halflife {
            self.decay_scores().await;
            self.last_decay = Instant::now();
        }
        // Check peer count against target
        let num_peers = self.network_globals.peers.read().await.len();
        if num_peers < self.target_peers {
            // Need more peers
            let wanted = self.target_peers - num_peers;
            HeartbeatResult::WantedPeers(wanted)
        } else if num_peers > self.target_peers {
            // Too many peers, need to drop some
            let prune_target = num_peers - self.target_peers;
            let excess_peers = self.prune_peers(prune_target).await;
            if !excess_peers.is_empty() {
                tracing::info!("Pruned {} excess peer(s)", excess_peers.len());
            }
            HeartbeatResult::ExcessPeers(excess_peers)
        } else {
            HeartbeatResult::NoAction
        }
    }

    pub async fn discovered_peers(&self, peer_ids: Vec<(PeerId, Multiaddr)>) -> Vec<(PeerId, Multiaddr)> {
        let peers = self.network_globals.peers.read().await;
        if peers.len() >= self.target_peers {
            return vec![];
        }

        let mut peers_to_dial = Vec::new();
        for (peer_id, multiaddr) in peer_ids {
            if let Some(peer_info) = peers.get(&peer_id) {
                if peer_info.is_connected || peer_info.score.is_banned() {
                    continue;
                }
            }
            peers_to_dial.push((peer_id, multiaddr));
        }

        let remaining_slots = self.target_peers - peers.len();
        // randomly select peers to dial if more than remaining slots
        if peers_to_dial.len() > remaining_slots {
            let mut rng = rand::thread_rng();
            peers_to_dial.shuffle(&mut rng);
            peers_to_dial.truncate(remaining_slots);
        }
        peers_to_dial
    }

    pub async fn connected_peer(&self, peer_id: &PeerId) {
        let mut peers = self.network_globals.peers.write().await;
        let entry = peers
            .entry(*peer_id)
            .or_default();
        entry.is_connected = true;
    }

    pub async fn disconnected_peer(&self, peer_id: &PeerId) {
        let mut peers = self.network_globals.peers.write().await;
        let Some(entry) = peers.get_mut(peer_id) else {
            tracing::error!("Disconnected peer {} not found in peer manager", peer_id);
            return;
        };
        entry.is_connected = false;
    }

    async fn decay_scores(&self) {
        let mut peers = self.network_globals.peers.write().await;
        for (_, peer_info) in peers.iter_mut() {
            peer_info.score.decay();
        }
    }

    async fn prune_peers(&self, num_peers: usize) -> Vec<PeerId> {
        let peers = self.network_globals.peers
            .read()
            .await;

        let connected_peers: Vec<_> = peers
            .iter()
            .filter(|(_, info)| info.is_connected && info.status.is_some())
            .collect();

        let mut peers_by_score: Vec<_> = connected_peers
            .into_iter()
            .map(|(peer_id, info)| (*peer_id, &info.score))
            .collect();

        // Sort peers by score
        peers_by_score.sort_by_key(|&(_, score)| score);
        // Prune the lowest-scoring peers
        let peers_to_prune = peers_by_score
            .into_iter()
            .take(num_peers)
            .map(|(peer_id, _)| peer_id)
            .collect();

        return peers_to_prune;
    }
    
}