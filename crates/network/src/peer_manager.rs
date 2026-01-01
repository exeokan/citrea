use std::sync::Arc;
use std::time::{Duration, Instant};

use libp2p::PeerId;
use crate::types::PeerAction;
use crate::NetworkGlobals;

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
        Self {
            network_globals,
            target_peers,
            score_halflife,
            last_decay: Instant::now(),
        }
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
        match num_peers.cmp(&self.target_peers) {
            // Need more peers
            std::cmp::Ordering::Less => {
                let wanted = self.target_peers - num_peers;
                HeartbeatResult::WantedPeers(wanted)
            }
            std::cmp::Ordering::Greater => {
                // Too many peers, need to drop some
                let prune_target = num_peers - self.target_peers;
                let excess_peers = self.prune_peers(prune_target).await;
                if !excess_peers.is_empty() {
                    tracing::info!("Pruned {} excess peer(s)", excess_peers.len());
                }
                HeartbeatResult::ExcessPeers(excess_peers)
            }
            std::cmp::Ordering::Equal => HeartbeatResult::NoAction,
        }
    }

    pub async fn should_dial_peer(
        &self,
        peer_id: PeerId,
    ) -> bool {
        let peers = self.network_globals.peers.read().await;
        let connected_count = peers
            .iter()
            .filter(|(_, info)| info.is_connected)
            .count();
        if connected_count >= self.target_peers {
            return false;
        }

        if let Some(peer_info) = peers.get(&peer_id) {
            if peer_info.is_connected || peer_info.score.is_banned() {
                return false;
            }
        }
        true
    }

    pub async fn connected_peer(&self, peer_id: &PeerId) {
        let mut peers = self.network_globals.peers.write().await;
        let entry = peers.entry(*peer_id).or_default();
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
        let peers = self.network_globals.peers.read().await;

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

        peers_by_score
            .into_iter()
            .take(num_peers)
            .map(|(peer_id, _)| peer_id)
            .collect()
    }
}
