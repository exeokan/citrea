use serde::{Deserialize, Serialize};

use crate::utils::read_env;
use crate::FromEnv;

const fn default_target_peers() -> usize {
    10
}

const fn default_discovery_enabled() -> bool {
    true
}

/// Network configuration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct NetworkConfig {
    /// Optional peer multiaddresses.
    #[serde(default)]
    pub dial_addresses: Vec<String>,
    /// Gossipsub configuration.
    #[serde(default)]
    pub gossipsub_config: GossipsubConfig,
    /// Target number of peers to maintain.
    #[serde(default = "default_target_peers")]
    pub target_peers: usize,
    /// Whether to add peers using discovery module.
    #[serde(default = "default_discovery_enabled")]
    pub discovery_enabled: bool,
    /// Optional UDP port for libp2p.
    pub udp_port: Option<u16>,
    /// Optional TCP port for libp2p.
    pub tcp_port: Option<u16>,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            dial_addresses: Vec::new(),
            gossipsub_config: GossipsubConfig::default(),
            target_peers: default_target_peers(),
            discovery_enabled: default_discovery_enabled(),
            udp_port: None,
            tcp_port: None,
        }
    }
}

const fn default_heartbeat_interval_secs() -> u64 {
    10
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct GossipsubConfig {
    pub heartbeat_interval_secs: u64,
}

impl Default for GossipsubConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_secs: default_heartbeat_interval_secs(),
        }
    }
}

impl FromEnv for GossipsubConfig {
    fn from_env() -> anyhow::Result<Self> {
        let heartbeat_interval_secs = read_env("GOSSIPSUB_HEARTBEAT_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(default_heartbeat_interval_secs);
        Ok(Self {
            heartbeat_interval_secs,
        })
    }
}

impl FromEnv for NetworkConfig {
    fn from_env() -> anyhow::Result<Self> {
        let dial_addresses = read_env("NETWORK_DIAL_ADDRESSES")?
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        let gossipsub_config = GossipsubConfig::from_env()?;
        let target_peers = read_env("NETWORK_TARGET_PEERS")?.parse()?;
        let discovery_enabled = read_env("NETWORK_DISCOVERY_ENABLED")?.parse()?;
        let udp_port = read_env("NETWORK_UDP_PORT")
            .ok()
            .and_then(|v| v.parse().ok());
        let tcp_port = read_env("NETWORK_TCP_PORT")
            .ok()
            .and_then(|v| v.parse().ok());

        Ok(Self {
            dial_addresses,
            gossipsub_config,
            target_peers,
            discovery_enabled,
            udp_port,
            tcp_port,
        })
    }
}
