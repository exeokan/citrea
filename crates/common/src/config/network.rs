use serde::{Deserialize, Serialize};

use crate::utils::read_env;
use crate::FromEnv;
use std::{net::{IpAddr, SocketAddr}, path::PathBuf};

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
    #[serde(default)]
    pub discovery: DiscoveryConfig
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
            discovery: DiscoveryConfig::default(),
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
        let discovery = DiscoveryConfig::from_env()?;

        Ok(Self {
            dial_addresses,
            gossipsub_config,
            target_peers,
            discovery_enabled,
            udp_port,
            tcp_port,
            discovery,
        })
    }
}

fn default_discovery_bind_address() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 9000))
}

const LIGHTHOUSE_BOOTNODES: [&str; 4] = [
    "enr:-Iu4QLm7bZGdAt9NSeJG0cEnJohWcQTQaI9wFLu3Q7eHIDfrI4cwtzvEW3F3VbG9XdFXlrHyFGeXPn9snTCQJ9bnMRABgmlkgnY0gmlwhAOTJQCJc2VjcDI1NmsxoQIZdZD6tDYpkpEfVo5bgiU8MGRjhcOmHGD2nErK0UKRrIN0Y3CCIyiDdWRwgiMo",
    "enr:-Ku4QImhMc1z8yCiNJ1TyUxdcfNucje3BGwEHzodEZUan8PherEo4sF7pPHPSIB1NNuSg5fZy7qFsjmUKs2ea1Whi0EBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQOVphkDqal4QzPMksc5wnpuC3gvSC8AfbFOnZY_On34wIN1ZHCCIyg",
    "enr:-LK4QA8FfhaAjlb_BXsXxSfiysR7R52Nhi9JBt4F8SPssu8hdE1BXQQEtVDC3qStCW60LSO7hEsVHv5zm8_6Vnjhcn0Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhAN4aBKJc2VjcDI1NmsxoQJerDhsJ-KxZ8sHySMOCmTO6sHM3iCFQ6VMvLTe948MyYN0Y3CCI4yDdWRwgiOM",
    "enr:-Le4QLHZDSvkLfqgEo8IWGG96h6mxwe_PsggC20CL3neLBjfXLGAQFOPSltZ7oP6ol54OvaNqO02Rnvb8YmDR274uq8ChGV0aDKQtTA_KgEAAAAAIgEAAAAAAIJpZIJ2NIJpcISLosQxg2lwNpAqAX4AAAAAAPA8kv_-ax65iXNlY3AyNTZrMaEDBJj7_dLFACaxBfaI8KZTh_SSJUjhyAyfshimvSqo22WDdWRwgiMohHVkcDaCI4I",
];

fn default_discovery_bootnodes() -> Vec<String> {
    LIGHTHOUSE_BOOTNODES
        .iter()
        .map(|enr| (*enr).to_string())
        .collect()
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct DiscoveryConfig {
    #[serde(default = "default_discovery_enabled")]
    pub enabled: bool,
    #[serde(default = "default_discovery_bind_address")]
    pub udp_bind: SocketAddr,
    pub enr_address: Option<IpAddr>,
    pub enr_udp_port: Option<u16>,
    pub enr_tcp_port: Option<u16>,
    #[serde(default)]
    pub private_key_path: Option<PathBuf>,
    #[serde(default = "default_discovery_bootnodes")]
    pub bootnodes: Vec<String>,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: default_discovery_enabled(),
            udp_bind: default_discovery_bind_address(),
            enr_address: None,
            enr_udp_port: None,
            enr_tcp_port: None,
            private_key_path: None,
            bootnodes: default_discovery_bootnodes(),
        }
    }
}

fn parse_bool_flag(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

impl FromEnv for DiscoveryConfig {
    fn from_env() -> anyhow::Result<Self> {
        let enabled = read_env("NETWORK_DISCOVERY_ENABLED")
            .ok()
            .map(|v| parse_bool_flag(&v))
            .unwrap_or_else(default_discovery_enabled);

        let udp_bind = read_env("NETWORK_DISCOVERY_BIND_ADDR")
            .ok()
            .map(|addr| addr.parse())
            .transpose()
            .map_err(|e| anyhow::anyhow!("Invalid NETWORK_DISCOVERY_BIND_ADDR: {e}"))?
            .unwrap_or_else(default_discovery_bind_address);

        let enr_address = read_env("NETWORK_DISCOVERY_ENR_ADDRESS")
            .ok()
            .map(|addr| addr.parse())
            .transpose()
            .map_err(|e| anyhow::anyhow!("Invalid NETWORK_DISCOVERY_ENR_ADDRESS: {e}"))?;

        let enr_udp_port = read_env("NETWORK_DISCOVERY_ENR_UDP_PORT")
            .ok()
            .map(|port| port.parse())
            .transpose()
            .map_err(|e| anyhow::anyhow!("Invalid NETWORK_DISCOVERY_ENR_UDP_PORT: {e}"))?;

        let enr_tcp_port = read_env("NETWORK_DISCOVERY_ENR_TCP_PORT")
            .ok()
            .map(|port| port.parse())
            .transpose()
            .map_err(|e| anyhow::anyhow!("Invalid NETWORK_DISCOVERY_ENR_TCP_PORT: {e}"))?;

        let private_key_path = read_env("NETWORK_DISCOVERY_KEY_PATH")
            .ok()
            .map(PathBuf::from);

        let bootnodes = read_env("NETWORK_DISCOVERY_BOOTNODES")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .filter_map(|entry| {
                        let trimmed = entry.trim();
                        if trimmed.is_empty() {
                            None
                        } else {
                            Some(trimmed.to_string())
                        }
                    })
                    .collect()
            })
            .unwrap_or_else(default_discovery_bootnodes);

        Ok(Self {
            enabled,
            udp_bind,
            enr_address,
            enr_udp_port,
            enr_tcp_port,
            private_key_path,
            bootnodes,
        })
    }
}
