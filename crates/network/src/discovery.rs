use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;

use anyhow::{Context, Result};
use citrea_common::DiscoveryConfig;
use discv5::{
    enr::{self, CombinedKey, CombinedPublicKey, Enr, EnrPublicKey},
    ConfigBuilder, Discv5, Event, ListenConfig,
};
use libp2p::{
    identity,
    multiaddr::{Multiaddr, Protocol},
    PeerId,
};
use rand::{rngs::OsRng, RngCore};
use tokio::sync::mpsc;
use tracing::{info, warn};

const HEX_PREFIX: &str = "0x";

pub(crate) struct DiscoveryComponents {
    pub service: DiscoveryService,
    pub event_rx: mpsc::Receiver<Event>,
    #[allow(dead_code)]
    pub local_enr: Enr<CombinedKey>,
}

pub(crate) struct DiscoveryService {
    discv5: Discv5,
}

impl DiscoveryService {
    pub fn new(discv5: Discv5) -> Self {
        Self { discv5 }
    }

    pub async fn random_lookup(&self) -> Result<Vec<Enr<CombinedKey>>> {
        let target = enr::NodeId::random();
        self.discv5
            .find_node(target)
            .await
            .map_err(|e| anyhow::anyhow!("discv5 query failed: {e:?}"))
    }

    pub fn add_bootnodes(&self, bootnodes: &[String]) -> Result<()> {
        for record in bootnodes {
            let enr: Enr<CombinedKey> = record
                .parse()
                .map_err(|e| anyhow::anyhow!("Invalid ENR {record}: {e}"))?;
            let addresses = enr_multiaddrs(&enr);
            if addresses.is_empty() {
                info!(
                    "Added discv5 bootnode {:?} without advertised multiaddrs",
                    enr.node_id()
                );
            } else {
                let addrs = addresses
                    .iter()
                    .map(|addr| addr.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                info!("Added discv5 bootnode {:?} at {addrs}", enr.node_id());
            }
            self.discv5
                .add_enr(enr)
                .map_err(|e| anyhow::anyhow!("Failed to add ENR {record}: {e}"))?;
        }
        Ok(())
    }

    pub fn update_socket_from_multiaddr(&self, addr: &Multiaddr) {
        if let Some((socket, is_tcp)) = multiaddr_to_socket(addr) {
            self.update_socket(socket, is_tcp);
        }
    }

    pub fn update_socket(&self, socket: SocketAddr, is_tcp: bool) {
        if self.discv5.update_local_enr_socket(socket, is_tcp) {
            info!("Updated discv5 ENR socket to {socket}");
            info!("Local discv5 ENR: {}", self.discv5.local_enr().to_base64());
        }
    }
}

pub(crate) fn prepare_identity(
    config: &DiscoveryConfig,
) -> Result<(identity::Keypair, Option<CombinedKey>)> {
    if !config.enabled {
        return Ok((identity::Keypair::generate_ed25519(), None));
    }
    let secret = load_or_generate_secret(config.private_key_path.as_deref())?;
    let enr_key = build_enr_key(&secret)?;
    let libp2p_key = build_libp2p_keypair(&secret)?;
    Ok((libp2p_key, Some(enr_key)))
}

pub(crate) async fn start_service(
    config: &DiscoveryConfig,
    enr_key: CombinedKey,
) -> Result<DiscoveryComponents> {
    let local_enr = build_local_enr(config, &enr_key)?;
    if config.enr_address.is_none() {
        warn!("discv5 is enabled but NETWORK_DISCOVERY_ENR_ADDRESS is not set");
    }
    info!("Local discv5 ENR: {}", local_enr.to_base64());
    let listen_config = listen_config(config.udp_bind);
    let discv5_config = ConfigBuilder::new(listen_config).build();
    let mut discv5 = Discv5::new(local_enr.clone(), enr_key, discv5_config)
        .map_err(|e| anyhow::anyhow!("Failed to create discv5 service: {e}"))?;
    discv5
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start discv5 service: {e}"))?;

    discv5
        .add_enr(
            discv5.local_enr().clone(), // ensure routing table is aware of local node
        )
        .ok();
    let event_rx = discv5
        .event_stream()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to subscribe to discv5 events: {e}"))?;
    let service = DiscoveryService::new(discv5);
    service.add_bootnodes(&config.bootnodes)?;
    Ok(DiscoveryComponents {
        service,
        event_rx,
        local_enr,
    })
}

fn listen_config(bind: SocketAddr) -> ListenConfig {
    ListenConfig::from_ip(bind.ip(), bind.port())
}

fn build_local_enr(config: &DiscoveryConfig, key: &CombinedKey) -> Result<Enr<CombinedKey>> {
    let mut builder = enr::Enr::builder();
    if let Some(advertised) = config.enr_address {
        match advertised {
            IpAddr::V4(addr) => {
                builder.ip4(addr);
                if let Some(port) = config.enr_udp_port.or(Some(config.udp_bind.port())) {
                    builder.udp4(port);
                }
                if let Some(tcp) = config.enr_tcp_port {
                    builder.tcp4(tcp);
                }
            }
            IpAddr::V6(addr) => {
                builder.ip6(addr);
                if let Some(port) = config.enr_udp_port.or(Some(config.udp_bind.port())) {
                    builder.udp6(port);
                }
                if let Some(tcp) = config.enr_tcp_port {
                    builder.tcp6(tcp);
                }
            }
        }
    }
    builder
        .build(key)
        .map_err(|e| anyhow::anyhow!("Unable to build local ENR: {e}"))
}

fn build_libp2p_keypair(secret: &[u8; 32]) -> Result<identity::Keypair> {
    let mut buf = secret.to_vec();
    let secret_key = identity::secp256k1::SecretKey::try_from_bytes(&mut buf)
        .map_err(|e| anyhow::anyhow!("Invalid secp256k1 private key: {e}"))?;
    let kp = identity::secp256k1::Keypair::from(secret_key);
    Ok(identity::Keypair::from(kp))
}

fn build_enr_key(secret: &[u8; 32]) -> Result<CombinedKey> {
    let mut buf = secret.to_vec();
    CombinedKey::secp256k1_from_bytes(&mut buf)
        .map_err(|e| anyhow::anyhow!("Invalid secp256k1 key for ENR: {e}"))
}

fn load_or_generate_secret(path: Option<&Path>) -> Result<[u8; 32]> {
    if let Some(path) = path {
        if path.exists() {
            let contents = fs::read_to_string(path)
                .with_context(|| format!("Failed to read discovery key from {}", path.display()))?;
            return decode_secret(&contents);
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "Failed to create discovery key directory {}",
                    parent.display()
                )
            })?;
        }
        let secret = random_secret();
        fs::write(path, hex::encode(secret))
            .with_context(|| format!("Failed to persist discovery key at {}", path.display()))?;
        return Ok(secret);
    }
    Ok(random_secret())
}

fn random_secret() -> [u8; 32] {
    let mut secret = [0u8; 32];
    OsRng.fill_bytes(&mut secret);
    secret
}

fn decode_secret(contents: &str) -> Result<[u8; 32]> {
    let trimmed = contents.trim();
    let without_prefix = trimmed
        .strip_prefix(HEX_PREFIX)
        .unwrap_or(trimmed)
        .replace(char::is_whitespace, "");
    let bytes = hex::decode(&without_prefix)
        .map_err(|e| anyhow::anyhow!("Invalid discovery key hex: {e}"))?;
    if bytes.len() != 32 {
        return Err(anyhow::anyhow!(
            "Discovery key must be 32 bytes, got {}",
            bytes.len()
        ));
    }
    let mut secret = [0u8; 32];
    secret.copy_from_slice(&bytes);
    Ok(secret)
}

pub(crate) fn enr_peer_id(enr: &Enr<CombinedKey>) -> Option<PeerId> {
    match enr.public_key() {
        CombinedPublicKey::Secp256k1(public_key) => {
            let bytes = public_key.encode();
            let libp2p_pk = identity::secp256k1::PublicKey::try_from_bytes(&bytes).ok()?;
            let public = identity::PublicKey::from(libp2p_pk);
            Some(PeerId::from_public_key(&public))
        }
        CombinedPublicKey::Ed25519(_) => None,
    }
}

pub(crate) fn enr_multiaddrs(enr: &Enr<CombinedKey>) -> Vec<Multiaddr> {
    let mut addrs = Vec::new();
    if let Some(ip) = enr.ip4() {
        if let Some(tcp) = enr.tcp4() {
            addrs.push(build_multiaddr(IpAddr::V4(ip), Protocol::Tcp(tcp)));
        }
        if let Some(udp) = enr.udp4() {
            let mut addr = build_multiaddr(IpAddr::V4(ip), Protocol::Udp(udp));
            addr.push(Protocol::QuicV1);
            addrs.push(addr);
        }
    }
    if let Some(ip) = enr.ip6() {
        if let Some(tcp) = enr.tcp6() {
            addrs.push(build_multiaddr(IpAddr::V6(ip), Protocol::Tcp(tcp)));
        }
        if let Some(udp) = enr.udp6() {
            let mut addr = build_multiaddr(IpAddr::V6(ip), Protocol::Udp(udp));
            addr.push(Protocol::QuicV1);
            addrs.push(addr);
        }
    }
    addrs
}

fn build_multiaddr(ip: IpAddr, transport: Protocol<'_>) -> Multiaddr {
    let mut addr = Multiaddr::empty();
    match ip {
        IpAddr::V4(ipv4) => addr.push(Protocol::Ip4(ipv4)),
        IpAddr::V6(ipv6) => addr.push(Protocol::Ip6(ipv6)),
    }
    addr.push(transport);
    addr
}

pub(crate) fn multiaddr_to_socket(addr: &Multiaddr) -> Option<(SocketAddr, bool)> {
    let mut protocols = addr.iter();
    let ip = match protocols.next()? {
        Protocol::Ip4(ipv4) => IpAddr::V4(ipv4),
        Protocol::Ip6(ipv6) => IpAddr::V6(ipv6),
        _ => return None,
    };
    for protocol in protocols {
        match protocol {
            Protocol::Tcp(port) => return Some((SocketAddr::new(ip, port), true)),
            Protocol::Udp(port) => return Some((SocketAddr::new(ip, port), false)),
            _ => continue,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket},
        path::Path,
        time::Duration,
    };
    use tokio::time::timeout;

    #[test]
    fn parse_multiaddr_socket() {
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/9000".parse().unwrap();
        let socket = multiaddr_to_socket(&addr).unwrap();
        assert_eq!(socket, ("127.0.0.1:9000".parse().unwrap(), true));

        let quic: Multiaddr = "/ip6/::1/udp/9182/quic-v1".parse().unwrap();
        let socket = multiaddr_to_socket(&quic).unwrap();
        assert_eq!(socket, ("[::1]:9182".parse().unwrap(), false));
    }

    #[test]
    fn enr_multiaddr_generation() {
        let key = CombinedKey::generate_secp256k1();
        let mut builder = enr::Enr::builder();
        builder
            .ip4(Ipv4Addr::LOCALHOST)
            .udp4(9101)
            .tcp4(9201)
            .ip6(Ipv6Addr::LOCALHOST)
            .udp6(9102)
            .tcp6(9202);
        let enr = builder.build(&key).unwrap();
        let addrs = enr_multiaddrs(&enr);
        assert!(addrs
            .iter()
            .any(|a| a.to_string() == "/ip4/127.0.0.1/tcp/9201"));
        assert!(addrs
            .iter()
            .any(|a| a.to_string() == "/ip4/127.0.0.1/udp/9101/quic-v1"));
        assert!(addrs.iter().any(|a| a.to_string() == "/ip6/::1/tcp/9202"));
    }

    #[test]
    fn discovery_secret_roundtrip() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let key_path = tmp_dir.path().join("disc.key");
        let first = load_or_generate_secret(Some(&key_path)).unwrap();
        let second = load_or_generate_secret(Some(&key_path)).unwrap();
        assert_eq!(first, second);
    }

    fn random_local_socket() -> SocketAddr {
        let socket = UdpSocket::bind(("127.0.0.1", 0)).expect("bind ephemeral udp");
        socket.local_addr().unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn start_service_runs_discv5() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut config = DiscoveryConfig::default();
        config.enabled = true;
        config.udp_bind = random_local_socket();
        config.enr_address = Some("127.0.0.1".parse().unwrap());
        config.enr_udp_port = Some(config.udp_bind.port());
        config.enr_tcp_port = Some(config.udp_bind.port());
        config.private_key_path = Some(tmp_dir.path().join("disc.key"));
        config.bootnodes = Vec::new(); // avoid hitting default public bootnodes in tests

        let (_libp2p_kp, enr_key) = prepare_identity(&config).expect("identity generation");
        let enr_key = enr_key.expect("discovery key");

        let DiscoveryComponents { service, .. } = start_service(&config, enr_key)
            .await
            .expect("discv5 service should start");

        // Calling random_lookup drives the underlying discv5 query API.
        timeout(std::time::Duration::from_secs(5), service.random_lookup())
            .await
            .expect("lookup should complete promptly")
            .expect("discv5 query should not error");
    }

    fn mk_test_config(base_path: &Path) -> DiscoveryConfig {
        let mut config = DiscoveryConfig::default();
        config.enabled = true;
        config.udp_bind = random_local_socket();
        config.enr_address = Some("127.0.0.1".parse().unwrap());
        config.enr_udp_port = Some(config.udp_bind.port());
        config.enr_tcp_port = Some(config.udp_bind.port());
        config.private_key_path = Some(base_path.join("disc.key"));
        config.bootnodes = Vec::new(); // keep tests fully local
        config
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]

    async fn discv5_discovers_bootnode_peer() {
        let tmp_a = tempfile::tempdir().unwrap();
        let tmp_b = tempfile::tempdir().unwrap();

        let mut config_a = mk_test_config(tmp_a.path());
        config_a.bootnodes = Vec::new();
        let (_libp2p_a, enr_key_a) = prepare_identity(&config_a).expect("identity generation");
        let enr_key_a = enr_key_a.expect("discovery key");
        let DiscoveryComponents {
            service: _service_a,
            event_rx: mut events_a,
            local_enr: enr_a,
        } = start_service(&config_a, enr_key_a)
            .await
            .expect("service A starts");

        let mut config_b = mk_test_config(tmp_b.path());
        config_b.bootnodes = vec![enr_a.to_base64()];
        let (_libp2p_b, enr_key_b) = prepare_identity(&config_b).expect("identity generation");
        let enr_key_b = enr_key_b.expect("discovery key");
        let DiscoveryComponents {
            service: service_b,
            local_enr: enr_b,
            ..
        } = start_service(&config_b, enr_key_b)
            .await
            .expect("service B starts");

        service_b
            .random_lookup()
            .await
            .expect("lookup request succeeds");
        let target_node_id = enr_b.node_id();

        let discovered = timeout(Duration::from_secs(10), async {
            while let Some(event) = events_a.recv().await {
                match event {
                    Event::Discovered(enr) => {
                        println!("discv5 discovered peer {}", enr.node_id());
                        return Some(enr.node_id());
                    }
                    Event::NodeInserted { node_id, .. } if node_id == target_node_id => {
                        println!("discv5 inserted peer {}", node_id);
                        return Some(node_id);
                    }
                    _ => continue,
                }
            }
            None
        })
        .await
        .expect("event stream to produce output");

        assert_eq!(discovered, Some(target_node_id));
    }
}
