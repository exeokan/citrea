#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'
umask 077

usage() { cat <<'EOF'
Usage:
  ./run-wan-discovery.sh node1
  ./run-wan-discovery.sh node2 <BOOTNODE_ENR>
  ./run-wan-discovery.sh node3 <BOOTNODE_ENR>

Node1 (sequencer + bootnode):
  - PUBLIC_IP (optional) public IPv4 to advertise; auto-detected if unset.

Node2 (full node):
  - BOOTNODE_ENR argument is required (from node1 output).
  - Provide NODE1_PUBLIC_IP or SEQUENCER_URL for the sequencer RPC.
  - PUBLIC_IP (optional) public IPv4 to advertise; auto-detected if unset.

Node3 (full node):
  - BOOTNODE_ENR argument is required (from node1 output).
  - Provide NODE1_PUBLIC_IP or SEQUENCER_URL for the sequencer RPC.
  - PUBLIC_IP (optional) public IPv4 to advertise; auto-detected if unset.

Optional env overrides:
  CITREA_ROOT, CITREA_BIN
  MOCK_DA_DB_PATH
  NODE1_DATA_DIR, NODE2_DATA_DIR
  NODE1_DISCOVERY_UDP_PORT, NODE2_DISCOVERY_UDP_PORT
  NODE1_P2P_TCP_PORT, NODE2_P2P_TCP_PORT
  NODE1_SEQUENCER_RPC_PORT, NODE2_RPC_PORT
  NODE1_PUBLIC_IP, SEQUENCER_URL
  ALLOW_PRIVATE_IPS=true (skip public IPv4 check for local/private testing)
  NODE3_DATA_DIR, NODE3_DISCOVERY_UDP_PORT, NODE3_P2P_TCP_PORT, NODE3_RPC_PORT
EOF
}

log() { printf '==> %s\n' "$*"; }
warn() { printf 'WARN: %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

find_root() {
  if [[ -n "${CITREA_ROOT:-}" ]]; then
    printf '%s\n' "$CITREA_ROOT"
    return 0
  fi
  local script_dir
  script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  if [[ -f "$script_dir/Cargo.toml" && -d "$script_dir/resources" ]]; then
    printf '%s\n' "$script_dir"
    return 0
  fi
  if [[ -f "$PWD/Cargo.toml" && -d "$PWD/resources" ]]; then
    printf '%s\n' "$PWD"
    return 0
  fi
  die "Could not locate repo root. Set CITREA_ROOT to the repo root."
}

select_citrea_bin() {
  if [[ -n "${CITREA_BIN:-}" ]]; then
    [[ -x "$CITREA_BIN" ]] || die "CITREA_BIN is not executable: $CITREA_BIN"
    printf '%s\n' "$CITREA_BIN"
    return 0
  fi
  if [[ -x "$ROOT_DIR/target/release/citrea" ]]; then
    printf '%s\n' "$ROOT_DIR/target/release/citrea"
    return 0
  fi
  if [[ -x "$ROOT_DIR/target/debug/citrea" ]]; then
    printf '%s\n' "$ROOT_DIR/target/debug/citrea"
    return 0
  fi
  die "citrea binary not found. Build it with: make build"
}

detect_public_ip() {
  if [[ -n "${PUBLIC_IP:-}" ]]; then
    printf '%s\n' "$PUBLIC_IP"
    return 0
  fi
  local ip=""
  if command -v curl >/dev/null 2>&1; then
    ip=$(curl -4 -s --max-time 5 https://api.ipify.org || true)
  elif command -v wget >/dev/null 2>&1; then
    ip=$(wget -qO- https://api.ipify.org || true)
  elif command -v dig >/dev/null 2>&1; then
    ip=$(dig +short myip.opendns.com @resolver1.opendns.com || true)
  fi
  ip=${ip//$'\r'/}
  ip=${ip//$'\n'/}
  if [[ -z "$ip" ]]; then
    die "PUBLIC_IP is not set and auto-detection failed. Set PUBLIC_IP to your public IPv4."
  fi
  printf '%s\n' "$ip"
}

is_public_ipv4() {
  local ip=$1
  [[ "$ip" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] || return 1
  local IFS=.
  local o1 o2 o3 o4
  read -r o1 o2 o3 o4 <<< "$ip"
  for o in $o1 $o2 $o3 $o4; do
    if (( o < 0 || o > 255 )); then
      return 1
    fi
  done
  if (( o1 == 10 || o1 == 127 || o1 == 0 )); then
    return 1
  fi
  if (( o1 == 169 && o2 == 254 )); then
    return 1
  fi
  if (( o1 == 192 && o2 == 168 )); then
    return 1
  fi
  if (( o1 == 172 && o2 >= 16 && o2 <= 31 )); then
    return 1
  fi
  if (( o1 == 100 && o2 >= 64 && o2 <= 127 )); then
    return 1
  fi
  return 0
}

require_public_ipv4() {
  local ip=$1
  if [[ "${ALLOW_PRIVATE_IPS:-}" == "true" ]]; then
    warn "ALLOW_PRIVATE_IPS=true, skipping public IPv4 validation for $ip"
    return 0
  fi
  if ! is_public_ipv4 "$ip"; then
    die "IP is not a public IPv4 address: $ip"
  fi
}

validate_port() {
  local port=$1
  if ! [[ "$port" =~ ^[0-9]+$ ]] || (( port < 1 || port > 65535 )); then
    die "Invalid port: $port"
  fi
}

ensure_unique_ports() {
  local -a ports=("$@")
  local i j
  for ((i=0; i<${#ports[@]}; i++)); do
    for ((j=i+1; j<${#ports[@]}; j++)); do
      if [[ "${ports[i]}" == "${ports[j]}" ]]; then
        die "Port collision detected: ${ports[i]}"
      fi
    done
  done
}

check_port_available() {
  local proto=$1
  local port=$2
  if command -v lsof >/dev/null 2>&1; then
    if [[ "$proto" == "tcp" ]]; then
      if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        die "TCP port $port is already in use."
      fi
    else
      if lsof -nP -iUDP:"$port" >/dev/null 2>&1; then
        die "UDP port $port is already in use."
      fi
    fi
    return 0
  fi
  if command -v ss >/dev/null 2>&1; then
    if [[ "$proto" == "tcp" ]]; then
      if ss -lnt "( sport = :$port )" 2>/dev/null | grep -q ":$port"; then
        die "TCP port $port is already in use."
      fi
    else
      if ss -lnu "( sport = :$port )" 2>/dev/null | grep -q ":$port"; then
        die "UDP port $port is already in use."
      fi
    fi
    return 0
  fi
  warn "Skipping port availability check for $proto/$port (no lsof or ss)."
}

resolve_da_db_path() {
  local path="${MOCK_DA_DB_PATH:-$HOME/.citrea-mock-da}"
  mkdir -p "$path"
  (cd "$path" && pwd)
}

write_node1_config() {
  local config_path=$1
  local data_dir=$2
  local public_ip=$3
  local discovery_udp_port=$4
  local p2p_tcp_port=$5
  local sequencer_rpc_port=$6
  local da_db_path=$7

  cat > "$config_path" <<EOF
[public_keys]
sequencer_public_key = "036360e856310ce5d294e8be33fc807077dc56ac80d95d9cd4ddbd21325eff73f7"
sequencer_da_pub_key = "02588d202afcc1ee4ab5254c7847ec25b9a135bbda0f2bc69ee1a714749fd77dc9"
prover_da_pub_key = ""

[da]
sender_address = "02588d202afcc1ee4ab5254c7847ec25b9a135bbda0f2bc69ee1a714749fd77dc9"
db_path = "$da_db_path"

[storage]
path = "$data_dir/db"
db_max_open_files = 5000

[rpc]
bind_host = "0.0.0.0"
bind_port = $sequencer_rpc_port
max_connections = 10000
enable_subscriptions = true
max_subscriptions_per_connection = 100

[network]

[network.discovery]
enabled = true
udp_bind = "0.0.0.0:$discovery_udp_port"
enr_address = "$public_ip"
enr_tcp_port = $p2p_tcp_port
private_key_path = "$data_dir/discv5.key"
bootnodes = []
target_peers = 32
query_interval_secs = 30
EOF
}

write_node2_config() {
  local config_path=$1
  local data_dir=$2
  local public_ip=$3
  local discovery_udp_port=$4
  local p2p_tcp_port=$5
  local rpc_port=$6
  local sequencer_url=$7
  local bootnode_enr=$8
  local dial_addr=$9
  local da_db_path=${10}

  cat > "$config_path" <<EOF
[public_keys]
sequencer_public_key = "036360e856310ce5d294e8be33fc807077dc56ac80d95d9cd4ddbd21325eff73f7"
sequencer_da_pub_key = "02588d202afcc1ee4ab5254c7847ec25b9a135bbda0f2bc69ee1a714749fd77dc9"
prover_da_pub_key = "03eedab888e45f3bdc3ec9918c491c11e5cf7af0a91f38b97fbc1e135ae4056601"

[da]
sender_address = "02588d202afcc1ee4ab5254c7847ec25b9a135bbda0f2bc69ee1a714749fd77dc9"
db_path = "$da_db_path"

[storage]
path = "$data_dir/db"
db_max_open_files = 5000

[rpc]
bind_host = "127.0.0.1"
bind_port = $rpc_port
enable_subscriptions = true
max_subscriptions_per_connection = 100

[runner]
include_tx_body = false
sequencer_client_url = "$sequencer_url"
scan_l1_start_height = 1

[network]
dial_addr = "$dial_addr"

[network.discovery]
enabled = true
udp_bind = "0.0.0.0:$discovery_udp_port"
enr_address = "$public_ip"
enr_tcp_port = $p2p_tcp_port
private_key_path = "$data_dir/discv5.key"
bootnodes = ["$bootnode_enr"]
target_peers = 32
query_interval_secs = 30
EOF
}

run_node1() {
  local public_ip
  public_ip=$(detect_public_ip)
  require_public_ipv4 "$public_ip"

  local data_dir="${NODE1_DATA_DIR:-$HOME/.citrea-node1}"
  mkdir -p "$data_dir"
  data_dir=$(cd "$data_dir" && pwd)

  # Node1 discovery UDP port (discv5 bind + ENR udp).
  local discovery_udp_port="${NODE1_DISCOVERY_UDP_PORT:-9000}"
  # Node1 libp2p TCP transport port.
  local p2p_tcp_port="${NODE1_P2P_TCP_PORT:-9100}"
  # Node1 sequencer JSON-RPC port (public).
  local sequencer_rpc_port="${NODE1_SEQUENCER_RPC_PORT:-12345}"

  validate_port "$discovery_udp_port"
  validate_port "$p2p_tcp_port"
  validate_port "$sequencer_rpc_port"
  ensure_unique_ports "$discovery_udp_port" "$p2p_tcp_port" "$sequencer_rpc_port"
  check_port_available "udp" "$discovery_udp_port"
  check_port_available "tcp" "$p2p_tcp_port"
  check_port_available "tcp" "$sequencer_rpc_port"

  local config_path="$data_dir/rollup_config.toml"
  local da_db_path="$DA_DB_PATH"
  write_node1_config "$config_path" "$data_dir" "$public_ip" "$discovery_udp_port" "$p2p_tcp_port" "$sequencer_rpc_port" "$da_db_path"

  export NETWORK_DISCOVERY_ENABLED=true
  export NETWORK_DISCOVERY_BIND_ADDR="0.0.0.0:$discovery_udp_port"
  export NETWORK_DISCOVERY_ENR_ADDRESS="$public_ip"
  export NETWORK_DISCOVERY_ENR_UDP_PORT="$discovery_udp_port"
  export NETWORK_DISCOVERY_ENR_TCP_PORT="$p2p_tcp_port"
  export NETWORK_DISCOVERY_KEY_PATH="$data_dir/discv5.key"
  export NETWORK_DISCOVERY_BOOTNODES=""
  export NETWORK_DISCOVERY_TARGET_PEERS=32
  export NETWORK_DISCOVERY_QUERY_INTERVAL_SECS=30

  log "Node1 public IPv4: $public_ip"
  log "Sequencer RPC URL: http://$public_ip:$sequencer_rpc_port"
  log "Discovery UDP port (discv5 ENR): $discovery_udp_port"
  log "P2P TCP port: $p2p_tcp_port"
  log "Mock DA db path: $da_db_path"
  log "Rollup config: $config_path"
  log "Open/forward on firewall/NAT: UDP $discovery_udp_port, TCP $p2p_tcp_port, TCP $sequencer_rpc_port"
  log "Validation: the Local discv5 ENR line should include $public_ip and udp:$discovery_udp_port"
  log "Copy the Local discv5 ENR from the logs to start node2."

  RUST_LOG=${RUST_LOG:-info} "$CITREA_BIN" --dev --da-layer mock \
    --rollup-config-path "$config_path" \
    --sequencer "$SEQUENCER_CONFIG" \
    --genesis-paths "$GENESIS_DIR"
}

run_node2() {
  local bootnode_enr=$1
  [[ -n "$bootnode_enr" ]] || die "Bootnode ENR required: ./run-wan-discovery.sh node2 <BOOTNODE_ENR>"
  if [[ "$bootnode_enr" != enr:* ]]; then
    die "Bootnode ENR must start with 'enr:'"
  fi

  local public_ip
  public_ip=$(detect_public_ip)
  require_public_ipv4 "$public_ip"

  local node1_sequencer_port="${NODE1_SEQUENCER_RPC_PORT:-12345}"
  validate_port "$node1_sequencer_port"

  local sequencer_url=""
  if [[ -n "${SEQUENCER_URL:-}" ]]; then
    sequencer_url="$SEQUENCER_URL"
  elif [[ -n "${NODE1_PUBLIC_IP:-}" ]]; then
    require_public_ipv4 "$NODE1_PUBLIC_IP"
    sequencer_url="http://${NODE1_PUBLIC_IP}:${node1_sequencer_port}"
  else
    die "Set NODE1_PUBLIC_IP or SEQUENCER_URL for the sequencer RPC."
  fi

  local data_dir="${NODE2_DATA_DIR:-$HOME/.citrea-node2}"
  mkdir -p "$data_dir"
  data_dir=$(cd "$data_dir" && pwd)

  # Node2 discovery UDP port (discv5 bind + ENR udp).
  local discovery_udp_port="${NODE2_DISCOVERY_UDP_PORT:-9001}"
  # Node2 libp2p TCP transport port.
  local p2p_tcp_port="${NODE2_P2P_TCP_PORT:-9101}"
  # Node2 JSON-RPC port (local only by default).
  local rpc_port="${NODE2_RPC_PORT:-12346}"

  validate_port "$discovery_udp_port"
  validate_port "$p2p_tcp_port"
  validate_port "$rpc_port"
  ensure_unique_ports "$discovery_udp_port" "$p2p_tcp_port" "$rpc_port"
  check_port_available "udp" "$discovery_udp_port"
  check_port_available "tcp" "$p2p_tcp_port"
  check_port_available "tcp" "$rpc_port"

  local config_path="$data_dir/rollup_config.toml"
  local dial_addr="${NETWORK_DIAL_ADDR:-/ip4/127.0.0.1/tcp/9100}"
  local da_db_path="$DA_DB_PATH"
  write_node2_config "$config_path" "$data_dir" "$public_ip" "$discovery_udp_port" "$p2p_tcp_port" "$rpc_port" "$sequencer_url" "$bootnode_enr" "$dial_addr" "$da_db_path"

  export NETWORK_DISCOVERY_ENABLED=true
  export NETWORK_DISCOVERY_BIND_ADDR="0.0.0.0:$discovery_udp_port"
  export NETWORK_DISCOVERY_ENR_ADDRESS="$public_ip"
  export NETWORK_DISCOVERY_ENR_UDP_PORT="$discovery_udp_port"
  export NETWORK_DISCOVERY_ENR_TCP_PORT="$p2p_tcp_port"
  export NETWORK_DISCOVERY_KEY_PATH="$data_dir/discv5.key"
  export NETWORK_DISCOVERY_BOOTNODES="$bootnode_enr"
  export NETWORK_DISCOVERY_TARGET_PEERS=32
  export NETWORK_DISCOVERY_QUERY_INTERVAL_SECS=30
  export NETWORK_DIAL_ADDR="$dial_addr"

  log "Node2 public IPv4: $public_ip"
  log "Bootnode ENR: $bootnode_enr"
  log "Sequencer RPC URL: $sequencer_url"
  log "Discovery UDP port (discv5 ENR): $discovery_udp_port"
  log "P2P TCP port: $p2p_tcp_port"
  log "Mock DA db path: $da_db_path"
  log "Rollup config: $config_path"
  log "Direct dial target: ${NETWORK_DIAL_ADDR:-/ip4/127.0.0.1/tcp/9100}"
  log "Open/forward on firewall/NAT: UDP $discovery_udp_port, TCP $p2p_tcp_port"
  log "Validation: look for 'discv5 discovered peer' and 'Connected to peer' in the logs."

  RUST_LOG=${RUST_LOG:-info} "$CITREA_BIN" --dev --da-layer mock \
    --rollup-config-path "$config_path" \
    --genesis-paths "$GENESIS_DIR"
}

run_node3() {
  local bootnode_enr=$1
  [[ -n "$bootnode_enr" ]] || die "Bootnode ENR required: ./run-wan-discovery.sh node3 <BOOTNODE_ENR>"
  if [[ "$bootnode_enr" != enr:* ]]; then
    die "Bootnode ENR must start with 'enr:'"
  fi

  local public_ip
  public_ip=$(detect_public_ip)
  require_public_ipv4 "$public_ip"

  local node1_sequencer_port="${NODE1_SEQUENCER_RPC_PORT:-12345}"
  validate_port "$node1_sequencer_port"

  local sequencer_url=""
  if [[ -n "${SEQUENCER_URL:-}" ]]; then
    sequencer_url="$SEQUENCER_URL"
  elif [[ -n "${NODE1_PUBLIC_IP:-}" ]]; then
    require_public_ipv4 "$NODE1_PUBLIC_IP"
    sequencer_url="http://${NODE1_PUBLIC_IP}:${node1_sequencer_port}"
  else
    die "Set NODE1_PUBLIC_IP or SEQUENCER_URL for the sequencer RPC."
  fi

  local data_dir="${NODE3_DATA_DIR:-$HOME/.citrea-node3}"
  mkdir -p "$data_dir"
  data_dir=$(cd "$data_dir" && pwd)

  # Node3 discovery UDP port (discv5 bind + ENR udp).
  local discovery_udp_port="${NODE3_DISCOVERY_UDP_PORT:-9002}"
  # Node3 libp2p TCP transport port.
  local p2p_tcp_port="${NODE3_P2P_TCP_PORT:-9102}"
  # Node3 JSON-RPC port (local only by default).
  local rpc_port="${NODE3_RPC_PORT:-12347}"

  validate_port "$discovery_udp_port"
  validate_port "$p2p_tcp_port"
  validate_port "$rpc_port"
  ensure_unique_ports "$discovery_udp_port" "$p2p_tcp_port" "$rpc_port"
  check_port_available "udp" "$discovery_udp_port"
  check_port_available "tcp" "$p2p_tcp_port"
  check_port_available "tcp" "$rpc_port"

  local config_path="$data_dir/rollup_config.toml"
  local dial_addr="${NETWORK_DIAL_ADDR:-/ip4/127.0.0.1/tcp/9100}"
  local da_db_path="$DA_DB_PATH"
  write_node2_config "$config_path" "$data_dir" "$public_ip" "$discovery_udp_port" "$p2p_tcp_port" "$rpc_port" "$sequencer_url" "$bootnode_enr" "$dial_addr" "$da_db_path"

  export NETWORK_DISCOVERY_ENABLED=true
  export NETWORK_DISCOVERY_BIND_ADDR="0.0.0.0:$discovery_udp_port"
  export NETWORK_DISCOVERY_ENR_ADDRESS="$public_ip"
  export NETWORK_DISCOVERY_ENR_UDP_PORT="$discovery_udp_port"
  export NETWORK_DISCOVERY_ENR_TCP_PORT="$p2p_tcp_port"
  export NETWORK_DISCOVERY_KEY_PATH="$data_dir/discv5.key"
  export NETWORK_DISCOVERY_BOOTNODES="$bootnode_enr"
  export NETWORK_DISCOVERY_TARGET_PEERS=32
  export NETWORK_DISCOVERY_QUERY_INTERVAL_SECS=30
  export NETWORK_DIAL_ADDR="$dial_addr"

  log "Node3 public IPv4: $public_ip"
  log "Bootnode ENR: $bootnode_enr"
  log "Sequencer RPC URL: $sequencer_url"
  log "Discovery UDP port (discv5 ENR): $discovery_udp_port"
  log "P2P TCP port: $p2p_tcp_port"
  log "Mock DA db path: $da_db_path"
  log "Rollup config: $config_path"
  log "Direct dial target: ${NETWORK_DIAL_ADDR:-/ip4/127.0.0.1/tcp/9100}"
  log "Open/forward on firewall/NAT: UDP $discovery_udp_port, TCP $p2p_tcp_port"
  log "Validation: look for 'discv5 discovered peer' and 'Connected to peer' in the logs."

  RUST_LOG=${RUST_LOG:-info} "$CITREA_BIN" --dev --da-layer mock \
    --rollup-config-path "$config_path" \
    --genesis-paths "$GENESIS_DIR"
}

main() {
  local node_type=${1:-}
  [[ -n "$node_type" ]] || { usage; exit 1; }
  shift || true

  ROOT_DIR=$(find_root)
  GENESIS_DIR="$ROOT_DIR/resources/genesis/mock"
  SEQUENCER_CONFIG="$ROOT_DIR/resources/configs/mock/sequencer_config.toml"
  [[ -d "$GENESIS_DIR" ]] || die "Missing genesis dir: $GENESIS_DIR"
  [[ -f "$SEQUENCER_CONFIG" ]] || die "Missing sequencer config: $SEQUENCER_CONFIG"

  CITREA_BIN=$(select_citrea_bin)
  DA_DB_PATH=$(resolve_da_db_path)

  case "$node_type" in
    node1)
      run_node1
      ;;
    node2)
      run_node2 "${1:-}"
      ;;
    node3)
      run_node3 "${1:-}"
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"
