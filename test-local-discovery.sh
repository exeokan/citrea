#!/bin/bash
# Script to test local discv5 discovery with two nodes
# 
# Usage:
#   1. In Terminal 1: ./test-local-discovery.sh node1
#   2. Copy the ENR from Terminal 1's output (look for "🔍 Local discv5 ENR")
#   3. In Terminal 2: ./test-local-discovery.sh node2 <PASTE_ENR_HERE>

set -e

NODE_TYPE=${1:-node1}
BOOTNODE_ENR=${2:-""}

MOCK_DA_DB_PATH=${MOCK_DA_DB_PATH:-$HOME/.citrea-mock-da}
mkdir -p "$MOCK_DA_DB_PATH"
DA_DB_PATH=$(cd "$MOCK_DA_DB_PATH" && pwd)

# Clean up function
cleanup() {
    echo "Cleaning up..."
    rm -rf /tmp/citrea-test-$NODE_TYPE
}
trap cleanup EXIT

case $NODE_TYPE in
    node1)
        echo "========================================="
        echo "Starting Node 1 (will act as bootnode)"
        echo "========================================="
        echo ""
        echo "⏳ Wait for the ENR to appear, then copy it to start node2"
        echo ""
        
        mkdir -p /tmp/citrea-test-node1
        echo "Using shared mock DA db path: $DA_DB_PATH"
        
        # Create custom config for node1 with shared mock DA DB
        cat > /tmp/citrea-test-node1/rollup_config.toml << EOF
[public_keys]
sequencer_public_key = "036360e856310ce5d294e8be33fc807077dc56ac80d95d9cd4ddbd21325eff73f7"
sequencer_da_pub_key = "02588d202afcc1ee4ab5254c7847ec25b9a135bbda0f2bc69ee1a714749fd77dc9"
prover_da_pub_key = "03eedab888e45f3bdc3ec9918c491c11e5cf7af0a91f38b97fbc1e135ae4056601"

[da]
sender_address = "02588d202afcc1ee4ab5254c7847ec25b9a135bbda0f2bc69ee1a714749fd77dc9"
db_path = "$DA_DB_PATH"

[storage]
path = "/tmp/citrea-test-node1/db"
db_max_open_files = 5000

[rpc]
bind_host = "127.0.0.1"
bind_port = 12346
enable_subscriptions = true
max_subscriptions_per_connection = 100

[runner]
include_tx_body = false
sequencer_client_url = "http://0.0.0.0:12345"
scan_l1_start_height = 1

[network]

[network.discovery]
enabled = true
udp_bind = "127.0.0.1:9000"
enr_address = "127.0.0.1"
enr_tcp_port = 9000
private_key_path = "/tmp/citrea-test-node1/disc.key"
bootnodes = []
target_peers = 2
query_interval_secs = 5
EOF

        ./target/debug/citrea --dev --da-layer mock \
            --rollup-config-path /tmp/citrea-test-node1/rollup_config.toml \
            --genesis-paths resources/genesis/mock/
        ;;
        
    node2)
        if [ -z "$BOOTNODE_ENR" ]; then
            echo "❌ Error: Please provide the ENR from node1 as the second argument"
            echo ""
            echo "Usage: $0 node2 <ENR_FROM_NODE1>"
            exit 1
        fi
        
        echo "========================================="
        echo "Starting Node 2 (discovering via node1)"
        echo "========================================="
        echo "Using bootnode ENR: $BOOTNODE_ENR"
        echo ""
        
        mkdir -p /tmp/citrea-test-node2
        echo "Using shared mock DA db path: $DA_DB_PATH"
        
        # Create custom config for node2 with shared mock DA DB and RPC port override
        cat > /tmp/citrea-test-node2/rollup_config.toml << EOF
[public_keys]
sequencer_public_key = "036360e856310ce5d294e8be33fc807077dc56ac80d95d9cd4ddbd21325eff73f7"
sequencer_da_pub_key = "02588d202afcc1ee4ab5254c7847ec25b9a135bbda0f2bc69ee1a714749fd77dc9"
prover_da_pub_key = "03eedab888e45f3bdc3ec9918c491c11e5cf7af0a91f38b97fbc1e135ae4056601"

[da]
sender_address = "02588d202afcc1ee4ab5254c7847ec25b9a135bbda0f2bc69ee1a714749fd77dc9"
db_path = "$DA_DB_PATH"

[storage]
path = "/tmp/citrea-test-node2/db"
db_max_open_files = 5000

[rpc]
bind_host = "127.0.0.1"
bind_port = 12347
enable_subscriptions = true
max_subscriptions_per_connection = 100

[runner]
include_tx_body = false
sequencer_client_url = "http://0.0.0.0:12345"
scan_l1_start_height = 1

[network]

[network.discovery]
enabled = true
udp_bind = "127.0.0.1:9001"
enr_address = "127.0.0.1"
enr_tcp_port = 9001
private_key_path = "/tmp/citrea-test-node2/disc.key"
bootnodes = ["$BOOTNODE_ENR"]
target_peers = 2
query_interval_secs = 5
EOF

        ./target/debug/citrea --dev --da-layer mock \
            --rollup-config-path /tmp/citrea-test-node2/rollup_config.toml \
            --genesis-paths resources/genesis/mock/
        ;;
        
    *)
        echo "Usage: $0 {node1|node2} [bootnode_enr]"
        echo ""
        echo "Example:"
        echo "  Terminal 1: $0 node1"
        echo "  Terminal 2: $0 node2 enr:-..."
        exit 1
        ;;
esac
