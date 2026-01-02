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
ROLLUP_CONFIG_TEMPLATE=${ROLLUP_CONFIG_TEMPLATE:-$(pwd)/resources/configs/network/rollup_config.toml}
if [ ! -f "$ROLLUP_CONFIG_TEMPLATE" ]; then
    echo "Missing rollup config template: $ROLLUP_CONFIG_TEMPLATE"
    exit 1
fi

write_rollup_config_from_template() {
    local config_path=$1
    local template_path=$2
    local da_db_path=$3
    local storage_path=$4
    local rpc_host=$5
    local rpc_port=$6
    local sequencer_url=$7
    local discovery_udp_port=$8
    local discovery_enr_address=$9
    local discovery_tcp_port=${10}
    local discovery_key_path=${11}
    local bootnode_enr=${12:-}

    cp "$template_path" "$config_path"

    perl -0pi -e "s|(?m)^db_path = \".*\"|db_path = \"$da_db_path\"|" "$config_path"
    perl -0pi -e "s|(?m)^path = \".*\"|path = \"$storage_path\"|" "$config_path"
    perl -0pi -e "s|(?m)^bind_host = \".*\"|bind_host = \"$rpc_host\"|" "$config_path"
    perl -0pi -e "s|(?m)^bind_port = .*|bind_port = $rpc_port|" "$config_path"
    perl -0pi -e "s|(?m)^sequencer_client_url = \".*\"|sequencer_client_url = \"$sequencer_url\"|" "$config_path"

    local bootnodes_line="bootnodes = []"
    if [ -n "$bootnode_enr" ]; then
        bootnodes_line="bootnodes = [\"$bootnode_enr\"]"
    fi

    {
        echo ""
        echo "[network]"
        echo "target_peers = 2"
        echo ""
        echo "[network.discovery]"
        echo "enabled = true"
        echo "udp_bind = \"$discovery_enr_address:$discovery_udp_port\""
        echo "enr_address = \"$discovery_enr_address\""
        echo "enr_tcp_port = $discovery_tcp_port"
        echo "private_key_path = \"$discovery_key_path\""
        echo "$bootnodes_line"
    } >> "$config_path"
}

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
        
        write_rollup_config_from_template \
            /tmp/citrea-test-node1/rollup_config.toml \
            "$ROLLUP_CONFIG_TEMPLATE" \
            "$DA_DB_PATH" \
            "/tmp/citrea-test-node1/db" \
            "127.0.0.1" \
            12346 \
            "http://0.0.0.0:12345" \
            9000 \
            "127.0.0.1" \
            9000 \
            "/tmp/citrea-test-node1/disc.key"

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
        
        write_rollup_config_from_template \
            /tmp/citrea-test-node2/rollup_config.toml \
            "$ROLLUP_CONFIG_TEMPLATE" \
            "$DA_DB_PATH" \
            "/tmp/citrea-test-node2/db" \
            "127.0.0.1" \
            12347 \
            "http://0.0.0.0:12345" \
            9001 \
            "127.0.0.1" \
            9001 \
            "/tmp/citrea-test-node2/disc.key" \
            "$BOOTNODE_ENR"

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
