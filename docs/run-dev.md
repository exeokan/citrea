# Running Citrea

This document covers how to run Citrea sequencer and a full node locally using a mock DA layer and Bitcoin Regtest.

## Prerequisites

Follow the instructions in [this document.](./dev-setup.md)

## Building and running

Build citrea:

```sh
make build
```

### Run on Mock DA

Run on a local da layer, sharable between nodes that run on your computer.

Run sequencer on Mock DA:

```sh
./target/debug/citrea --dev --da-layer mock --rollup-config-path resources/configs/mock/sequencer_rollup_config.toml --sequencer resources/configs/mock/sequencer_config.toml --genesis-paths resources/genesis/mock/
```

Sequencer RPC is accessible at `127.0.0.1:12345`

_Optional_: Run full node on Mock DA:

```sh
./target/debug/citrea --dev --da-layer mock --rollup-config-path resources/configs/mock/rollup_config.toml --genesis-paths resources/genesis/mock/
```

Full node RPC is accessible at `127.0.0.1:12346`

If test_mode is set to false in the sequencer config, the sequencer will publish blocks every 2 seconds.

_Optional_: Run batch prover on Mock DA:
```sh
PARALLEL_PROOF_LIMIT=1 ./target/debug/citrea --dev --da-layer mock --rollup-config-path resources/configs/mock/batch_prover_rollup_config.toml --genesis-paths resources/genesis/mock/ --batch-prover resources/configs/mock/batch_prover_config.toml
```

_Optional_: Run light client prover on Mock DA:
```sh
./target/debug/citrea --dev --da-layer mock --rollup-config-path resources/configs/mock/light_client_prover_rollup_config.toml --genesis-paths resources/genesis/mock/ --light-client-prover resources/configs/mock/light_client_prover_config.toml
```

### Run on Bitcoin Regtest

Run on local Bitcoin network.

Run Bitcoin Regtest:

```sh
bitcoind -regtest -txindex=1 -addresstype=bech32m -fallbackfee=0.0001
```

Or using docker:

```sh
docker compose -f docker/docker-compose.regtest.yml up
```

Keep this terminal open.

Create bitcoin wallet for Bitcoin DA adapter.

```sh
bitcoin-cli -regtest createwallet citreatesting
bitcoin-cli -regtest loadwallet citreatesting
```

Mine blocks so that the wallet has BTC:

```sh
bitcoin-cli -regtest -generate 201
```

Edit `resources/configs/bitcoin-regtest/sequencer_config.toml` to adjust the sequencer settings.

Edit `resources/configs/bitcoin-regtest/sequencer_rollup_config.toml` file and put in your rpc url, username and password:

```toml
[da]
# fill here
node_url = ""
# fill here
node_username = ""
# fill here
node_password = ""
```

Run sequencer:

```sh
./target/debug/citrea --dev --da-layer bitcoin --rollup-config-path resources/configs/bitcoin-regtest/sequencer_rollup_config.toml --sequencer resources/configs/bitcoin-regtest/sequencer_config.toml --genesis-paths resources/genesis/bitcoin-regtest/
```

Sequencer RPC is accessible at `127.0.0.1:12345`

_Optional_: Run full node

Run full node:

```sh
./target/debug/citrea --dev --da-layer bitcoin --rollup-config-path resources/configs/bitcoin-regtest/rollup_config.toml --genesis-paths resources/genesis/bitcoin-regtest/
```

Full node RPC is accessible at `127.0.0.1:12346`

_Optional_: Run batch prover:

```sh
PARALLEL_PROOF_LIMIT=1 ./target/debug/citrea --dev --da-layer bitcoin --rollup-config-path resources/configs/bitcoin-regtest/batch_prover_rollup_config.toml --batch-prover resources/configs/bitcoin-regtest/batch_prover_config.toml --genesis-paths resources/genesis/bitcoin-regtest
```

If you want to test proofs, make sure to set `proof_sampling_number` in `resources/configs/bitcoin-regtest/batch_prover_config.toml` to 0, and you can set the `max_l2_blocks_per_commitment` to a number between 5-50, as higher numbers than that take too long even if you run the prover in execute mode.

To publish blocks on Bitcoin Regtest, run the sequencer with `test_mode` in sequencer config set to false and blocks will be published every two seconds.

### Discovery configuration

The network service now uses discv5 for peer discovery. When running local nodes, you can control the discovery layer with the following environment variables:

- `NETWORK_DISCOVERY_ENABLED` – set to `false` to disable discv5 (default: enabled).
- `NETWORK_DISCOVERY_BIND_ADDR` – UDP socket used for the discovery service (e.g. `0.0.0.0:9000`).
- `NETWORK_DISCOVERY_BOOTNODES` – comma separated list of base64-encoded ENRs to seed the routing table.
- `NETWORK_DISCOVERY_KEY_PATH` – path to a file containing the node's hex-encoded secp256k1 private key; if the file does not exist it will be created so that ENRs stay stable between restarts.
- `NETWORK_DISCOVERY_TARGET_PEERS` – maximum number of peers to search for before pausing background queries.
- `NETWORK_DISCOVERY_QUERY_INTERVAL_SECS` – number of seconds between background FINDNODE queries (default: 30).

_Optional_: Run light client prover:

```sh
./target/debug/citrea --dev --da-layer bitcoin --rollup-config-path resources/configs/bitcoin-regtest/light_client_prover_rollup_config.toml --light-client-prover resources/configs/bitcoin-regtest/light_client_prover_config.toml --genesis-paths resources/genesis/bitcoin-regtest
```

To delete sequencer or full nodes databases run:

```sh
make clean-node
```

#### Notes
If you want to run both the sequencer and the batch prover, it's a good idea to create different bitcoin wallets for each.

Then the wallets can be used separately by modifying rollup_config.toml files for both nodes like so:

```toml
# sequencer_rollup_config.toml
[da]
# node_url = "http://HOST:PORT/wallet/wallet-name"
node_url = "http://0.0.0.0:18433/wallet/sequencer-wallet"

# batch_prover_rollup_config.toml
[da]
# node_url = "http://HOST:PORT/wallet/wallet-name"
node_url = "http://0.0.0.0:18433/wallet/batch-prover-wallet"
```

Both wallets should be funded by running

```sh
bitcoin-cli -regtest -rpcwallet=wallet-name -generate 201
```

If your testing of the local network requires mining sequencer commitments and batch proofs, run in a separate terminal:

```sh
bitcoin-cli -regtest -generate
```

Or you can run this command in a separate terminal to periodically mine new regtest blocks:
```sh
while true; do; bitcoin-cli -regtest -generate; sleep 10; done;
```

## Testing

To run tests:

```sh
make test
```

This will run [`cargo nextest`](https://nexte.st), which will run all Rust tests inside the repo. As our e2e tests use docker, docker engine should be on when this is ran.

To run smart contract tests:
```sh
cd crates/evm/src/evm/system_contracts
forge test
```
