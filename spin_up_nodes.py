#!/usr/bin/env python3
import argparse
import asyncio
import atexit
import json
import os
import random
import re
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

ENR_REGEX = re.compile(r"(enr:[^\s]+)")
CONNECTIVITY_MARKERS = [
    "discv5 discovered peer",
    "Connection established with peer",
    "discv5 inserted node",
]
SUPPORTED_TOPOLOGIES = ("star", "line", "ring", "mesh", "random")
SUPPORTED_NODE_TYPES = ("sequencer", "full_node")

# Track Process Group IDs (PGIDs) instead of just PIDs
ACTIVE_PGIDS: List[int] = []
ACTIVE_STATE_DIR: Optional[Path] = None


@dataclass
class NodeSpec:
    name: str
    node_type: str
    bootnodes: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "type": self.node_type, "bootnodes": self.bootnodes}


@dataclass
class NodeRuntime:
    spec: NodeSpec
    discovery_port: int
    p2p_port: int
    rpc_port: int
    data_dir: Path


@dataclass
class NodeHandle:
    name: str
    process: asyncio.subprocess.Process
    stream_task: asyncio.Task
    watch_task: asyncio.Task
    log_path: Path


def _dedupe_preserve(items: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    ordered: List[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def generate_topology_config(
    topology: str,
    node_count: int,
    degree: int = 2,
    seed: Optional[int] = None,
) -> List[NodeSpec]:
    if node_count < 1:
        raise ValueError("Node count must be at least 1")

    normalized = topology.lower()
    if normalized not in SUPPORTED_TOPOLOGIES:
        raise ValueError(f"Unsupported topology '{topology}'. Expected one of {SUPPORTED_TOPOLOGIES}.")

    rng = random.Random(seed)
    specs: List[NodeSpec] = [NodeSpec("node1", "sequencer", [])]
    if node_count == 1:
        return specs

    for idx in range(2, node_count + 1):
        name = f"node{idx}"
        bootnodes: List[str] = []

        if normalized == "star":
            bootnodes = ["node1"]
        elif normalized == "line":
            bootnodes = [f"node{idx - 1}"]
        elif normalized == "ring":
            bootnodes = [f"node{idx - 1}"]
            if idx == node_count and node_count > 2:
                bootnodes.append("node1")
        elif normalized == "mesh":
            bootnodes = [spec.name for spec in specs]
        elif normalized == "random":
            candidates = [spec.name for spec in specs]
            degree_clamped = max(1, degree)
            sample_size = min(len(candidates), degree_clamped)
            bootnodes = rng.sample(candidates, sample_size) if candidates else []

        specs.append(NodeSpec(name, "full_node", _dedupe_preserve(bootnodes)))

    return specs


def write_topology_config(specs: Sequence[NodeSpec], path: Path) -> Path:
    path = path.expanduser().resolve()
    payload = [spec.to_dict() for spec in specs]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))
    return path


def _validate_node_spec_types(entry: Dict[str, Any]) -> NodeSpec:
    try:
        name = entry["name"]
        node_type = entry["type"]
    except KeyError as missing:
        raise ValueError(f"Missing required key: {missing}") from None

    bootnodes_raw = entry.get("bootnodes", [])
    if not isinstance(name, str) or not name:
        raise ValueError("Node 'name' must be a non-empty string.")
    if not isinstance(node_type, str):
        raise ValueError("Node 'type' must be a string.")
    if not isinstance(bootnodes_raw, list) or any(not isinstance(b, str) for b in bootnodes_raw):
        raise ValueError("Node 'bootnodes' must be a list of strings.")

    return NodeSpec(name=name, node_type=node_type, bootnodes=list(bootnodes_raw))


def load_topology_config(path: Path) -> List[NodeSpec]:
    path = path.expanduser().resolve()
    if not path.exists():
        raise SystemExit(f"Topology config not found: {path}")
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Failed to parse topology JSON: {exc}") from exc

    if not isinstance(data, list):
        raise SystemExit("Topology config must be a JSON array.")

    specs: List[NodeSpec] = []
    try:
        for entry in data:
            if not isinstance(entry, dict):
                raise ValueError("Each topology entry must be an object.")
            specs.append(_validate_node_spec_types(entry))
    except ValueError as exc:
        raise SystemExit(f"Invalid topology config: {exc}") from exc

    return specs


def _ensure_acyclic(specs: Sequence[NodeSpec]) -> None:
    adjacency = {spec.name: spec.bootnodes for spec in specs}
    visiting: Set[str] = set()
    visited: Set[str] = set()

    def dfs(node: str) -> None:
        if node in visiting:
            raise ValueError(f"Cycle detected involving '{node}'.")
        if node in visited:
            return
        visiting.add(node)
        for dependency in adjacency.get(node, []):
            dfs(dependency)
        visiting.remove(node)
        visited.add(node)

    for spec in specs:
        dfs(spec.name)


def validate_topology(specs: Sequence[NodeSpec]) -> None:
    if not specs:
        raise ValueError("Topology config cannot be empty.")

    names = [spec.name for spec in specs]
    if len(set(names)) != len(names):
        raise ValueError("Node names must be unique.")

    for spec in specs:
        if spec.node_type not in SUPPORTED_NODE_TYPES:
            raise ValueError(f"Unsupported node type '{spec.node_type}' for {spec.name}.")
        for bootnode in spec.bootnodes:
            if bootnode not in names:
                raise ValueError(f"Unknown bootnode '{bootnode}' referenced by {spec.name}.")
            if bootnode == spec.name:
                raise ValueError(f"Node {spec.name} cannot list itself as a bootnode.")

    sequencers = [spec for spec in specs if spec.node_type == "sequencer"]
    if len(sequencers) != 1:
        raise ValueError("Topology must include exactly one sequencer.")
    if specs[0].node_type != "sequencer":
        raise ValueError("The first node in the topology must be the sequencer.")

    _ensure_acyclic(specs)


def assign_runtime(
    specs: Sequence[NodeSpec], args: argparse.Namespace, state_dir: Path
) -> Dict[str, NodeRuntime]:
    runtime: Dict[str, NodeRuntime] = {}
    for idx, spec in enumerate(specs, start=1):
        if idx == 1:
            discovery_port = args.node1_discovery_port
            p2p_port = args.node1_p2p_port
            rpc_port = args.node1_rpc_port
        else:
            discovery_port = args.base_discovery_port + idx - 2
            p2p_port = args.base_p2p_port + idx - 2
            rpc_port = args.base_rpc_port + idx - 2

        data_dir = (state_dir / spec.name).resolve()
        data_dir.mkdir(parents=True, exist_ok=True)
        runtime[spec.name] = NodeRuntime(
            spec=spec,
            discovery_port=discovery_port,
            p2p_port=p2p_port,
            rpc_port=rpc_port,
            data_dir=data_dir,
        )
    return runtime


def build_env_for_node(
    base_env: Dict[str, str],
    runtime: NodeRuntime,
    runtime_map: Dict[str, NodeRuntime],
    args: argparse.Namespace,
    bootnode_names: Sequence[str],
) -> Dict[str, str]:
    env = base_env.copy()
    if runtime.spec.node_type == "sequencer":
        env["NODE1_DISCOVERY_UDP_PORT"] = str(runtime.discovery_port)
        env["NODE1_P2P_TCP_PORT"] = str(runtime.p2p_port)
        env["NODE1_SEQUENCER_RPC_PORT"] = str(runtime.rpc_port)
        env["NODE1_DATA_DIR"] = str(runtime.data_dir)
        env["NETWORK_DIAL_ADDR"] = f"/ip4/{args.public_ip}/tcp/{runtime.p2p_port}"
        return env

    env["NODE2_DISCOVERY_UDP_PORT"] = str(runtime.discovery_port)
    env["NODE2_P2P_TCP_PORT"] = str(runtime.p2p_port)
    env["NODE2_RPC_PORT"] = str(runtime.rpc_port)
    env["NODE2_DATA_DIR"] = str(runtime.data_dir)

    sequencer_runtime = next(
        (node for node in runtime_map.values() if node.spec.node_type == "sequencer"), None
    )
    if sequencer_runtime is None:
        raise RuntimeError("No sequencer defined in runtime map.")

    dial_target = sequencer_runtime
    if bootnode_names:
        dial_target = runtime_map.get(bootnode_names[0], sequencer_runtime)

    env["NETWORK_DIAL_ADDR"] = f"/ip4/{args.public_ip}/tcp/{dial_target.p2p_port}"
    return env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Spin up a local cluster of citrea nodes using run-wan-discovery.sh."
    )
    parser.add_argument(
        "nodes",
        nargs="?",
        type=int,
        help="Total number of nodes to launch (>=1). Required unless --config is provided.",
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to a topology JSON file. Overrides --nodes/--topology generation.",
    )
    parser.add_argument(
        "--topology",
        choices=SUPPORTED_TOPOLOGIES,
        default="star",
        help="Topology shape to generate when --config is not provided (default: star).",
    )
    parser.add_argument(
        "--degree",
        type=int,
        default=2,
        help="Degree used for --topology random (number of bootnodes sampled per node).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional seed for deterministic random topology generation.",
    )
    parser.add_argument(
        "--write-config",
        type=str,
        help="Write the generated topology JSON to this path before launching.",
    )
    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="Only generate the topology JSON (with --write-config) and exit without starting nodes.",
    )
    parser.add_argument(
        "--script",
        default="./run-wan-discovery.sh",
        help="Path to run-wan-discovery.sh (default: ./run-wan-discovery.sh).",
    )
    parser.add_argument(
        "--log-dir",
        default="logs",
        help="Directory to write node logs (default: ./logs).",
    )
    parser.add_argument(
        "--state-dir",
        default="~/.citrea-cluster",
        help="Directory to store per-node data (default: ~/.citrea-cluster).",
    )
    parser.add_argument(
        "--public-ip",
        default="127.0.0.1",
        help="Public IP advertised by nodes (default: 127.0.0.1).",
    )
    parser.add_argument(
        "--node1-public-ip",
        default=None,
        help="Public IP for node1 sequencer RPC (default: same as --public-ip).",
    )
    parser.add_argument(
        "--rust-log",
        default="info",
        help='Value for RUST_LOG (default: "info").',
    )
    parser.add_argument(
        "--enr-timeout",
        type=int,
        default=60,
        help="Seconds to wait for required bootnode ENRs before aborting a node launch (default: 60).",
    )
    parser.add_argument(
        "--check-interval",
        type=int,
        default=15,
        help="Seconds between connectivity log checks (default: 15).",
    )
    parser.add_argument(
        "--node1-discovery-port",
        type=int,
        default=9000,
        help="Discovery UDP port for node1 (default: 9000).",
    )
    parser.add_argument(
        "--node1-p2p-port",
        type=int,
        default=9100,
        help="P2P TCP port for node1 (default: 9100).",
    )
    parser.add_argument(
        "--node1-rpc-port",
        type=int,
        default=12345,
        help="Sequencer RPC port for node1 (default: 12345).",
    )
    parser.add_argument(
        "--base-discovery-port",
        type=int,
        default=9001,
        help="Base discovery UDP port for nodes >=2 (increments by 1 per node).",
    )
    parser.add_argument(
        "--base-p2p-port",
        type=int,
        default=9101,
        help="Base P2P TCP port for nodes >=2 (increments by 1 per node).",
    )
    parser.add_argument(
        "--base-rpc-port",
        type=int,
        default=12346,
        help="Base RPC port for nodes >=2 (increments by 1 per node).",
    )
    parser.add_argument(
        "--target-peers",
        type=int,
        default=None,
        help="Target peer count for nodes (passed as TARGET_PEER_COUNT environment variable).",
    )
    parser.add_argument(
        "--stagger-seconds",
        type=float,
        default=0.0,
        help="Seconds to wait between starting each node (default: 0 for simultaneous start).",
    )
    return parser.parse_args()


def build_base_env(args: argparse.Namespace, sequencer_runtime: NodeRuntime) -> Dict[str, str]:
    env = os.environ.copy()
    env["ALLOW_PRIVATE_IPS"] = "true"
    env["PUBLIC_IP"] = args.public_ip
    env["NODE1_PUBLIC_IP"] = args.node1_public_ip or args.public_ip
    env["RUST_LOG"] = args.rust_log
    env["NODE1_DISCOVERY_UDP_PORT"] = str(sequencer_runtime.discovery_port)
    env["NODE1_P2P_TCP_PORT"] = str(sequencer_runtime.p2p_port)
    env["NODE1_SEQUENCER_RPC_PORT"] = str(sequencer_runtime.rpc_port)
    env["NODE1_DATA_DIR"] = str(sequencer_runtime.data_dir.resolve())
    env["NETWORK_DIAL_ADDR"] = f"/ip4/{args.public_ip}/tcp/{sequencer_runtime.p2p_port}"
    if args.target_peers is not None:
        env["TARGET_PEER_COUNT"] = str(args.target_peers)
    return env


async def stream_output(
    name: str,
    process: asyncio.subprocess.Process,
    log_path: Path,
    enr_future: Optional[asyncio.Future] = None,
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w") as log_file:
        while True:
            line = await process.stdout.readline()  # type: ignore[union-attr]
            if not line:
                break
            text = line.decode(errors="replace")

            # Write to file
            log_file.write(text)
            log_file.flush()

            # ALWAYS print to console (with node name prefix)
            sys.stdout.write(f"[{name}] {text}")
            sys.stdout.flush()

            if enr_future and not enr_future.done():
                match = ENR_REGEX.search(text)
                if match:
                    enr_future.set_result(match.group(1))

async def launch_node(
    name: str,
    script_path: Path,
    node_args: List[str],
    env: Dict[str, str],
    log_path: Path,
    enr_future: Optional[asyncio.Future] = None,
) -> Tuple[asyncio.subprocess.Process, asyncio.Task]:
    process = await asyncio.create_subprocess_exec(
        str(script_path),
        *node_args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=env,
        preexec_fn=os.setsid if hasattr(os, "setsid") else None,
    )

    # Capture the PGID immediately
    try:
        pgid = os.getpgid(process.pid)
        ACTIVE_PGIDS.append(pgid)
    except ProcessLookupError:
        pass

    stream_task = asyncio.create_task(stream_output(name, process, log_path, enr_future))
    return process, stream_task


async def watch_process(
    name: str,
    process: asyncio.subprocess.Process,
    stop_event: asyncio.Event,
    enr_future: Optional[asyncio.Future] = None,
) -> None:
    code = await process.wait()
    if enr_future and not enr_future.done():
        enr_future.set_exception(RuntimeError(f"{name} exited before emitting an ENR (code {code})."))
    # If the process crashes unexpectedly, stop everything
    if not stop_event.is_set():
        print(f"[{name}] exited unexpectedly with code {code}")
        stop_event.set()


async def run_node_task(
    spec: NodeSpec,
    runtime: NodeRuntime,
    script_path: Path,
    base_env: Dict[str, str],
    runtime_map: Dict[str, NodeRuntime],
    args: argparse.Namespace,
    enr_futures: Dict[str, asyncio.Future],
    stop_event: asyncio.Event,
    log_dir: Path,
) -> NodeHandle:
    bootnode_enrs: List[str] = []
    try:
        if spec.bootnodes:
            try:
                bootnode_enrs = await asyncio.wait_for(
                    asyncio.gather(*(enr_futures[name] for name in spec.bootnodes)),
                    timeout=args.enr_timeout,
                )
            except asyncio.TimeoutError as exc:
                raise RuntimeError(
                    f"{spec.name} timed out after {args.enr_timeout}s waiting for bootnode ENRs {spec.bootnodes}"
                ) from exc

        env = build_env_for_node(base_env, runtime, runtime_map, args, spec.bootnodes)
        log_path = log_dir / f"{spec.name}.log"

        if spec.node_type == "sequencer":
            node_args = ["node1"]
        else:
            if not bootnode_enrs:
                raise RuntimeError(f"{spec.name} requires at least one bootnode ENR.")
            bootnode_arg = '","'.join(bootnode_enrs)
            node_args = ["node2", bootnode_arg]

        process, stream_task = await launch_node(
            spec.name,
            script_path,
            node_args,
            env,
            log_path,
            enr_future=enr_futures[spec.name],
        )
        watch_task = asyncio.create_task(
            watch_process(spec.name, process, stop_event, enr_futures[spec.name])
        )
        return NodeHandle(
            name=spec.name,
            process=process,
            stream_task=stream_task,
            watch_task=watch_task,
            log_path=log_path,
        )
    except Exception as exc:
        if not enr_futures[spec.name].done():
            enr_futures[spec.name].set_exception(exc)
        stop_event.set()
        raise


async def run_node_task_with_delay(
    start_delay: float,
    spec: NodeSpec,
    runtime: NodeRuntime,
    script_path: Path,
    base_env: Dict[str, str],
    runtime_map: Dict[str, NodeRuntime],
    args: argparse.Namespace,
    enr_futures: Dict[str, asyncio.Future],
    stop_event: asyncio.Event,
    log_dir: Path,
) -> NodeHandle:
    if start_delay > 0:
        await asyncio.sleep(start_delay)
    return await run_node_task(
        spec,
        runtime,
        script_path,
        base_env,
        runtime_map,
        args,
        enr_futures,
        stop_event,
        log_dir,
    )


class ConnectivityWatcher:
    def __init__(self, patterns: Iterable[str]):
        self.patterns = [p.lower() for p in patterns]
        self.offsets: Dict[Path, int] = {}

    def scan(self, path: Path) -> int:
        if not path.exists():
            return 0
        offset = self.offsets.get(path, 0)
        count = 0
        with path.open("r") as handle:
            handle.seek(offset)
            for line in handle:
                lower = line.lower()
                if any(marker in lower for marker in self.patterns):
                    count += 1
            self.offsets[path] = handle.tell()
        return count


async def update_rpc_ports_file(
    runtime_map: Dict[str, NodeRuntime], state_dir: Path, stop_event: asyncio.Event
) -> None:
    """Periodically update the nodes.txt file with active RPC ports."""
    while not stop_event.is_set():
        try:
            ports_file = state_dir / "nodes.txt"
            lines = [f"localhost:{runtime.rpc_port}" for runtime in sorted(runtime_map.values(), key=lambda r: r.rpc_port)]
            ports_file.write_text("\n".join(lines) + "\n")
            await asyncio.sleep(5)  # Update every 5 seconds
        except Exception as e:
            print(f"[error] Failed to update nodes.txt: {e}")
            await asyncio.sleep(5)


def kill_pgid(pgid: int, sig: signal.Signals) -> None:
    """Helper to safely kill a process group."""
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        pass  # Already dead
    except Exception as e:
        print(f"[cleanup] Error killing pgid {pgid}: {e}")


async def terminate_processes(processes: List[Tuple[str, asyncio.subprocess.Process]]) -> None:
    """
    Revised termination logic:
    1. Send SIGTERM to the Process Group (PGID).
    2. Wait 5 seconds.
    3. Send SIGKILL to the PGID (Force Kill) regardless of process state.
    """
    if not processes:
        return

    print("[cleanup] Stopping all nodes...")

    # Step 1: SIGTERM to groups
    for name, proc in processes:
        try:
            pgid = os.getpgid(proc.pid)
            kill_pgid(pgid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    # Step 2: Wait gracefully
    print("[cleanup] Waiting 5s for graceful shutdown...")
    running_procs = [p for _, p in processes if p.returncode is None]
    if running_procs:
        try:
            await asyncio.wait_for(
                asyncio.gather(*(p.wait() for p in running_procs), return_exceptions=True),
                timeout=5,
            )
        except asyncio.TimeoutError:
            print("[cleanup] Timed out waiting for graceful shutdown.")

    # Step 3: SCORCHED EARTH - SIGKILL to all groups found at start
    # We do not rely on proc.returncode because the shell might have died while child stays alive.
    print("[cleanup] Force killing any remaining process groups...")
    for pgid in ACTIVE_PGIDS:
        kill_pgid(pgid, signal.SIGKILL)

    ACTIVE_PGIDS.clear()


def force_kill_active_pgids() -> None:
    """Synchronous cleanup for atexit / KeyboardInterrupt."""
    if not ACTIVE_PGIDS:
        return
    print("\n[cleanup] Force killing all tracked process groups...")
    for pgid in ACTIVE_PGIDS:
        kill_pgid(pgid, signal.SIGKILL)
    ACTIVE_PGIDS.clear()


def write_rpc_ports(runtime_map: Dict[str, NodeRuntime], state_dir: Path) -> Path:
    """Write RPC ports to nodes.txt file in localhost:port format."""
    ports_file = state_dir / "nodes.txt"
    lines = [f"localhost:{runtime.rpc_port}" for runtime in sorted(runtime_map.values(), key=lambda r: r.rpc_port)]
    ports_file.write_text("\n".join(lines) + "\n")
    return ports_file


async def main_async(args: argparse.Namespace) -> None:
    script_path = Path(args.script).expanduser().resolve()
    if not script_path.exists():
        raise SystemExit(f"Could not find script at {script_path}")

    log_dir = Path(args.log_dir).expanduser().resolve()
    state_dir = Path(args.state_dir).expanduser().resolve()
    global ACTIVE_STATE_DIR
    ACTIVE_STATE_DIR = state_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    if args.generate_only and not args.write_config:
        raise SystemExit("--generate-only requires --write-config to be set.")

    if args.config:
        topology = load_topology_config(Path(args.config))
        topology_label = str(Path(args.config).expanduser().resolve())
    else:
        if args.nodes is None or args.nodes < 1:
            raise SystemExit("Provide a node count >= 1 or a --config file.")
        topology = generate_topology_config(args.topology, args.nodes, args.degree, args.seed)
        topology_label = args.topology

    try:
        validate_topology(topology)
    except ValueError as exc:
        raise SystemExit(f"Invalid topology: {exc}") from exc

    if args.write_config:
        output_path = write_topology_config(topology, Path(args.write_config))
        print(f"[config] Wrote topology to {output_path}")
        if args.generate_only:
            print("[config] Generation-only mode: exiting without starting nodes.")
            return

    runtime_map = assign_runtime(topology, args, state_dir)
    sequencer_runtime = runtime_map[topology[0].name]
    base_env = build_base_env(args, sequencer_runtime)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def handle_sig():
        print("\n[signal] Interrupt received, stopping...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_sig)

    processes: List[Tuple[str, asyncio.subprocess.Process]] = []
    stream_tasks: List[asyncio.Task] = []
    watch_tasks: List[asyncio.Task] = []
    node_tasks: List[asyncio.Task] = []
    connectivity_task: Optional[asyncio.Task] = None
    ports_update_task: Optional[asyncio.Task] = None
    log_paths: List[Path] = []
    enr_futures: Dict[str, asyncio.Future] = {spec.name: loop.create_future() for spec in topology}

    try:
        if args.stagger_seconds > 0:
            print(f"[config] Staggering node startup by {args.stagger_seconds} seconds per node.")

        node_tasks = []
        for index, spec in enumerate(topology):
            delay = args.stagger_seconds * index if args.stagger_seconds else 0.0
            node_tasks.append(
                asyncio.create_task(
                    run_node_task_with_delay(
                        delay,
                        spec,
                        runtime_map[spec.name],
                        script_path,
                        base_env,
                        runtime_map,
                        args,
                        enr_futures,
                        stop_event,
                        log_dir,
                    )
                )
            )

        handles = await asyncio.gather(*node_tasks)
        for handle in handles:
            processes.append((handle.name, handle.process))
            stream_tasks.append(handle.stream_task)
            watch_tasks.append(handle.watch_task)
            log_paths.append(handle.log_path)

        # Write RPC ports to file and print them after nodes are running
        ports_file = write_rpc_ports(runtime_map, state_dir)
        print(f"[config] RPC ports written to {ports_file}")
        print("[config] RPC Port Mapping:")
        for runtime in sorted(runtime_map.values(), key=lambda r: r.rpc_port):
            print(f"  localhost:{runtime.rpc_port}")

        print(f"[info] All {len(handles)} nodes running with topology '{topology_label}'. Logs in {log_dir}")
        print("[info] Press Ctrl+C to stop all nodes.")

        # connectivity_task = asyncio.create_task(
        #     monitor_connectivity(log_paths, args.check_interval, stop_event)
        # )
        ports_update_task = asyncio.create_task(
            update_rpc_ports_file(runtime_map, state_dir, stop_event)
        )

        await stop_event.wait()
    except Exception as e:
        print(f"[error] {e}")
        stop_event.set()
        raise
    finally:
        if connectivity_task:
            connectivity_task.cancel()
            await asyncio.gather(connectivity_task, return_exceptions=True)
        if ports_update_task:
            ports_update_task.cancel()
            await asyncio.gather(ports_update_task, return_exceptions=True)
        for task in node_tasks:
            task.cancel()
        if node_tasks:
            await asyncio.gather(*node_tasks, return_exceptions=True)
        await terminate_processes(processes)
        await asyncio.gather(*stream_tasks, return_exceptions=True)
        await asyncio.gather(*watch_tasks, return_exceptions=True)


def main() -> None:
    args = parse_args()
    
    delete_previous = True
    if delete_previous:
        #delete all previous logs and state
        global ACTIVE_STATE_DIR
        ACTIVE_STATE_DIR = Path(args.state_dir).expanduser().resolve()
        if ACTIVE_STATE_DIR.exists():
            import shutil
            shutil.rmtree(ACTIVE_STATE_DIR)
        


    atexit.register(force_kill_active_pgids)
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        # Fallback if async loop is interrupted directly
        force_kill_active_pgids()


if __name__ == "__main__":
    main()
