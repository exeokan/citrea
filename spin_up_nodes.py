#!/usr/bin/env python3
import argparse
import asyncio
import os
import re
import signal
import sys
import atexit
import subprocess
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

ENR_REGEX = re.compile(r"(enr:[^\s]+)")
CONNECTIVITY_MARKERS = [
    "discv5 discovered peer",
    "Connection established with peer",
    "discv5 inserted node",
]

# Track Process Group IDs (PGIDs) instead of just PIDs
ACTIVE_PGIDS: List[int] = []
ACTIVE_STATE_DIR: Optional[Path] = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Spin up a local cluster of citrea nodes using run-wan-discovery.sh."
    )
    parser.add_argument("nodes", type=int, help="Total number of nodes to launch (>=1).")
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
        help="Seconds to wait for the bootnode ENR before aborting (default: 60).",
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
    return parser.parse_args()


def build_base_env(args: argparse.Namespace, state_dir: Path) -> Dict[str, str]:
    env = os.environ.copy()
    env["ALLOW_PRIVATE_IPS"] = "true"
    env["PUBLIC_IP"] = args.public_ip
    env["NODE1_PUBLIC_IP"] = args.node1_public_ip or args.public_ip
    env["RUST_LOG"] = args.rust_log
    env["NODE1_DISCOVERY_UDP_PORT"] = str(args.node1_discovery_port)
    env["NODE1_P2P_TCP_PORT"] = str(args.node1_p2p_port)
    env["NODE1_SEQUENCER_RPC_PORT"] = str(args.node1_rpc_port)
    env["NODE1_DATA_DIR"] = str((state_dir / "node1").resolve())
    env["NETWORK_DIAL_ADDR"] = f"/ip4/{args.public_ip}/tcp/{args.node1_p2p_port}"
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


async def wait_for_enr(
    enr_future: asyncio.Future,
    process: asyncio.subprocess.Process,
    timeout: int,
) -> str:
    proc_wait = asyncio.create_task(process.wait())
    try:
        done, pending = await asyncio.wait(
            {enr_future, proc_wait},
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if enr_future in done:
            return enr_future.result()
        if proc_wait in done:
            code = proc_wait.result()
            raise RuntimeError(
                f"Bootnode exited with code {code} before emitting an ENR."
            )
        raise RuntimeError(
            f"Timed out waiting for bootnode ENR after {timeout} seconds."
        )
    finally:
        proc_wait.cancel()


async def watch_process(
    name: str, process: asyncio.subprocess.Process, stop_event: asyncio.Event
) -> None:
    code = await process.wait()
    # If the process crashes unexpectedly, stop everything
    if not stop_event.is_set():
        print(f"[{name}] exited unexpectedly with code {code}")
        stop_event.set()


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


async def monitor_connectivity(
    log_paths: List[Path], interval: int, stop_event: asyncio.Event
) -> None:
    watcher = ConnectivityWatcher(CONNECTIVITY_MARKERS)
    while not stop_event.is_set():
        await asyncio.sleep(interval)
        for path in log_paths:
            matches = watcher.scan(path)
            if matches:
                print(f"[connectivity] {path.name}: {matches} new peer events")


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


async def main_async(args: argparse.Namespace) -> None:
    if args.nodes < 1:
        raise SystemExit("nodes must be >= 1")

    script_path = Path(args.script).expanduser().resolve()
    if not script_path.exists():
        raise SystemExit(f"Could not find script at {script_path}")

    log_dir = Path(args.log_dir).expanduser().resolve()
    state_dir = Path(args.state_dir).expanduser().resolve()
    global ACTIVE_STATE_DIR
    ACTIVE_STATE_DIR = state_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    base_env = build_base_env(args, state_dir)

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
    connectivity_task: Optional[asyncio.Task] = None
    log_paths: List[Path] = []

    try:
        print("[setup] Starting node1 (bootnode) and waiting for ENR...")
        node1_enr_future: asyncio.Future = loop.create_future()
        node1_env = base_env.copy()
        node1_log = log_dir / "node1.log"
        node1_process, node1_stream = await launch_node(
            "node1",
            script_path,
            ["node1"],
            node1_env,
            node1_log,
            enr_future=node1_enr_future,
        )
        processes.append(("node1", node1_process))
        stream_tasks.append(node1_stream)
        watch_tasks.append(asyncio.create_task(watch_process("node1", node1_process, stop_event)))
        log_paths.append(node1_log)

        bootnode_enr = await wait_for_enr(node1_enr_future, node1_process, args.enr_timeout)
        print(f"[setup] Captured bootnode ENR")

        for idx in range(2, args.nodes + 1):
            node_env = base_env.copy()
            node_env["NODE2_DISCOVERY_UDP_PORT"] = str(args.base_discovery_port + idx - 2)
            node_env["NODE2_P2P_TCP_PORT"] = str(args.base_p2p_port + idx - 2)
            node_env["NODE2_RPC_PORT"] = str(args.base_rpc_port + idx - 2)
            node_env["NODE2_DATA_DIR"] = str((state_dir / f"node{idx}").resolve())
            node_env["NETWORK_DIAL_ADDR"] = f"/ip4/{args.public_ip}/tcp/{args.node1_p2p_port}"

            node_name = f"node{idx}"
            node_log = log_dir / f"{node_name}.log"
            print(f"[setup] Starting {node_name}...")
            process, stream_task = await launch_node(
                node_name,
                script_path,
                ["node2", bootnode_enr],
                node_env,
                node_log,
            )
            processes.append((node_name, process))
            stream_tasks.append(stream_task)
            watch_tasks.append(asyncio.create_task(watch_process(node_name, process, stop_event)))
            log_paths.append(node_log)

        print(f"[info] All {args.nodes} nodes running. Logs in {log_dir}")
        print("[info] Press Ctrl+C to stop all nodes.")

        connectivity_task = asyncio.create_task(
            monitor_connectivity(log_paths, args.check_interval, stop_event)
        )

        await stop_event.wait()
    except Exception as e:
        print(f"[error] {e}")
        stop_event.set()
        raise
    finally:
        if connectivity_task:
            connectivity_task.cancel()
        await terminate_processes(processes)
        await asyncio.gather(*stream_tasks, return_exceptions=True)
        # We don't await watch_tasks here because the processes are already killed


def main() -> None:
    args = parse_args()
    atexit.register(force_kill_active_pgids)
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        # Fallback if async loop is interrupted directly
        force_kill_active_pgids()


if __name__ == "__main__":
    main()
