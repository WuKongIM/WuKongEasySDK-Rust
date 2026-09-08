#!/usr/bin/env python3
"""Bounded three-node acceptance of the exact Rust crate through verified WSS.

Only loopback processes and synthetic identities. A TLS proxy never changes a
protocol frame; its independent SEND counter catches replay hidden by dedup.
"""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import platform
import tempfile
import time
import urllib.request

from run import (SDK, Proxy, available_ports, certificates,
                 command, registry_consumer, stop)
from wire import SendCounter

SERVER_REVISION = "7ee20aed390aa7aef9d630b2a3566f5aca24e061"

USERS = ["group-alice", "group-bob", "group-carol", "group-dave"]
MAIN = "rust-cluster-main"
ISOLATED = "rust-cluster-isolated"


async def request(api, path, body=None):
    def perform():
        req = urllib.request.Request(f"http://127.0.0.1:{api}{path}",
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.load(response)
            if response.status != 200 or (body is not None and isinstance(result, dict)
                                          and result.get("status") != 200):
                raise RuntimeError(f"product request failed: {path}")
            return result
    return await asyncio.to_thread(perform)


class AuditedProxy(Proxy):
    """Keep bounded wire evidence independently of server message deduplication."""
    def __init__(self, upstream):
        super().__init__(upstream)
        self.sends = 0
        self.audit_error = False
        self.blocked_chunks = 0
        self.downstream = asyncio.Event()
        self.downstream.set()

    def wire_observer(self):
        owner = self

        def count():
            owner.sends += 1

        class Observer(SendCounter):
            def feed(self, data):
                try:
                    super().feed(data)
                except (ValueError, UnicodeError):
                    owner.audit_error = True
                    raise
        return Observer(count)

    async def before_forward(self, direction):
        if direction == "downstream" and not self.downstream.is_set():
            self.blocked_chunks += 1
            await self.downstream.wait()


class Cluster:
    """Own three isolated durable node directories and their exact process handles."""
    def __init__(self, directory, binary, env):
        self.directory, self.binary, self.env = directory, binary, env
        self.ports = [available_ports() for _ in range(3)]
        flat = [port for row in self.ports for port in row]
        if len(set(flat)) != len(flat):
            raise RuntimeError("ephemeral port collision")
        self.processes = [None] * 3
        self.logs = []
        self.proxies = []
        self.listeners = []

    async def start(self, tls):
        nodes = ",".join(f'{{id={i+1},addr="127.0.0.1:{p[0]}"}}' for i, p in enumerate(self.ports))
        for i, (rpc, api, manager, tcp, ws) in enumerate(self.ports):
            directory = self.directory / f"node{i+1}"
            directory.mkdir()
            (directory / "wukongim.toml").write_text(f'''[node]
id={i+1}
data_dir="{directory}/data"
[cluster]
id="rust-cluster-acceptance"
listen_addr="127.0.0.1:{rpc}"
nodes=[{nodes}]
initial_slot_count=12
hash_slot_count=256
slot_replica_n=3
join_token="synthetic-local-join"
[api]
listen_addr="127.0.0.1:{api}"
[manager]
listen_addr="127.0.0.1:{manager}"
auth_on=true
jwt_secret="synthetic-local-manager"
users=[{{username="admin",password="synthetic-local-password",permissions=[{{resource="*",actions=["*"]}}]}}]
[gateway]
token_auth_on=true
listeners=[{{name="ws",network="websocket",address="127.0.0.1:{ws}",transport="gnet",protocol="wsmux"}}]
[plugin]
socket_path="{self.directory}/n{i+1}.sock"
[observability]
metrics_enable=true
[log]
level="warn"
dir="{directory}/logs"
''')
            await self.spawn(i)
            proxy = AuditedProxy(ws)
            self.proxies.append(proxy)
            self.listeners.append(await asyncio.start_server(proxy.handle, "127.0.0.1", 0,
                ssl=tls, ssl_handshake_timeout=3))
        await asyncio.gather(*(self.ready(i) for i in range(3)))

    async def spawn(self, i):
        directory = self.directory / f"node{i+1}"
        log = (directory / "process.log").open("ab")
        self.logs.append(log)
        self.processes[i] = await asyncio.create_subprocess_exec(str(self.binary),
            "-config", str(directory / "wukongim.toml"), cwd=directory, env=self.env,
            stdout=log, stderr=log)

    async def ready(self, i):
        async with asyncio.timeout(60):
            while True:
                if self.processes[i].returncode is not None:
                    raise RuntimeError("node exited before readiness")
                try:
                    await request(self.ports[i][1], "/readyz")
                    reader, writer = await asyncio.wait_for(asyncio.open_connection(
                        "127.0.0.1", self.ports[i][4]), timeout=1)
                    try:
                        writer.write(b"GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
                        await writer.drain()
                        if b" 101 " in await asyncio.wait_for(reader.read(4096), timeout=1):
                            return
                    finally:
                        writer.close()
                        await writer.wait_closed()
                except (OSError, TimeoutError):
                    pass
                await asyncio.sleep(.1)

    async def close(self):
        for listener in self.listeners:
            listener.close()
            await listener.wait_closed()
        await asyncio.gather(*(proxy.close() for proxy in self.proxies))
        await asyncio.gather(*(stop(process) for process in self.processes))
        for log in self.logs:
            log.close()


async def exercise(cluster, executable, der, env, seconds, report):
    process = None
    attempted = 0
    phases = report["phases"]

    async def receive(status):
        line = await asyncio.wait_for(process.stdout.readline(), timeout=65)
        if not line:
            await asyncio.wait_for(process.wait(), timeout=10)
            raise RuntimeError("cluster probe exited: " + (await process.stderr.read()).decode(errors="replace")[:2000])
        result = json.loads(line)
        if result.get("status") != status:
            raise RuntimeError("unexpected cluster probe status")
        return result

    async def control(op, status, **fields):
        process.stdin.write((json.dumps({"op": op, **fields}) + "\n").encode())
        await process.stdin.drain()
        return await receive(status)

    async def send(phase, sender, recipients, reason=1, channel=MAIN, channel_type=2, unknown=False):
        nonlocal attempted
        report["active_phase"] = phase
        result = await control("send", "pass", phase=phase, sender=sender, recipients=recipients,
            reason=reason, channel=channel, channel_type=channel_type, unknown=unknown)
        if result.get("phase") != phase or result.get("reason") != reason or result.get("deliveries") != len(recipients):
            raise RuntimeError("phase receipt mismatch")
        attempted += 1
        phases.append(result)
        report.pop("active_phase", None)

    async def mutate(path, body, node=1):
        return await request(cluster.ports[node][1], path, body)

    async def members(path, indices):
        await mutate(path, {"channel_id": MAIN, "channel_type": 2,
                           "subscribers": [USERS[i] for i in indices]})

    async def exchange(prefix):
        # All six directed pairs traverse distinct ingress nodes; all four inboxes
        # are inspected on every send so unexpected fanout fails immediately.
        for sender, receiver in [(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]:
            await send(f"{prefix}_person_{sender}_{receiver}", sender, [receiver],
                       channel=USERS[receiver], channel_type=1)
        await send(f"{prefix}_group", 0, [1, 2])
        await send(f"{prefix}_isolation", 0, [1], channel=ISOLATED)

    async def wait_routes():
        # CONNECT authenticates the socket; Slot authority changes can still
        # clear volatile presence. Observe all API ingresses for two 25-second
        # heartbeat intervals before asserting loss-free steady-state delivery.
        stable_since = None
        async with asyncio.timeout(100):
            while True:
                views = await asyncio.gather(*(request(ports[1], "/user/onlinestatus", USERS)
                                               for ports in cluster.ports))
                if all(len(view) == 4 and {item.get("uid") for item in view} == set(USERS) and
                       all(item.get("online") == 1 for item in view) for view in views):
                    stable_since = stable_since or time.monotonic()
                    if time.monotonic() - stable_since >= 50:
                        return
                else:
                    stable_since = None
                await asyncio.sleep(.5)

    async def restart(index):
        await control("arm_reconnect", "armed")
        proxy = cluster.proxies[0]
        proxy.downstream.clear()
        blocked = proxy.blocked_chunks
        before = sum(item.sends for item in cluster.proxies)
        await send(f"unknown_before_crash_{index}", 0, [2], reason=0,
                   channel=USERS[2], channel_type=1, unknown=True)
        if proxy.blocked_chunks <= blocked or sum(item.sends for item in cluster.proxies) != before + 1:
            raise RuntimeError("withheld SENDACK evidence missing")
        started = time.monotonic()
        # Abruptly kill exactly the owned ingress process. Keep its address and
        # durable directory; this is same-endpoint reconnect, not URL failover.
        cluster.processes[0].kill()
        await cluster.processes[0].wait()
        await asyncio.sleep(1)
        proxy.downstream.set()
        await cluster.spawn(0)
        await cluster.ready(0)
        recovered = await control("wait_reconnect", "reconnected", clients=[0])
        if recovered.get("nodes") != [1, 2, 3, 2]:
            raise RuntimeError("original ingress identity changed")
        recovery = round((time.monotonic() - started) * 1000)
        await wait_routes()
        routable = round((time.monotonic() - started) * 1000)
        # No application retry. Observe all inboxes for an additional 500 ms
        # and match cumulative wire sends after an acknowledged cross-node send.
        await send(f"recovered_{index}", 2, [0], channel=USERS[0], channel_type=1)
        if sum(item.sends for item in cluster.proxies) != attempted:
            raise RuntimeError("unexpected outbound SEND/replay after restart")
        report["faults"].append({"kind": "withheld_ack_then_ingress_crash_restart",
            "node_id": 1, "unknown_send_outcome": "Timeout", "peer_deliveries": 1,
            "same_endpoint": True, "fault_to_reconnected_ms": recovery,
            "fault_to_stable_routes_ms": routable, "all_node_route_stability_ms": 50000})
        print(f"PASS ingress crash/restart {index}: connection {recovery} ms, stable routes {routable} ms, uncertain SEND not replayed", flush=True)

    try:
        for index, uid in enumerate(USERS):
            await mutate("/user/token", {"uid": uid, "token": uid + "-synthetic-token",
                "device_flag": 2, "device_level": 1}, node=index % 3)
        for channel, indices in [(MAIN, [0, 1, 2]), (ISOLATED, [0, 1])]:
            await mutate("/channel", {"channel_id": channel, "channel_type": 2,
                "allow_stranger": 0, "reset": 1, "subscribers": [USERS[i] for i in indices]})
        urls = [f"wss://127.0.0.1:{listener.sockets[0].getsockname()[1]}" for listener in cluster.listeners]
        process = await asyncio.create_subprocess_exec(str(executable),
            env={**env, "WK_WS_URL": urls[0], "WK_WS_URLS": json.dumps([urls[i] for i in [0, 1, 2, 1]]),
                 "WK_TEST_CA_DER": str(der)}, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        connected = await receive("ready")
        if connected.get("nodes") != [1, 2, 3, 2] or connected.get("clients") != 4:
            raise RuntimeError("clients did not authenticate on all three ingress nodes")
        report["connected_nodes"] = connected["nodes"]
        await exchange("initial")
        await send("nonmember_rejected", 3, [], reason=3)
        await members("/channel/subscriber_remove", [1])
        await send("removed_member_rejected", 1, [], reason=3)
        await send("removed_member_excluded", 0, [2])
        await mutate("/channel/blacklist_add", {"channel_id": MAIN, "channel_type": 2, "uids": [USERS[2]]})
        await send("denylisted_member_rejected", 2, [], reason=4)
        await restart(1)
        await send("recovered_removed_member_rejected", 1, [], reason=3)
        await send("recovered_denylisted_member_rejected", 2, [], reason=4)
        await send("recovered_removed_member_excluded", 0, [2])
        await mutate("/channel/blacklist_remove", {"channel_id": MAIN, "channel_type": 2, "uids": [USERS[2]]})
        await send("denylist_removed", 2, [0])
        await members("/channel/subscriber_add", [1])
        await send("member_readded", 1, [0, 2])
        await exchange("restored")
        started = time.monotonic()
        rounds = 0
        next_progress = 0
        repeated = False
        # The uninterrupted bounded workload includes another node failure at
        # its midpoint; fault/recovery time is included in the measured duration.
        while time.monotonic() - started < seconds:
            rounds += 1
            await exchange(f"round_{rounds}")
            elapsed = time.monotonic() - started
            if not repeated and elapsed >= seconds / 2:
                await restart(2)
                repeated = True
            if elapsed >= next_progress:
                print(f"RUN {elapsed:.0f}/{seconds}s: {rounds} rounds, {attempted} sends", flush=True)
                next_progress += 30
        report["workload_seconds"] = round(time.monotonic() - started, 3)
        report["rounds"] = rounds
        shutdown = await control("stop", "stopped")
        await asyncio.wait_for(process.wait(), timeout=10)
        if process.returncode or shutdown.get("clients_destroyed") != 4:
            raise RuntimeError("probe cleanup failed")
        async with asyncio.timeout(10):
            while any(proxy.tasks or proxy.writers for proxy in cluster.proxies):
                await asyncio.sleep(.02)
        async with asyncio.timeout(20):
            while True:
                online = await mutate("/user/onlinestatus", USERS)
                if online == []:
                    break
                await asyncio.sleep(.1)
        wire = sum(proxy.sends for proxy in cluster.proxies)
        if wire != attempted or any(proxy.audit_error for proxy in cluster.proxies):
            raise RuntimeError("cumulative wire SEND audit failed")
        report.update({"wire_sends": wire, "application_sends": attempted,
            "deliveries": sum(item["deliveries"] for item in phases),
            "clients_destroyed": 4, "offline_confirmed": True,
            "exclusion_observation_ms": 500})
    finally:
        await stop(process)


async def run(args, directory, report):
    source = args.server_source.resolve()
    revision = command(["git", "rev-parse", "HEAD"], cwd=source).stdout.strip()
    if revision != SERVER_REVISION or command(["git", "status", "--porcelain"], cwd=source).stdout.strip():
        raise RuntimeError("server requires exact clean pinned source")
    sdk_revision = command(["git", "rev-parse", "HEAD"], cwd=SDK).stdout.strip()
    sdk_status = command(["git", "status", "--porcelain", "--untracked-files=all"], cwd=SDK).stdout
    env = {key: value for key, value in os.environ.items() if not key.startswith("WK_")}
    env["GOWORK"] = "off"
    binary = directory / "wukongim"
    print("Building pinned three-node server and public API probe", flush=True)
    await asyncio.to_thread(command, ["go", "build", "-o", str(binary), "./cmd/wukongim"], cwd=source, env=env)
    if args.distribution == "registry":
        executables, distribution = await asyncio.to_thread(registry_consumer, directory, env)
    else:
        await asyncio.to_thread(command, ["cargo", "build", "--locked", "--example", "cluster_acceptance"], cwd=SDK, env=env)
        executables, distribution = SDK / "target/debug/examples", {"distribution": "source"}
    report.update(distribution)
    report.update({"server_revision": revision, "server_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "sdk_revision": sdk_revision, "sdk_tree_clean": not sdk_status.strip(),
        "platform": platform.system() + " " + platform.machine(),
        "harness_sha256": {name: hashlib.sha256((SDK / name).read_bytes()).hexdigest() for name in
            ["tests/acceptance/cluster.py", "tests/acceptance/run.py", "tests/acceptance/wire.py", "examples/cluster_acceptance.rs"]}})
    tls, der = await asyncio.to_thread(certificates, directory)
    cluster = Cluster(directory, binary, env)
    try:
        await cluster.start(tls)
        print("Three-node cluster ready; 256 hash slots, 12 logical slots, 3 replicas", flush=True)
        await asyncio.wait_for(exercise(cluster, executables / "cluster_acceptance", der, env, args.seconds, report),
                               timeout=args.seconds + 300)
        if sdk_revision != command(["git", "rev-parse", "HEAD"], cwd=SDK).stdout.strip() or sdk_status != command(["git", "status", "--porcelain", "--untracked-files=all"], cwd=SDK).stdout:
            raise RuntimeError("SDK source changed during acceptance")
    except Exception:
        # Retain bounded diagnostics for failed stages before deleting owned files.
        # Workload identities are synthetic; exclude any line mentioning secrets
        # or raw payloads, and retain only delivery/routing failure signals.
        diagnostics = []
        for index in range(3):
            log = directory / f"node{index+1}" / "process.log"
            lines = []
            if log.exists():
                with log.open("rb") as stream:
                    stream.seek(max(0, log.stat().st_size - 65536))
                    lines = stream.read(65536).decode(errors="replace").splitlines()
            selected = [line[:1500] for line in lines if any(word in line.lower() for word in
                ("delivery", "recipient", "post_commit", "append_failed", "route")) and not any(word in line.lower() for word in
                ("token", "password", "secret", "payload"))][-30:]
            def metrics():
                with urllib.request.urlopen(f"http://127.0.0.1:{cluster.ports[index][1]}/metrics", timeout=2) as response:
                    data = response.read(2 * 1024 * 1024).decode()
                return [line for line in data.splitlines() if not line.startswith("#") and
                    any(word in line for word in ("delivery", "post_commit", "recipient", "presence")) and "_bucket{" not in line][:600]
            try:
                values = await asyncio.to_thread(metrics)
            except (OSError, TimeoutError):
                values = []
            diagnostics.append({"node": index+1, "logs": selected, "metrics": values})
        report["diagnostics"] = diagnostics
        raise
    finally:
        await cluster.close()
        report["cleanup"] = {"running_owned_processes": sum(p.returncode is None for p in cluster.processes if p),
            "proxy_streams": sum(len(p.writers) for p in cluster.proxies),
            "proxy_tasks": sum(len(p.tasks) for p in cluster.proxies)}
        if any(report["cleanup"].values()):
            report["status"] = "failed"
            raise RuntimeError("owned cluster cleanup incomplete")
    report["status"] = "pass"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-source", type=Path, required=True)
    parser.add_argument("--distribution", choices=("source", "registry"), default="registry")
    parser.add_argument("--seconds", type=int, choices=(60, 600), default=60)
    parser.add_argument("--output", type=Path, default=Path(".acceptance/cluster.json"))
    args = parser.parse_args()
    report = {"status": "failed", "topology": "three-node cluster", "hash_slots": 256,
        "initial_logical_slots": 12, "slot_replicas": 3, "token_auth_on": True,
        "transport": "WSS with verified private CA and hostname", "requested_workload_seconds": args.seconds,
        "phases": [], "faults": []}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(prefix="wkrc-", dir="/tmp") as directory:
            asyncio.run(run(args, Path(directory), report))
    except Exception as error:
        report["status"] = "failed"
        report["error"] = (type(error).__name__ + ": " + str(error))[:2000]
        raise
    finally:
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"PASS three-node cluster: {report['wire_sends']} SENDs, {report['deliveries']} deliveries; cleanup complete", flush=True)


if __name__ == "__main__":
    main()
