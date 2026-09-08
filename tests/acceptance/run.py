#!/usr/bin/env python3
"""Bounded real-server Rust/JS acceptance with TLS validation and three TCP cuts.

Builds the pinned server, uses only loopback listeners and synthetic identities,
retains a redacted JSON receipt, and always stops every process it starts.
"""
import argparse
import asyncio
import contextlib
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import time
import urllib.request

SERVER_REVISION = "27a39f15bf163b433f417b78ab6bfc6e589585e5"
SDK = Path(__file__).resolve().parents[2]


def command(args, **kwargs):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=600, **kwargs)


def available_ports():
    sockets = []
    try:
        for _ in range(5):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def certificates(directory):
    ca = directory / "ca.pem"
    ca_key = directory / "ca.key"
    leaf = directory / "server.pem"
    leaf_key = directory / "server.key"
    csr = directory / "server.csr"
    extensions = directory / "extensions.cnf"
    extensions.write_text("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n")
    command(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-subj", "/CN=Rust SDK Test Root", "-keyout", str(ca_key), "-out", str(ca)])
    command(["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost", "-keyout", str(leaf_key), "-out", str(csr)])
    command(["openssl", "x509", "-req", "-in", str(csr), "-CA", str(ca), "-CAkey", str(ca_key), "-CAcreateserial", "-days", "1", "-extfile", str(extensions), "-out", str(leaf)])
    der = directory / "ca.der"
    command(["openssl", "x509", "-in", str(ca), "-outform", "DER", "-out", str(der)])
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.minimum_version = ssl.TLSVersion.TLSv1_2
    tls.load_cert_chain(leaf, leaf_key)
    return tls, der


class Proxy:
    """TLS termination and raw byte forwarding; cuts affect only the Rust client."""
    def __init__(self, upstream):
        self.upstream = upstream
        self.paused = False
        self.writers = set()
        self.tasks = set()
        self.cut_receipts = []

    async def handle(self, reader, writer):
        current = asyncio.current_task()
        self.tasks.add(current)
        self.writers.add(writer)
        other = None
        pumps = []
        try:
            if self.paused:
                return
            remote, other = await asyncio.open_connection("127.0.0.1", self.upstream)
            self.writers.add(other)

            async def pump(source, target):
                while data := await source.read(65536):
                    target.write(data)
                    await target.drain()

            pumps = [asyncio.create_task(pump(reader, other)), asyncio.create_task(pump(remote, writer))]
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        except (OSError, ConnectionError):
            pass
        finally:
            for task in pumps:
                task.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            for stream in [writer, other]:
                if stream:
                    stream.close()
                    self.writers.discard(stream)
            self.tasks.discard(current)

    async def cut(self, index):
        self.paused = True
        active = len(self.writers)
        for writer in list(self.writers):
            writer.transport.abort()
        await asyncio.sleep(1)
        self.paused = False
        self.cut_receipts.append({"index": index, "active_streams": active, "outage_seconds": 1})
        if active < 2:
            raise RuntimeError("fault injection found no active connection")
        print(f"Network interruption {index}/3 completed", flush=True)

    async def close(self):
        for writer in list(self.writers):
            writer.transport.abort()
        for task in list(self.tasks):
            task.cancel()
        await asyncio.gather(*list(self.tasks), return_exceptions=True)


async def stop(process):
    if process and process.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=10)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()


async def run(args, directory):
    source = args.server_source.resolve()
    revision = command(["git", "rev-parse", "HEAD"], cwd=source).stdout.strip()
    if revision != SERVER_REVISION:
        raise RuntimeError("server source must match the pinned revision")
    command(["git", "diff", "HEAD", "--exit-code", "--", "cmd", "internal", "pkg", "go.mod", "go.sum"], cwd=source)
    env = {key: value for key, value in os.environ.items() if not key.startswith("WK_")}
    env["GOWORK"] = "off"
    binary = directory / "wukongim"
    print("Building pinned WuKongIM server", flush=True)
    await asyncio.to_thread(command, ["go", "build", "-o", str(binary), "./cmd/wukongim"], cwd=source, env=env)
    await asyncio.to_thread(command, ["cargo", "build", "--locked", "--examples"], cwd=SDK, env=env)
    await asyncio.to_thread(command, ["npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund"], cwd=SDK / "tests/acceptance", env=env)
    tls, der = await asyncio.to_thread(certificates, directory)
    rpc, api, manager, tcp, ws = available_ports()
    config = directory / "wukongim.toml"
    config.write_text(f'''[node]
id=1
data_dir="{directory}/data"
[cluster]
id="rust-sdk-acceptance"
listen_addr="127.0.0.1:{rpc}"
nodes=[{{id=1,addr="127.0.0.1:{rpc}"}}]
initial_slot_count=10
hash_slot_count=256
slot_replica_n=1
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
listeners=[{{name="tcp",network="tcp",address="127.0.0.1:{tcp}",transport="gnet",protocol="wkproto"}},{{name="ws",network="websocket",address="127.0.0.1:{ws}",transport="gnet",protocol="wsmux"}}]
[log]
level="warn"
dir="{directory}/logs"
''')
    server = peer = probe = None
    proxy = Proxy(ws)
    listener = None
    faults = None
    peer_output = b""
    receipt = None
    with (directory / "server.log").open("wb") as server_log:
        try:
            server = await asyncio.create_subprocess_exec(str(binary), "-config", str(config), env=env, cwd=directory, stdout=server_log, stderr=server_log)
            ready_deadline = time.monotonic() + 60
            while time.monotonic() < ready_deadline:
                if server.returncode is not None:
                    raise RuntimeError("server exited before readiness")
                try:
                    await asyncio.to_thread(lambda: urllib.request.urlopen(f"http://127.0.0.1:{api}/readyz", timeout=1).read())
                    break
                except (OSError, TimeoutError):
                    await asyncio.sleep(.1)
            else:
                raise RuntimeError("server readiness timed out")
            # HTTP readiness can precede the final shared gateway listener restart.
            # Probe a real HTTP Upgrade before starting the clients under test.
            gateway_deadline = time.monotonic() + 15
            while time.monotonic() < gateway_deadline:
                try:
                    def upgraded():
                        with socket.create_connection(("127.0.0.1", ws), timeout=1) as sock:
                            sock.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
                            return b" 101 " in sock.recv(4096)
                    if await asyncio.to_thread(upgraded):
                        break
                except OSError:
                    pass
                await asyncio.sleep(.1)
            else:
                raise RuntimeError("gateway Upgrade readiness timed out")
            for uid, flag in [("rust-alice", 2), ("rust-bob", 2), ("js-bob", 1)]:
                request = urllib.request.Request(f"http://127.0.0.1:{api}/user/token", data=json.dumps({"uid": uid, "token": uid + "-synthetic-token", "device_flag": flag, "device_level": 1}).encode(), headers={"Content-Type": "application/json"})
                await asyncio.to_thread(lambda: urllib.request.urlopen(request, timeout=10).read())
            client_env = {**env, "WK_WS_URL": f"ws://127.0.0.1:{ws}", "WK_UID": "rust-alice", "WK_TOKEN": "rust-alice-synthetic-token", "WK_PEER_UID": "rust-bob", "WK_PEER_TOKEN": "rust-bob-synthetic-token"}
            roundtrip = SDK / "target/debug/examples/roundtrip"
            # Explicit timeout for each executable; no server/client process escapes ownership.
            result = await asyncio.to_thread(subprocess.run, [str(roundtrip)], env=client_env, capture_output=True, text=True, timeout=30)
            if result.returncode:
                raise RuntimeError("Rust/Rust roundtrip failed: " + result.stderr[:1000])
            print("Rust/Rust roundtrip passed", flush=True)
            # Check actual error category, not an arbitrary nonzero process exit.
            wrong = await asyncio.to_thread(subprocess.run, [str(SDK / "target/debug/examples/auth_check")], env={**client_env, "WK_TOKEN": "incorrect-token"}, capture_output=True, text=True, timeout=10)
            if wrong.returncode or wrong.stdout.strip() != "AUTH_REJECTED":
                raise RuntimeError("invalid Token rejection was not proven")
            peer_env = {**env, "WK_WS_URL": f"ws://127.0.0.1:{ws}", "WK_UID": "js-bob", "WK_TOKEN": "js-bob-synthetic-token", "WK_TEST_SECONDS": str(args.seconds)}
            peer = await asyncio.create_subprocess_exec("node", str(SDK / "tests/acceptance/peer.mjs"), env=peer_env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            if await asyncio.wait_for(peer.stdout.readline(), timeout=10) != b"READY\n":
                raise RuntimeError("JavaScript peer failed to connect")
            listener = await asyncio.start_server(proxy.handle, "127.0.0.1", 0, ssl=tls, ssl_handshake_timeout=3)
            port = listener.sockets[0].getsockname()[1]
            probe_env = {**env, "WK_WS_URL": f"wss://127.0.0.1:{port}", "WK_TEST_CA_DER": str(der), "WK_UID": "rust-alice", "WK_TOKEN": "rust-alice-synthetic-token", "WK_PEER_UID": "js-bob", "WK_TEST_SECONDS": str(args.seconds)}
            probe = await asyncio.create_subprocess_exec(str(SDK / "target/debug/examples/acceptance"), env=probe_env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

            async def interrupt():
                for index in range(1, 4):
                    await asyncio.sleep(args.seconds / 4 - (1 if index > 1 else 0))
                    await proxy.cut(index)

            faults = asyncio.create_task(interrupt())
            output, errors = await asyncio.wait_for(probe.communicate(), timeout=args.seconds + 20)
            if probe.returncode:
                raise RuntimeError("sustained WSS probe failed: " + errors.decode(errors="replace")[:1000])
            await faults
            receipt = json.loads(output)
            if receipt.get("status") != "pass":
                raise RuntimeError("invalid probe receipt")
            await stop(peer)
            peer_output = await peer.stdout.read()
            peer_receipt = json.loads(peer_output)
            if peer.returncode or peer_receipt.get("status") != "pass" or peer_receipt["replies"] < receipt["completed"]:
                raise RuntimeError("JS peer failed or did not confirm replies")
            receipt.update({"server_revision": revision, "sdk_revision": command(["git", "rev-parse", "HEAD"], cwd=SDK).stdout.strip(), "sdk_tree_clean": not command(["git", "status", "--porcelain", "--untracked-files=no"], cwd=SDK).stdout.strip(), "js_package": "easyjssdk@2.0.4", "topology": "single-node cluster", "hash_slots": 256, "token_auth_on": True, "incorrect_token": "rejected", "transport": "WSS with verified private CA and hostname", "rust_rust": "pass", "network_cuts": proxy.cut_receipts, "peer": peer_receipt})
        finally:
            if faults:
                faults.cancel()
                await asyncio.gather(faults, return_exceptions=True)
            await stop(probe)
            await stop(peer)
            if receipt is None and peer and peer.stdout:
                print("JS result: " + (await peer.stdout.read()).decode(errors="replace")[:1000], flush=True)
            if listener:
                listener.close()
                await listener.wait_closed()
            await proxy.close()
            await stop(server)
            if receipt is None:
                for log in (directory / "logs").rglob("*.log"):
                    print("Synthetic server failure log: " + log.read_text(errors="replace")[-6000:], flush=True)
            print("Owned processes and proxy stopped", flush=True)
    if receipt is None:
        raise RuntimeError("no successful receipt")
    receipt["cleanup"] = "complete"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-source", type=Path, required=True)
    parser.add_argument("--seconds", type=int, choices=[30, 120, 600], default=120)
    parser.add_argument("--output", type=Path, default=SDK / ".acceptance/receipt.json")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="wukong-easy-sdk-") as directory:
        asyncio.run(run(args, Path(directory)))


if __name__ == "__main__":
    main()
