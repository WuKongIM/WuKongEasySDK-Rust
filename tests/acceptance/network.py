"""Directional TLS proxy faults and bounded public-package resource acceptance."""
import asyncio
import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.request


def process_sample(pid):
    """Observe the probe, not the server or Python harness; never inspect payloads."""
    rss = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], check=True,
                         capture_output=True, text=True, timeout=5)
    if sys.platform.startswith("linux"):
        descriptors = len(list(Path(f"/proc/{pid}/fd").iterdir()))
    elif sys.platform == "darwin":
        files = subprocess.run(["lsof", "-a", "-p", str(pid), "-F", "f"],
                               check=True, capture_output=True, text=True, timeout=5)
        descriptors = sum(line[1:].isdigit() for line in files.stdout.splitlines() if line.startswith("f"))
    else:
        raise RuntimeError("resource acceptance requires Linux or macOS")
    return {"rss_kib": int(rss.stdout.strip()), "file_descriptors": descriptors}


async def run_network(executable, api, ws, tls, der, env, proxy_factory, stop, seconds):
    class FaultProxy(proxy_factory):
        def __init__(self, upstream):
            super().__init__(upstream)
            self.gates = {name: asyncio.Event() for name in ("upstream", "downstream")}
            for gate in self.gates.values():
                gate.set()
            self.delay = False
            self.delayed_chunks = 0
            self.blocked_chunks = 0

        async def before_forward(self, direction):
            if not self.gates[direction].is_set():
                self.blocked_chunks += 1
            await self.gates[direction].wait()
            if self.delay:
                # A deterministic 20/40/60 ms cycle varies each forwarded chunk.
                duration = (.02, .04, .06)[self.delayed_chunks % 3]
                self.delayed_chunks += 1
                await asyncio.sleep(duration)

    fault, healthy = FaultProxy(ws), proxy_factory(ws)
    listeners = []
    process = None
    samples = []
    cycles = []
    started = time.monotonic()

    async def control(op, **fields):
        process.stdin.write((json.dumps({"op": op, **fields}) + "\n").encode())
        await process.stdin.drain()
        line = await asyncio.wait_for(process.stdout.readline(), timeout=20)
        if not line:
            await asyncio.wait_for(process.wait(), timeout=10)
            errors = await process.stderr.read()
            raise RuntimeError("network probe exited: " + errors.decode(errors="replace")[:2000])
        result = json.loads(line)
        if result.get("status") != ("stopped" if op == "stop" else "pass") or (op != "stop" and result.get("op") != op):
            raise RuntimeError("unexpected network command receipt")
        return result

    async def disconnected_proxies():
        deadline = time.monotonic() + 5
        while fault.writers or fault.tasks or healthy.writers or healthy.tasks:
            if time.monotonic() >= deadline:
                raise RuntimeError("destroy did not drain owned proxy connections/tasks")
            await asyncio.sleep(.02)

    try:
        for uid in ["network-alice", "network-bob"]:
            def register():
                req = urllib.request.Request(f"http://127.0.0.1:{api}/user/token",
                    data=json.dumps({"uid": uid, "token": uid + "-synthetic-token", "device_flag": 2, "device_level": 1}).encode(),
                    headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=10) as response:
                    if response.status != 200 or json.load(response).get("status") != 200:
                        raise RuntimeError("network identity registration failed")
            await asyncio.to_thread(register)
        for proxy in [fault, healthy]:
            listeners.append(await asyncio.start_server(proxy.handle, "127.0.0.1", 0,
                                                       ssl=tls, ssl_handshake_timeout=3))
        urls = [f"wss://127.0.0.1:{listener.sockets[0].getsockname()[1]}" for listener in listeners]
        process = await asyncio.create_subprocess_exec(str(executable),
            env={**env, "WK_WS_URL": urls[0], "WK_PEER_WS_URL": urls[1], "WK_TEST_CA_DER": str(der)},
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        ready = json.loads(await asyncio.wait_for(process.stdout.readline(), timeout=10))
        if ready != {"status": "ready"}:
            raise RuntimeError("network probe readiness failed")
        started = time.monotonic()
        # Warm up three complete lifecycles before using quiescent samples as a
        # growth baseline. Short CI executes at least four cycles, including a post-warmup sample.
        while time.monotonic() - started < seconds or len(cycles) < 4:
            index = len(cycles) + 1
            await control("new")
            await control("exchange")
            fault.delay = True
            before = fault.delayed_chunks
            delayed = await control("exchange")
            fault.delay = False
            if fault.delayed_chunks <= before or delayed["elapsed_ms"] < 40:
                raise RuntimeError("delay/jitter injection was not observed")
            fault.gates["downstream"].clear()
            blocked_before = fault.blocked_chunks
            unknown = await control("blocked_ack")
            if unknown["details"] != {"timeouts": 2, "backpressure": 1, "peer_deliveries": 2} or fault.blocked_chunks == blocked_before:
                raise RuntimeError("unknown-outcome/admission evidence missing")
            fault.gates["downstream"].set()
            await control("quiet")
            # Prove timed-out requests release admission and old sends are not replayed.
            await control("exchange")
            lagged = await control("lag")
            if lagged["details"].get("lagged", 0) < 32:
                raise RuntimeError("bounded slow-observer loss was not reported")
            await control("arm")
            fault_started = time.monotonic()
            if index % 2:
                fault.gates["upstream"].clear()
                fault.gates["downstream"].clear()
                await control("disconnected", heartbeat=True)
                for gate in fault.gates.values():
                    gate.set()
                fault_kind = "bidirectional_blackhole"
            else:
                await fault.cut(index // 2, total=None)
                await control("disconnected", heartbeat=False)
                fault_kind = "transport_abort"
            recovered = await control("recovered")
            fault_to_recovered_ms = round((time.monotonic() - fault_started) * 1000)
            await control("exchange")
            destroyed = await control("destroy")
            if destroyed["details"].get("clients_destroyed") != 2 or destroyed["details"].get("deliveries") != 58:
                raise RuntimeError("cycle delivery or destruction totals mismatch")
            await disconnected_proxies()
            sample = await asyncio.to_thread(process_sample, process.pid)
            sample.update({"cycle": index, "elapsed_seconds": round(time.monotonic() - started, 3),
                           "active_proxy_streams": 0, "active_proxy_tasks": 0})
            samples.append(sample)
            cycle = {"cycle": index, "fault": fault_kind, "fault_to_recovered_ms": fault_to_recovered_ms,
                     "reconnect_wait_ms": recovered["elapsed_ms"], "delayed_exchange_ms": delayed["elapsed_ms"],
                     "timeouts": 2, "backpressure": 1, "lagged_events": lagged["details"]["lagged"], "deliveries": 58}
            cycles.append(cycle)
            if len(samples) > 3:
                baseline = samples[2]
                if sample["rss_kib"] > baseline["rss_kib"] + 65536 or sample["file_descriptors"] > baseline["file_descriptors"] + 8:
                    raise RuntimeError("quiescent probe resource growth exceeded the fixed allowance")
            print(f"Network cycle {index} passed: {fault_kind}; RSS {sample['rss_kib']} KiB, FDs {sample['file_descriptors']}", flush=True)
        await control("stop")
        await asyncio.wait_for(process.wait(), timeout=10)
        if process.returncode:
            raise RuntimeError("network probe did not exit cleanly")
        return {"status": "pass", "requested_seconds": seconds, "elapsed_seconds": round(time.monotonic() - started, 3),
                "cycles": cycles, "samples": samples, "clients_destroyed": 2 * len(cycles),
                "deliveries": 58 * len(cycles), "unknown_outcomes": 2 * len(cycles),
                "delayed_chunks": fault.delayed_chunks, "blocked_chunks": fault.blocked_chunks,
                "delay_per_chunk_ms": [20, 40, 60], "replay_observation_ms": 150,
                "request_timeout_ms": 800, "pong_timeout_ms": 3000, "max_in_flight": 2, "event_capacity": 16,
                "resource_limits": {"warmup_cycles": 3, "rss_growth_kib": 65536, "fd_growth": 8,
                    "baseline": samples[2], "growth_checked": len(samples) > 3},
                "cleanup": "complete"}
    finally:
        await stop(process)
        for listener in listeners:
            listener.close()
            await listener.wait_closed()
        await fault.close()
        await healthy.close()
