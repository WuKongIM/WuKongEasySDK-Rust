"""Trusted, bounded group-management driver for the public Rust API probe."""
import asyncio
import json
import urllib.request

USERS = ["group-alice", "group-bob", "group-carol", "group-dave"]
MAIN = "rust-group-main"
ISOLATED = "rust-group-isolated"


async def run_group(executable, api, ws, tls, der, env, proxy_factory, stop):
    proxy = proxy_factory(ws)
    listener = process = None
    phases = []

    async def mutate(path, body):
        def request():
            req = urllib.request.Request(
                f"http://127.0.0.1:{api}{path}", data=json.dumps(body).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status != 200 or json.load(response).get("status") != 200:
                    raise RuntimeError(f"group setup mutation failed: {path}")
        await asyncio.to_thread(request)

    async def receive(status):
        line = await asyncio.wait_for(process.stdout.readline(), timeout=20)
        if not line:
            await asyncio.wait_for(process.wait(), timeout=10)
            errors = await process.stderr.read()
            raise RuntimeError("group probe exited: " + errors.decode(errors="replace")[:2000])
        result = json.loads(line)
        if result.get("status") != status:
            raise RuntimeError(f"unexpected group probe receipt: {result}")
        return result

    async def control(op, status, **fields):
        process.stdin.write((json.dumps({"op": op, **fields}) + "\n").encode())
        await process.stdin.drain()
        return await receive(status)

    async def send(phase, sender, recipients, reason=1, channel=MAIN):
        result = await control("send", "pass", phase=phase, sender=sender,
                               recipients=recipients, reason=reason, channel=channel)
        if result.get("phase") != phase or result.get("reason") != reason or result.get("deliveries") != len(recipients):
            raise RuntimeError("group phase receipt mismatch")
        phases.append(result)
        print(f"Group phase passed: {phase}", flush=True)

    async def members(path, indices):
        await mutate(path, {"channel_id": MAIN, "channel_type": 2,
                            "subscribers": [USERS[i] for i in indices]})

    try:
        for uid in USERS:
            await mutate("/user/token", {"uid": uid, "token": uid + "-synthetic-token",
                                          "device_flag": 2, "device_level": 1})
        for channel, indices in [(MAIN, [0, 1, 2]), (ISOLATED, [0, 1])]:
            await mutate("/channel", {"channel_id": channel, "channel_type": 2,
                                      "allow_stranger": 0, "reset": 1,
                                      "subscribers": [USERS[i] for i in indices]})
        listener = await asyncio.start_server(proxy.handle, "127.0.0.1", 0,
                                             ssl=tls, ssl_handshake_timeout=3)
        port = listener.sockets[0].getsockname()[1]
        process = await asyncio.create_subprocess_exec(
            str(executable), env={**env, "WK_WS_URL": f"wss://127.0.0.1:{port}",
                                  "WK_TEST_CA_DER": str(der)},
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        if (await receive("ready")).get("clients") != 4:
            raise RuntimeError("group clients missing")
        await send("member_fanout", 0, [1, 2])
        await send("channel_isolation", 0, [1], channel=ISOLATED)
        await send("nonmember_rejected", 3, [], reason=3)
        await members("/channel/subscriber_add", [3])
        await send("added_member_send", 3, [0, 1, 2])
        await members("/channel/subscriber_remove", [1])
        await send("removed_member_rejected", 1, [], reason=3)
        await send("removed_member_excluded", 2, [0, 3])
        await mutate("/channel/blacklist_add", {"channel_id": MAIN, "channel_type": 2, "uids": [USERS[2]]})
        await send("denylisted_member_rejected", 2, [], reason=4)
        await mutate("/channel/blacklist_remove", {"channel_id": MAIN, "channel_type": 2, "uids": [USERS[2]]})
        await send("denylist_removed_send", 2, [0, 3])
        await control("arm_reconnect", "armed")
        await proxy.cut(1, total=1)
        if proxy.cut_receipts[0]["active_streams"] != 8:
            raise RuntimeError("group cut did not cover all four connections")
        recovery = await control("wait_reconnect", "reconnected")
        if recovery.get("clients") != 4:
            raise RuntimeError("group reconnection incomplete")
        await send("reconnected_membership_preserved", 0, [2, 3])
        await members("/channel/subscriber_add", [1])
        await send("readded_member_send", 1, [0, 2, 3])
        shutdown = await control("stop", "stopped")
        await asyncio.wait_for(process.wait(), timeout=10)
        if process.returncode or shutdown.get("clients_destroyed") != 4:
            raise RuntimeError("group client shutdown incomplete")
        return {"status": "pass", "clients": 4, "channels": 2, "phases": phases,
                "deliveries": sum(phase["deliveries"] for phase in phases),
                "exclusion_observation_ms": 500, "network_cuts": proxy.cut_receipts,
                "reconnected_clients": 4, "clients_destroyed": 4, "cleanup": "complete"}
    finally:
        await stop(process)
        if listener:
            listener.close()
            await listener.wait_closed()
        await proxy.close()
