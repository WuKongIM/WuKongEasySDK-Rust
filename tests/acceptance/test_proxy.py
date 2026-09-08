"""Deterministic cleanup regression: close may interrupt an active finalizer."""
import asyncio
import unittest
from unittest.mock import AsyncMock, patch
from run import Proxy


class Reader:
    def __init__(self):
        self.started = asyncio.Event()
        self.draining = asyncio.Event()

    async def read(self, _):
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.draining.set()
            await asyncio.Event().wait()


class Writer:
    def __init__(self):
        self.closed = False
        self.transport = self

    def close(self):
        self.closed = True

    def abort(self):
        self.close()


class ProxyCleanupTest(unittest.IsolatedAsyncioTestCase):
    async def test_close_during_pump_cleanup_releases_streams_and_task(self):
        local, remote = Reader(), Reader()
        client, upstream = Writer(), Writer()
        proxy = Proxy(1)
        with patch("run.asyncio.open_connection", AsyncMock(return_value=(remote, upstream))):
            task = asyncio.create_task(proxy.handle(local, client))
            await asyncio.wait_for(local.started.wait(), 1)
            await asyncio.wait_for(remote.started.wait(), 1)
            task.cancel()
            await asyncio.wait_for(local.draining.wait(), 1)
            await asyncio.wait_for(remote.draining.wait(), 1)
            # A second cancellation reaches the handler while it awaits its pumps.
            await asyncio.wait_for(proxy.close(), 1)
        self.assertTrue(task.done())
        self.assertTrue(client.closed and upstream.closed)
        self.assertEqual(proxy.writers, set())
        self.assertEqual(proxy.tasks, set())
