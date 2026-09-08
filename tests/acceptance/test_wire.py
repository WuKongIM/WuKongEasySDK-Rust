import json
import unittest
from wire import SendCounter


def frame(method="send", extended=0, opcode=1):
    body = json.dumps({"method": method}).encode() if opcode == 1 else b"ping"
    mask = b"\x01\x02\x03\x04"
    if extended:
        size = bytes([0x80 | (126 if extended == 2 else 127)]) + len(body).to_bytes(extended, "big")
    else:
        size = bytes([0x80 | len(body)])
    return bytes([0x80 | opcode]) + size + mask + bytes(c ^ mask[i % 4] for i, c in enumerate(body))


class WireAudit(unittest.TestCase):
    def setUp(self):
        self.sends = 0
        self.parser = SendCounter(self.count)

    def count(self):
        self.sends += 1

    def test_split_upgrade_and_frames_count_only_send(self):
        data = b"GET / HTTP/1.1\r\nUpgrade: websocket\r\n\r\n" + frame("connect") + frame() + frame(opcode=9) + frame("recvack")
        for byte in data:
            self.parser.feed(bytes([byte]))
        self.assertEqual(self.sends, 1)
        self.assertEqual(len(self.parser.buffer), 0)

    def test_batched_extended_frames_count_retransmissions(self):
        self.parser.feed(b"GET / HTTP/1.1\r\n\r\n" + frame(extended=2) + frame(extended=8))
        self.assertEqual(self.sends, 2)

    def test_unsupported_fragmentation_fails_closed(self):
        self.parser.feed(b"GET / HTTP/1.1\r\n\r\n")
        with self.assertRaises(ValueError):
            self.parser.feed(bytes([1, 0x80]))

    def test_oversized_frame_fails_without_waiting_for_body(self):
        self.parser.feed(b"GET / HTTP/1.1\r\n\r\n")
        with self.assertRaises(ValueError):
            self.parser.feed(bytes([0x81, 0xff]) + (1000000).to_bytes(8, "big"))

    def test_oversized_upgrade_is_bounded(self):
        with self.assertRaises(ValueError):
            self.parser.feed(b"G" * 65537)

    def test_malformed_json_fails_closed(self):
        self.parser.feed(b"GET / HTTP/1.1\r\n\r\n")
        with self.assertRaises(ValueError):
            self.parser.feed(b"\x81\x81\x00\x00\x00\x00{")
