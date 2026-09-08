"""Bounded parser for the probe's uncompressed, unfragmented client WS frames.

Audit only SEND counts. Never retain or print credentials, IDs or message bodies.
Unsupported framing fails the acceptance instead of weakening its evidence.
"""
import json


class SendCounter:
    def __init__(self, count):
        self.buffer = bytearray()
        self.upgraded = False
        self.count = count

    def feed(self, data):
        self.buffer.extend(data)
        if len(self.buffer) > 65536:
            raise ValueError("probe wire audit buffer exceeded 64 KiB")
        if not self.upgraded:
            end = self.buffer.find(b"\r\n\r\n")
            if end < 0:
                return
            if not self.buffer.startswith(b"GET "):
                raise ValueError("probe wire audit expected HTTP Upgrade")
            del self.buffer[:end + 4]
            self.upgraded = True
        while len(self.buffer) >= 2:
            first, second = self.buffer[:2]
            if first & 0x70 or not first & 0x80 or not second & 0x80:
                raise ValueError("probe wire audit requires masked unfragmented frames")
            opcode = first & 15
            if opcode not in (1, 8, 9, 10):
                raise ValueError("unsupported probe wire opcode")
            length = second & 127
            header = 2
            if length in (126, 127):
                width = 2 if length == 126 else 8
                if len(self.buffer) < 2 + width:
                    return
                length = int.from_bytes(self.buffer[2:2 + width], "big")
                header += width
            if length > 60000 or (opcode >= 8 and length > 125):
                raise ValueError("probe wire audit frame exceeded bound")
            end = header + 4 + length
            if len(self.buffer) < end:
                return
            mask = self.buffer[header:header + 4]
            body = bytes(value ^ mask[index % 4] for index, value in enumerate(self.buffer[header + 4:end]))
            del self.buffer[:end]
            if opcode == 1:
                request = json.loads(body)
                if not isinstance(request, dict):
                    raise ValueError("probe wire audit expected JSON-RPC object")
                if request.get("method") == "send":
                    self.count()
