"""Demo receiver standing in for the playout machine.

Binds the relay's forward address, records the recovered in-order stream and
reports:

* packet count and sequence range,
* gaps (numbers never received),
* out-of-order arrivals (should never happen for the relay output),
* the relay's own skip list fetched from its FastAPI ``/skipped`` endpoint,
  for cross-checking.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import urllib.request

from app.rtp import is_rtp_packet, seq_delta, sequence_number


class Sink:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.seqs: list[int] = []
        self.out_of_order: list[int] = []

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        if not is_rtp_packet(data):
            return
        seq = sequence_number(data)
        if self.seqs and seq_delta(seq, self.seqs[-1]) <= 0:
            self.out_of_order.append(seq)
        self.seqs.append(seq)

    async def run(self) -> dict:
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: _SinkProtocol(self),
            local_addr=(self.args.host, self.args.port),
        )
        try:
            await asyncio.sleep(self.args.duration)
        finally:
            transport.close()

        gaps = self._compute_gaps()
        report = {
            "received": len(self.seqs),
            "first_seq": self.seqs[0] if self.seqs else None,
            "last_seq": self.seqs[-1] if self.seqs else None,
            "gaps": gaps,
            "out_of_order": self.out_of_order,
        }
        if self.args.api_url:
            try:
                with urllib.request.urlopen(
                        self.args.api_url.rstrip("/") + "/skipped",
                        timeout=2) as resp:
                    report["relay_skipped"] = json.load(resp)["skipped"]
            except Exception as exc:  # demo tool: report, don't crash
                report["relay_skipped_error"] = repr(exc)
        print(json.dumps(report, indent=2))
        return report

    def _compute_gaps(self) -> list[int]:
        if len(self.seqs) < 2:
            return []
        gaps: list[int] = []
        ordered = sorted(self.seqs)
        for prev, cur in zip(ordered, ordered[1:]):
            distance = seq_delta(cur, prev)
            for missing in range(1, distance):
                gaps.append((prev + missing) & 0xFFFF)
        return gaps


class _SinkProtocol(asyncio.DatagramProtocol):
    def __init__(self, sink: Sink) -> None:
        self._sink = sink

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        self._sink.datagram_received(data, addr)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Demo playout-side receiver")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9200)
    p.add_argument("--duration", type=float, default=4.0)
    p.add_argument("--api-url", default="http://127.0.0.1:8000",
                   help="relay FastAPI base URL for /skipped cross-check")
    return p.parse_args(argv)


if __name__ == "__main__":
    asyncio.run(Sink(parse_args()).run())
