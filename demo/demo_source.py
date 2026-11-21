"""Demo traffic generator.

Sends the *same* RTP programme out two UDP sockets (path A / path B) with
independent loss patterns, so the relay can demonstrate cross-path fill-in.

Examples
--------
# Path A loses seq 10 and 20, path B loses seq 15; every packet present on
# at least one path -> relay output has no skips.
python demo/demo_source.py --duration 3 --loss-a 10,20 --loss-b 15

# Both paths lose seq 30 -> relay waits 80 ms, then skips it.
python demo/demo_source.py --duration 3 --loss-a 30 --loss-b 30

# Path A loses seq 40, path B's copy is 40 ms late (still inside the 80 ms
# budget) -> the gap is recovered.
python demo/demo_source.py --duration 3 --loss-a 40 --late-b 40:0.04

# Both paths go silent for 300 ms after seq 50 -> the relay still skips the
# gap on time and keeps forwarding afterwards.
python demo/demo_source.py --duration 4 --silence-after 50 --silence-ms 300

# Send 10 packets back-to-back at seq 60 (emulating a gap-fill burst), while
# the relay's byte-rate shaper smooths them out for the playout machine.
python demo/demo_source.py --duration 4 --burst-at 60 --burst-packets 10
"""
from __future__ import annotations

import argparse
import asyncio
import random
import sys

from app.rtp import build_packet


def _parse_loss(raw: str) -> set[int]:
    if not raw:
        return set()
    out = set()
    for item in raw.split(","):
        item = item.strip()
        if item:
            out.add(int(item))
    return out


def _parse_late(raw: str) -> dict[int, float]:
    """Parse ``seq:delay_seconds`` pairs, comma separated."""
    out: dict[int, float] = {}
    if not raw:
        return out
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        seq, delay = item.split(":")
        out[int(seq)] = float(delay)
    return out


class DemoSource:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.loss_a = _parse_loss(args.loss_a)
        self.loss_b = _parse_loss(args.loss_b)
        self.late_b = _parse_late(args.late_b)
        self.rng = random.Random(args.seed)
        self.sent_a = 0
        self.sent_b = 0
        self._transport_a: asyncio.DatagramTransport | None = None
        self._transport_b: asyncio.DatagramTransport | None = None

    def _maybe_random_drop(self, path: str) -> bool:
        rate = self.args.random_loss_a if path == "A" else self.args.random_loss_b
        return rate > 0 and self.rng.random() < rate

    async def _send_one(self, transport: asyncio.DatagramTransport,
                        seq: int, addr: tuple[str, int]) -> None:
        transport.sendto(build_packet(seq), addr)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        self._transport_a, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol, remote_addr=(self.args.host, self.args.port_a))
        self._transport_b, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol, remote_addr=(self.args.host, self.args.port_b))

        interval = 1.0 / self.args.packets_per_sec
        start = loop.time()
        deadline = start + self.args.duration
        seq = self.args.start_seq
        pending: list[asyncio.Task] = []
        try:
            while loop.time() < deadline:
                # Optional full silence window: stop sending on both paths.
                if (self.args.silence_after is not None
                        and seq == self.args.silence_after):
                    await asyncio.sleep(self.args.silence_ms / 1000.0)

                # Optional instant burst: send several packets at once so the
                # relay's post-merge shaper has something to smooth.
                if (self.args.burst_at is not None
                        and seq == self.args.burst_at):
                    for offset in range(self.args.burst_packets):
                        burst_seq = (seq + offset) & 0xFFFF
                        self._send_now(self._transport_a, burst_seq,
                                       self.args.port_a)
                        self._send_now(self._transport_b, burst_seq,
                                       self.args.port_b)
                    seq = (seq + self.args.burst_packets) & 0xFFFF
                    continue

                packet_time = loop.time()
                if seq not in self.loss_a and not self._maybe_random_drop("A"):
                    await self._send_one(self._transport_a, seq,
                                         (self.args.host, self.args.port_a))
                    self.sent_a += 1
                if seq not in self.loss_b and not self._maybe_random_drop("B"):
                    delay = self.late_b.get(seq, 0.0)
                    if delay > 0:
                        pending.append(asyncio.create_task(
                            self._late_send(self._transport_b, seq,
                                            (self.args.host, self.args.port_b),
                                            delay)))
                    else:
                        await self._send_one(self._transport_b, seq,
                                             (self.args.host, self.args.port_b))
                        self.sent_b += 1
                seq = (seq + 1) & 0xFFFF

                next_at = packet_time + interval
                wait = next_at - loop.time()
                if wait > 0:
                    await asyncio.sleep(wait)
        finally:
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            self._transport_a.close()
            self._transport_b.close()
            print(f"[source] done: path A sent {self.sent_a}, "
                  f"path B sent {self.sent_b}, next seq would be {seq}",
                  file=sys.stderr)

    def _send_now(self, transport: asyncio.DatagramTransport,
                  seq: int, port: int) -> None:
        """Fire one packet on one path without awaiting."""
        transport.sendto(build_packet(seq), (self.args.host, port))
        if port == self.args.port_a:
            self.sent_a += 1
        else:
            self.sent_b += 1

    async def _late_send(self, transport: asyncio.DatagramTransport,
                         seq: int, addr: tuple[str, int], delay: float) -> None:
        await asyncio.sleep(delay)
        transport.sendto(build_packet(seq), addr)
        self.sent_b += 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Dual-path RTP demo traffic generator")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port-a", type=int, default=9100)
    p.add_argument("--port-b", type=int, default=9101)
    p.add_argument("--duration", type=float, default=3.0,
                   help="stream duration in seconds")
    p.add_argument("--packets-per-sec", type=int, default=50,
                   help="RTP packet rate per path (default 50 pps = 20 ms spacing)")
    p.add_argument("--start-seq", type=int, default=0)
    p.add_argument("--loss-a", default="", help="comma-separated seqs dropped on A")
    p.add_argument("--loss-b", default="", help="comma-separated seqs dropped on B")
    p.add_argument("--random-loss-a", type=float, default=0.0)
    p.add_argument("--random-loss-b", type=float, default=0.0)
    p.add_argument("--late-b", default="",
                   help="comma-separated seq:delay_seconds pairs delayed on path B")
    p.add_argument("--silence-after", type=int, default=None,
                   help="seq at which both paths go silent for --silence-ms")
    p.add_argument("--silence-ms", type=float, default=300.0)
    p.add_argument("--burst-at", type=int, default=None,
                   help="seq at which to emit an instant back-to-back burst")
    p.add_argument("--burst-packets", type=int, default=10,
                   help="packets sent at once by --burst-at (default 10)")
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args(argv)


if __name__ == "__main__":
    asyncio.run(DemoSource(parse_args()).run())
