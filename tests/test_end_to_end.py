"""End-to-end tests over real UDP sockets plus the live FastAPI server."""
from __future__ import annotations

import asyncio
import json
import socket
import unittest

import uvicorn

from app.api import create_app
from app.config import Settings
from app.relay import RelayEngine
from app.rtp import SEQ_MODULUS, build_packet, is_rtp_packet, sequence_number
from app.udp_transport import open_forwarder, open_ingress


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class SinkProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.seqs: list[int] = []
        self.packets: list[bytes] = []
        self.arrivals: list[tuple[float, int]] = []

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        if is_rtp_packet(data):
            seq = sequence_number(data)
            self.seqs.append(seq)
            self.packets.append(data)
            self.arrivals.append((asyncio.get_running_loop().time(), seq))


class EndToEndBase(unittest.IsolatedAsyncioTestCase):
    # Subclasses may add pacing settings (rate/burst/backlog).
    SETTINGS_OVERRIDES: dict = {}

    def make_settings(self, port_a: int, port_b: int,
                      fwd_port: int, api_port: int) -> Settings:
        return Settings(
            port_a=port_a, port_b=port_b,
            forward_host="127.0.0.1", forward_port=fwd_port,
            api_host="127.0.0.1", api_port=api_port,
            window=64, gap_timeout_ms=40,
            **self.SETTINGS_OVERRIDES,
        )

    async def asyncSetUp(self) -> None:
        port_a, port_b = _free_udp_port(), _free_udp_port()
        fwd_port = _free_udp_port()
        api_port = _free_tcp_port()
        self.settings = self.make_settings(port_a, port_b, fwd_port, api_port)
        self.settings.validate()

        self.forward_transport = await open_forwarder()
        self.engine = RelayEngine(self.settings, self.forward_transport)
        self.ingress_a = await open_ingress(self.engine, port_a, "A")
        self.ingress_b = await open_ingress(self.engine, port_b, "B")

        # Sink standing in for the playout machine.
        loop = asyncio.get_running_loop()
        self.sink = SinkProtocol()
        self.sink_transport, _ = await loop.create_datagram_endpoint(
            lambda: self.sink, local_addr=("127.0.0.1", fwd_port))

        # Live FastAPI server in the same event loop. NOTE: never use a
        # blocking HTTP client here -- it would stall the loop that uvicorn
        # needs to serve the request. Use the async _get() helper below.
        config = uvicorn.Config(create_app(self.engine),
                                host="127.0.0.1", port=api_port,
                                log_level="warning", lifespan="off")
        self.server = uvicorn.Server(config)
        self.server_task = asyncio.create_task(self.server.serve())
        self.api_base = ("127.0.0.1", api_port)
        for _ in range(50):
            try:
                self.assertEqual((await self._get("/health"))["status"], "ok")
                break
            except (OSError, asyncio.TimeoutError):
                await asyncio.sleep(0.02)
        else:
            raise RuntimeError("test API server did not become ready")

    async def _get(self, path: str) -> dict:
        """Minimal async HTTP/1.1 GET returning the parsed JSON body."""
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(*self.api_base), timeout=2)
        try:
            writer.write(f"GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n".encode())
            await writer.drain()
            raw = await asyncio.wait_for(reader.read(), timeout=2)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (OSError, ConnectionError):
                pass
        body = raw.split(b"\r\n\r\n", 1)[1]
        return json.loads(body)

    async def asyncTearDown(self) -> None:
        self.server_task.cancel()
        try:
            await self.server_task
        except asyncio.CancelledError:
            pass
        await self.engine.aclose()
        self.ingress_a.close()
        self.ingress_b.close()
        self.forward_transport.close()
        self.sink_transport.close()

    async def _send_path(self, port: int, packets: list[bytes]) -> None:
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol, remote_addr=("127.0.0.1", port))
        for data in packets:
            transport.sendto(data)
        # Give the loop a chance to deliver datagrams before closing.
        await asyncio.sleep(0.02)
        transport.close()


class EndToEndTest(EndToEndBase):
    async def test_cross_path_fill_in_no_skips(self) -> None:
        # Path A loses seq 2, path B has it; path B loses seq 4, path A has it.
        path_a = [build_packet(s) for s in range(0, 8) if s != 2]
        path_b = [build_packet(s) for s in range(0, 8) if s != 4]
        await asyncio.gather(
            self._send_path(self.settings.port_a, path_a),
            self._send_path(self.settings.port_b, path_b),
        )
        await asyncio.sleep(0.1)
        # The recovered output is complete and in order with zero skips.
        self.assertEqual(self.sink.seqs, list(range(0, 8)))
        self.assertEqual((await self._get("/skipped"))["skipped"], [])
        status = await self._get("/status")
        self.assertGreaterEqual(status["stats"]["gaps_recovered"], 1)
        self.assertEqual(status["stats"]["gaps_timed_out"], 0)
        # Both paths carried the overlapping programme: duplicates were seen
        # and suppressed rather than forwarded twice.
        self.assertGreater(status["stats"]["duplicates"], 0)
        self.assertEqual(status["stats"]["forwarded"], 8)

    async def test_both_paths_lose_same_packet_skips_after_wait(self) -> None:
        path_a = [build_packet(s) for s in range(0, 6) if s != 3]
        path_b = [build_packet(s) for s in range(0, 6) if s != 3]
        await asyncio.gather(
            self._send_path(self.settings.port_a, path_a),
            self._send_path(self.settings.port_b, path_b),
        )
        # Wait for the timer-driven skip (precise 40 ms timing is covered by
        # the unit tests; over real sockets we only assert a generous bound).
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 0.5
        while not (await self._get("/skipped"))["skipped"] and loop.time() < deadline:
            await asyncio.sleep(0.01)
        self.assertEqual(self.sink.seqs, [0, 1, 2, 4, 5])
        self.assertEqual((await self._get("/skipped"))["skipped"], [3])
        status = await self._get("/status")
        self.assertEqual(status["stats"]["gaps_timed_out"], 1)
        self.assertEqual(status["stats"]["gaps_recovered"], 0)

    async def test_silence_timeout_then_recovery_and_wrap(self) -> None:
        start = SEQ_MODULUS - 2
        seqs = [start, (start + 1) % SEQ_MODULUS, (start + 3) % SEQ_MODULUS,
                (start + 4) % SEQ_MODULUS]
        # Missing (start+2) == 1 on both paths, with total silence.
        await asyncio.gather(
            self._send_path(self.settings.port_a, [build_packet(s) for s in seqs]),
            self._send_path(self.settings.port_b, [build_packet(s) for s in seqs]),
        )
        await asyncio.sleep(0.15)
        self.assertEqual(self.sink.seqs, seqs)
        self.assertEqual((await self._get("/skipped"))["skipped"],
                         [(start + 2) % SEQ_MODULUS])

    async def test_window_limit_drops_far_ahead_packets(self) -> None:
        # Seq 0 is forwarded immediately, so the pending seq becomes 1.
        # Seq 65 is at modular distance 64 -> the 65th position -> rejected;
        # seq 63 (distance 62) must still be accepted into the window.
        packets = [build_packet(0), build_packet(63), build_packet(65)]
        await self._send_path(self.settings.port_a, packets)
        # Assert immediately, before the 40 ms gap timer anchored by seq 63
        # can fire: 63 must stay buffered behind the missing head seq 1.
        self.assertEqual(self.sink.seqs, [0])
        status = await self._get("/status")
        self.assertEqual(status["stats"]["out_of_window"], 1)
        self.assertEqual(status["buffered"], 1)
        self.assertTrue(status["gap_wait_active"])


class RateLimitedEndToEndTest(EndToEndBase):
    """Same real sockets/HTTP path, with post-merge byte pacing enabled."""

    # RTP packets here are 172 bytes (12-byte header + 160-byte payload).
    # 3440 B/s = 20 packets/s; a 344-byte burst allows two packets at once.
    SETTINGS_OVERRIDES = {"rate_bps": 3440, "burst_bytes": 344,
                          "max_backlog_bytes": 1720}

    async def _wait_until(self, predicate, timeout: float = 3.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not predicate():
            if loop.time() > deadline:
                self.fail("condition not met before timeout")
            await asyncio.sleep(0.02)

    async def test_paced_output_is_in_order_and_measured(self) -> None:
        # Ten in-order packets, delivered far faster than the send budget.
        packets = [build_packet(s) for s in range(10)]
        await self._send_path(self.settings.port_a, packets)

        # Some are sent at once, the rest wait in the bounded backlog.
        # Read the engine state directly (same loop) to avoid HTTP latency
        # skewing the timing-sensitive snapshot.
        status = self.engine.status()
        self.assertEqual(status["stats"]["forwarded"], 2)  # 344-byte burst
        self.assertEqual(status["backlog_bytes"], 8 * 172)
        self.assertEqual(status["backlog_packets"], 8)

        await self._wait_until(
            lambda: len(self.sink.seqs) == 10, timeout=3.0)
        # Arrival order is exactly merge order and contents were untouched.
        self.assertEqual(self.sink.seqs, list(range(10)))
        self.assertEqual([s for _, s in self.sink.arrivals],
                         list(range(10)))
        self.assertEqual(self.sink.packets,
                         [build_packet(s) for s in range(10)])

        # Rate sanity: 10 packets x 172 B = 1720 B; the first burst (344 B)
        # is instant, the remaining 1376 B take >= ~0.4 s at 3440 B/s.
        first_t = self.sink.arrivals[0][0]
        last_t = self.sink.arrivals[-1][0]
        self.assertGreaterEqual(last_t - first_t, 0.35)

        status = await self._get("/status")
        self.assertEqual(status["stats"]["forwarded"], 10)
        self.assertEqual(status["backlog_bytes"], 0)
        self.assertEqual(status["stats"]["rate_limit_drops"], 0)
        self.assertEqual(status["rate_limit_drops"], 0)
        # Pacing never creates merge skips or skipped-sequence records.
        self.assertEqual(status["stats"]["skipped"], 0)
        self.assertEqual((await self._get("/skipped"))["skipped"], [])

    async def test_gap_skip_runs_while_backlog_waits_for_credit(self) -> None:
        # 0..4 in order: 0,1 fit the burst, 2..4 queue.
        await self._send_path(self.settings.port_a,
                              [build_packet(s) for s in range(5)])
        # Seq 6 arrives while 2..4 are still waiting: seq 5 is a real gap.
        await self._send_path(self.settings.port_a, [build_packet(6)])

        # The 40 ms gap timer fires independently of send pacing, so the
        # merge-only skip list records 5 even though packets are still backed
        # up behind the token bucket.
        await self._wait_until(
            lambda: self.engine.recent_skipped() == [5], timeout=1.0)
        status = self.engine.status()
        self.assertEqual(status["stats"]["gaps_timed_out"], 1)
        self.assertEqual(status["stats"]["skipped"], 1)
        self.assertEqual(status["stats"]["rate_limit_drops"], 0)
        self.assertEqual(status["backlog_packets"], 3)  # 2,3,4 still waiting
        # The gap skip did not skip over the still-buffered numbers.
        self.assertEqual(status["next_seq"], 7)
        self.assertEqual((await self._get("/skipped"))["skipped"], [5])

        await self._wait_until(
            lambda: len(self.sink.seqs) == 6, timeout=3.0)
        self.assertEqual(self.sink.seqs, [0, 1, 2, 3, 4, 6])

    async def test_oversized_and_overflow_drops_do_not_touch_skips(self) -> None:
        # A 400-byte packet exceeds the 344-byte burst: dropped by the shaper
        # while the merge cursor still advances past it.
        await self._send_path(self.settings.port_a,
                              [build_packet(0, payload_size=388)])
        status = self.engine.status()
        self.assertEqual(status["stats"]["rate_limit_drops"], 1)
        self.assertEqual(status["stats"]["forwarded"], 0)
        self.assertEqual(status["stats"]["skipped"], 0)
        self.assertEqual(status["rate_limit_drops"], 1)

        # Offer 30 normal packets at once: at most two send on the burst and
        # at most ten (1720 B) may wait; the rest are dropped on arrival.
        await self._send_path(
            self.settings.port_a, [build_packet(s) for s in range(1, 31)])
        status = self.engine.status()
        self.assertEqual(status["backlog_bytes"], 1720)
        self.assertEqual(status["backlog_packets"], 10)
        self.assertEqual(status["stats"]["forwarded"], 2)
        self.assertEqual(status["stats"]["rate_limit_drops"], 1 + 18)
        self.assertEqual(status["stats"]["skipped"], 0)
        self.assertEqual((await self._get("/skipped"))["skipped"], [])

        # Everything buffered drains on its own, with no skip records.
        await self._wait_until(
            lambda: self.engine.status()["backlog_bytes"] == 0, timeout=3.0)
        self.assertEqual(self.engine.status()["stats"]["forwarded"], 12)
        self.assertEqual(self.engine.status()["stats"]["skipped"], 0)
        self.assertEqual((await self._get("/skipped"))["skipped"], [])
        # Output stayed in merge order; the oversized seq 0 is simply absent.
        self.assertEqual(self.sink.seqs, list(range(1, 13)))
        self.assertEqual(len(self.sink.seqs), 12)


if __name__ == "__main__":
    unittest.main()
