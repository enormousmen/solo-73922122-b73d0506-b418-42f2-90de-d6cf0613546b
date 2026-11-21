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

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        if is_rtp_packet(data):
            self.seqs.append(sequence_number(data))


class EndToEndTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        port_a, port_b = _free_udp_port(), _free_udp_port()
        fwd_port = _free_udp_port()
        api_port = _free_tcp_port()
        self.settings = Settings(
            port_a=port_a, port_b=port_b,
            forward_host="127.0.0.1", forward_port=fwd_port,
            api_host="127.0.0.1", api_port=api_port,
            window=64, gap_timeout_ms=40,
        )
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


if __name__ == "__main__":
    unittest.main()
