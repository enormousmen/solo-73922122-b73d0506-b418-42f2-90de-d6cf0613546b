"""Tests for post-merge byte-rate pacing and its engine integration."""
from __future__ import annotations

import asyncio
import unittest

from app.config import Settings
from app.forwarder import RateLimitedForwarder
from app.relay import RelayEngine, RelayStats
from app.rtp import build_packet, sequence_number


class FakeTimerHandle:
    def __init__(self, loop: "FakeLoop", when: float, callback) -> None:
        self.loop = loop
        self.when = when
        self.callback = callback
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


class FakeLoop:
    """Minimal event-loop clock: only ``time`` and ``call_later`` are used."""

    def __init__(self) -> None:
        self.now = 1000.0
        self._timers: list[FakeTimerHandle] = []

    def time(self) -> float:
        return self.now

    def call_later(self, delay: float, callback) -> FakeTimerHandle:
        handle = FakeTimerHandle(self, self.now + delay, callback)
        self._timers.append(handle)
        return handle

    def advance(self, seconds: float) -> None:
        """Advance virtual time and run all timers that became due, in order."""
        target = self.now + seconds
        while True:
            due = sorted(
                (h for h in self._timers
                 if not h.cancelled and h.when <= target),
                key=lambda h: h.when,
            )
            if not due:
                break
            handle = due[0]
            self._timers.remove(handle)
            self.now = max(self.now, handle.when)
            handle.callback()
        self.now = target


class RecordingTransport:
    def __init__(self, fail: bool = False) -> None:
        self.sent: list[tuple[bytes, tuple]] = []
        self.fail = fail

    def sendto(self, data: bytes, addr: tuple) -> None:
        if self.fail:
            raise OSError("sink unreachable")
        self.sent.append((data, addr))


def stats() -> RelayStats:
    return RelayStats()


class TestRateLimitedForwarder(unittest.TestCase):
    def make(self, **kwargs) -> tuple[RateLimitedForwarder, RecordingTransport,
                                     RelayStats, FakeLoop]:
        rate = kwargs.pop("rate_bps", 1000)
        burst = kwargs.pop("burst_bytes", 300)
        backlog = kwargs.pop("max_backlog_bytes", 10000)
        loop = FakeLoop()
        transport = RecordingTransport(kwargs.pop("fail", False))
        st = stats()
        fwd = RateLimitedForwarder(transport, ("127.0.0.1", 9200), st,
                                   rate_bps=rate, burst_bytes=burst,
                                   max_backlog_bytes=backlog, loop=loop)
        return fwd, transport, st, loop

    def test_unlimited_mode_sends_immediately(self) -> None:
        fwd, transport, st, _ = self.make(rate_bps=0, burst_bytes=0,
                                          max_backlog_bytes=0)
        for i in range(5):
            fwd.offer_packet(bytes([i]) * 500)
        self.assertEqual(len(transport.sent), 5)
        self.assertEqual(st.forwarded, 5)
        self.assertEqual(st.rate_limit_drops, 0)
        self.assertEqual(fwd.status()["backlog_bytes"], 0)

    def test_burst_allowance_then_fifo_queue(self) -> None:
        fwd, transport, st, loop = self.make()
        for i in range(6):
            fwd.offer_packet(bytes([i]) * 100)
        # 300-byte burst covers exactly three packets.
        self.assertEqual(len(transport.sent), 3)
        self.assertEqual(fwd.status()["backlog_bytes"], 300)
        self.assertEqual(fwd.status()["backlog_packets"], 3)

        loop.advance(0.1)  # 100 tokens -> exactly one more packet
        self.assertEqual(len(transport.sent), 4)
        self.assertEqual([d[0] for d, _ in transport.sent], [0, 1, 2, 3])
        self.assertEqual(fwd.status()["backlog_bytes"], 200)

        loop.advance(0.2)  # 200 more tokens -> the remaining two
        self.assertEqual([d[0] for d, _ in transport.sent], [0, 1, 2, 3, 4, 5])
        self.assertEqual(fwd.status()["backlog_bytes"], 0)
        self.assertEqual(st.forwarded, 6)

    def test_packet_bytes_are_forwarded_verbatim(self) -> None:
        fwd, transport, _, _ = self.make(rate_bps=0, burst_bytes=0,
                                         max_backlog_bytes=0)
        packet = build_packet(4242, payload_size=333)
        fwd.offer_packet(packet)
        self.assertIs(transport.sent[0][0], packet)

    def test_idle_credit_is_capped_at_burst(self) -> None:
        fwd, transport, _, loop = self.make()
        loop.advance(100.0)  # would accrue 100000 bytes without the cap
        for i in range(5):
            fwd.offer_packet(bytes([i]) * 100)
        self.assertEqual(len(transport.sent), 3)  # exactly one burst
        self.assertEqual(fwd.status()["backlog_bytes"], 200)

    def test_late_wake_cannot_exceed_burst(self) -> None:
        fwd, transport, _, loop = self.make(rate_bps=500, burst_bytes=200,
                                            max_backlog_bytes=10000)
        loop.advance(60.0)  # event loop woke up very late
        fwd.offer_packet(bytes(200))
        fwd.offer_packet(bytes(200))
        self.assertEqual(len(transport.sent), 1)
        self.assertEqual(fwd.status()["backlog_bytes"], 200)

    def test_new_packet_queues_behind_remaining_backlog(self) -> None:
        fwd, transport, _, loop = self.make()
        for i in range(4):  # 3 sent, 1 queued, tokens == 0
            fwd.offer_packet(bytes([i]) * 100)
        loop.advance(0.05)  # 50 tokens: head (100 B) still not affordable
        fwd.offer_packet(bytes([9]) * 50)
        # The 50-byte newcomer fits in the remaining credit but must not jump
        # ahead of the 100-byte head, so it queues behind it.
        self.assertEqual(len(transport.sent), 3)
        self.assertEqual(fwd.status()["backlog_packets"], 2)
        self.assertEqual(fwd.status()["backlog_bytes"], 150)
        loop.advance(0.13)  # 130 -> 180 tokens: drain 100 then 50, in order
        self.assertEqual([d[0] for d, _ in transport.sent],
                         [0, 1, 2, 3, 9])
        self.assertEqual(fwd.status()["backlog_bytes"], 0)

    def test_backlog_drains_before_new_packet_decision(self) -> None:
        fwd, transport, _, loop = self.make()
        for i in range(4):  # 3 sent, one 100-byte packet queued
            fwd.offer_packet(bytes([i]) * 100)
        loop.advance(0.15)  # timer: 150 tokens -> head drains, 50 left
        self.assertEqual(len(transport.sent), 4)
        self.assertEqual(fwd.status()["backlog_bytes"], 0)
        # Now the backlog is empty and exactly enough credit remains for the
        # newcomer, so it is sent at once rather than queued.
        fwd.offer_packet(bytes([9]) * 50)
        self.assertEqual(len(transport.sent), 5)
        self.assertEqual([d[0] for d, _ in transport.sent], [0, 1, 2, 3, 9])
        self.assertEqual(fwd.status()["backlog_bytes"], 0)

    def test_oversized_packet_dropped_immediately(self) -> None:
        fwd, transport, st, _ = self.make(burst_bytes=200)
        fwd.offer_packet(bytes(201))
        self.assertEqual(st.rate_limit_drops, 1)
        self.assertEqual(len(transport.sent), 0)
        self.assertEqual(fwd.status()["backlog_bytes"], 0)
        # A normal packet afterwards is not blocked by the oversized one.
        fwd.offer_packet(bytes(100))
        self.assertEqual(len(transport.sent), 1)

    def test_backlog_full_drops_new_packet_only(self) -> None:
        fwd, transport, st, _ = self.make(rate_bps=100, burst_bytes=100,
                                          max_backlog_bytes=250)
        fwd.offer_packet(bytes([0]) * 100)  # sent (burst)
        fwd.offer_packet(bytes([1]) * 100)  # queued, 100
        fwd.offer_packet(bytes([2]) * 100)  # queued, 200
        fwd.offer_packet(bytes([3]) * 100)  # would be 300 > 250 -> drop
        self.assertEqual(st.rate_limit_drops, 1)
        self.assertEqual(fwd.status()["backlog_bytes"], 200)
        self.assertEqual([d[0] for d, _ in transport.sent], [0])

    def test_oversized_is_not_queued_even_with_room(self) -> None:
        fwd, transport, st, _ = self.make(burst_bytes=50,
                                          max_backlog_bytes=10000)
        fwd.offer_packet(bytes(60))
        self.assertEqual(st.rate_limit_drops, 1)
        self.assertEqual(transport.sent, [])
        self.assertEqual(fwd.status()["backlog_packets"], 0)

    def test_timer_drains_after_upstream_stops(self) -> None:
        fwd, transport, st, loop = self.make(rate_bps=1000, burst_bytes=100,
                                             max_backlog_bytes=10000)
        fwd.offer_packet(bytes([0]) * 100)  # sent
        fwd.offer_packet(bytes([1]) * 100)  # queued, no further input
        self.assertEqual(len(transport.sent), 1)
        loop.advance(0.099)
        self.assertEqual(len(transport.sent), 1)   # not affordable yet
        loop.advance(0.002)
        self.assertEqual(len(transport.sent), 2)   # timer-only drain
        self.assertEqual(st.forwarded, 2)
        self.assertEqual(fwd.status()["backlog_bytes"], 0)

    def test_send_error_counts_only_errors(self) -> None:
        fwd, _, st, _ = self.make(rate_bps=0, burst_bytes=0,
                                  max_backlog_bytes=0, fail=True)
        fwd.offer_packet(bytes(10))
        self.assertEqual(st.send_errors, 1)
        self.assertEqual(st.forwarded, 0)

    def test_close_cancels_pending_timer(self) -> None:
        fwd, transport, _, loop = self.make(burst_bytes=100)
        fwd.offer_packet(bytes(100))  # sent
        fwd.offer_packet(bytes(100))  # queued -> timer armed
        fwd.close()
        loop.advance(10.0)
        self.assertEqual(len(transport.sent), 1)


def make_settings(**overrides) -> Settings:
    base = dict(
        port_a=9100, port_b=9101,
        forward_host="127.0.0.1", forward_port=9999,
        api_host="127.0.0.1", api_port=8099,
        window=64, gap_timeout_ms=40,
    )
    base.update(overrides)
    return Settings(**base)


class TestEngineIntegration(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.transport = RecordingTransport()

    async def test_engine_paces_without_blocking_ingest_or_skips(self) -> None:
        settings = make_settings(rate_bps=1720, burst_bytes=172,
                                 max_backlog_bytes=10000)
        settings.validate()
        engine = RelayEngine(settings, self.transport)
        self.assertEqual(engine.status()["backlog_bytes"], 0)

        # In-order stream: seq 0 consumes the initial burst; 1 and 2 wait for
        # send credit while ingress keeps accepting.
        for seq in range(3):
            engine.handle_packet(build_packet(seq), "A")
        self.assertEqual(engine.stats.forwarded, 1)
        self.assertEqual(engine.status()["backlog_bytes"], 344)

        # A gap at 3 appears while 1 and 2 still wait for credit: the gap
        # timer must run independently and still skip on time.
        engine.handle_packet(build_packet(4), "A")
        self.assertTrue(engine.status()["gap_wait_active"])
        await asyncio.sleep(0.06)  # past the 40 ms gap budget
        self.assertEqual(engine.recent_skipped(), [3])
        self.assertEqual(engine.stats.skipped, 1)
        self.assertEqual(engine.stats.rate_limit_drops, 0)
        self.assertEqual(engine.status()["next_seq"], 5)

        await asyncio.sleep(0.4)  # pacing drains everything, in order
        self.assertEqual(
            [sequence_number(d) for d, _ in self.transport.sent], [0, 1, 2, 4])
        self.assertEqual(engine.stats.forwarded, 4)
        self.assertEqual(engine.status()["backlog_bytes"], 0)
        await engine.aclose()

    async def test_queue_drains_after_upstream_stops(self) -> None:
        settings = make_settings(rate_bps=1720, burst_bytes=172,
                                 max_backlog_bytes=10000)
        engine = RelayEngine(settings, self.transport)
        for seq in range(3):
            engine.handle_packet(build_packet(seq), "A")
        # One packet sent on the initial burst, two waiting for credit.
        self.assertEqual(engine.stats.forwarded, 1)
        self.assertEqual(engine.status()["backlog_packets"], 2)
        await asyncio.sleep(0.35)  # no further input at all
        self.assertEqual(
            [sequence_number(d) for d, _ in self.transport.sent], [0, 1, 2])
        self.assertEqual(engine.stats.forwarded, 3)
        self.assertEqual(engine.status()["backlog_bytes"], 0)
        await engine.aclose()

    async def test_rate_drop_is_not_a_merge_skip(self) -> None:
        settings = make_settings(rate_bps=1000, burst_bytes=172,
                                 max_backlog_bytes=10000)
        engine = RelayEngine(settings, self.transport)
        engine.handle_packet(build_packet(0), "A")  # sent
        # 300-byte packet exceeds the burst and is dropped by the shaper.
        engine.handle_packet(build_packet(1, payload_size=288), "A")
        engine.handle_packet(build_packet(2), "A")  # queued
        self.assertEqual(engine.stats.rate_limit_drops, 1)
        self.assertEqual(engine.stats.skipped, 0)
        self.assertEqual(engine.recent_skipped(), [])
        self.assertEqual(engine.status()["next_seq"], 3)
        self.assertEqual(engine.status()["backlog_bytes"], 172)
        await engine.aclose()

    async def test_status_exposes_backlog_and_rate_drops(self) -> None:
        settings = make_settings(rate_bps=100, burst_bytes=100,
                                 max_backlog_bytes=250)
        engine = RelayEngine(settings, self.transport)
        for seq in range(6):
            engine.handle_packet(build_packet(seq, payload_size=88), "A")
        status = engine.status()
        self.assertEqual(status["stats"]["rate_limit_drops"], 3)
        self.assertEqual(status["rate_limit_drops"], 3)
        self.assertEqual(status["backlog_bytes"], 200)
        self.assertEqual(status["backlog_packets"], 2)
        self.assertEqual(status["stats"]["skipped"], 0)
        self.assertEqual(status["stats"]["forwarded"], 1)
        await engine.aclose()


class TestSettingsValidation(unittest.TestCase):
    def test_disabled_by_default(self) -> None:
        make_settings().validate()

    def test_rate_requires_burst(self) -> None:
        with self.assertRaises(ValueError):
            make_settings(rate_bps=1000).validate()

    def test_backlog_must_cover_burst(self) -> None:
        # Zero is legal: the forwarder derives a finite default bound.
        make_settings(rate_bps=1000, burst_bytes=1000,
                      max_backlog_bytes=0).validate()
        with self.assertRaises(ValueError):
            make_settings(rate_bps=1000, burst_bytes=1000,
                          max_backlog_bytes=-1).validate()

    def test_auto_backlog_bound_is_finite(self) -> None:
        fwd, _, _, _ = TestRateLimitedForwarder().make(
            rate_bps=500, burst_bytes=200, max_backlog_bytes=0)
        # max(burst, rate) = 500 bytes.
        self.assertEqual(fwd.status()["max_backlog_bytes"], 500)

    def test_negative_values_rejected(self) -> None:
        for overrides in ({"rate_bps": -1},
                          {"burst_bytes": -1},
                          {"max_backlog_bytes": -1}):
            with self.assertRaises(ValueError):
                make_settings(**overrides).validate()


if __name__ == "__main__":
    unittest.main()
