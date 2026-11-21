"""Unit tests for the RelayEngine: windowing, dedup, gap waits, wrap-around."""
from __future__ import annotations

import asyncio
import unittest

from app.config import Settings
from app.relay import RelayEngine
from app.rtp import SEQ_MODULUS, build_packet, seq_delta


def make_settings(**overrides) -> Settings:
    base = dict(
        port_a=9100, port_b=9101,
        forward_host="127.0.0.1", forward_port=9999,
        api_host="127.0.0.1", api_port=8099,
        window=64, gap_timeout_ms=40,  # short timeout keeps the suite fast
    )
    base.update(overrides)
    return Settings(**base)


class RecordingTransport:
    """Stand-in for asyncio.DatagramTransport recording sent packets."""

    def __init__(self) -> None:
        self.sent: list[tuple[bytes, tuple]] = []

    def sendto(self, data: bytes, addr: tuple) -> None:
        self.sent.append((data, addr))

    def close(self) -> None:
        pass


class EngineTestBase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.settings = make_settings()
        self.transport = RecordingTransport()

    def make_engine(self) -> RelayEngine:
        return RelayEngine(self.settings, self.transport)

    def feed(self, engine: RelayEngine, seq: int, path: str = "A") -> None:
        engine.handle_packet(build_packet(seq), path)

    def forwarded_seqs(self) -> list[int]:
        from app.rtp import sequence_number
        return [sequence_number(data) for data, _ in self.transport.sent]


class TestBasicForwarding(EngineTestBase):
    async def test_first_packet_anchors_and_forwards(self) -> None:
        engine = self.make_engine()
        self.feed(engine, 100)
        self.assertEqual(self.forwarded_seqs(), [100])
        self.assertEqual(engine.status()["next_seq"], 101)
        await engine.aclose()

    async def test_in_order_stream(self) -> None:
        engine = self.make_engine()
        for seq in range(10, 20):
            self.feed(engine, seq)
        self.assertEqual(self.forwarded_seqs(), list(range(10, 20)))
        await engine.aclose()

    async def test_reordering_is_sorted(self) -> None:
        engine = self.make_engine()
        self.feed(engine, 0)
        for seq in (2, 3, 1):  # gap at 1, filled out of order
            self.feed(engine, seq)
        self.assertEqual(self.forwarded_seqs(), [0, 1, 2, 3])
        await engine.aclose()


class TestDedupAndWindow(EngineTestBase):
    async def test_duplicate_from_other_path_first_copy_wins(self) -> None:
        engine = self.make_engine()
        self.feed(engine, 5, "A")
        first_packet = self.transport.sent[-1][0]
        # Same sequence from path B before the head moves.
        self.feed(engine, 5, "B")
        self.assertEqual(engine.stats.duplicates, 1)
        self.assertIs(self.transport.sent[-1][0], first_packet)

        # Late duplicate after the number was already forwarded.
        self.feed(engine, 6)
        self.feed(engine, 5, "B")
        self.assertEqual(engine.stats.duplicates, 2)
        self.assertEqual(self.forwarded_seqs(), [5, 6])
        await engine.aclose()

    async def test_window_accepts_64_positions(self) -> None:
        engine = self.make_engine()
        self.feed(engine, 0)
        # Hold seq 1 back so everything else buffers behind the head gap.
        # Pending seq is 1; positions 1..64 are the 64 allowed slots, so
        # seqs 2..64 (distance 1..63) must be accepted and seq 65 rejected.
        for seq in range(2, 65):
            self.feed(engine, seq)
        self.assertEqual(engine.status()["buffered"], 63)
        self.assertEqual(engine.stats.out_of_window, 0)
        # Distance 64 from the pending seq is the 65th position -> rejected.
        self.feed(engine, 65)
        self.assertEqual(engine.stats.out_of_window, 1)
        await engine.aclose()

    async def test_window_uses_wrapping_distance(self) -> None:
        engine = self.make_engine()
        near_wrap = SEQ_MODULUS - 5
        self.feed(engine, near_wrap)
        # A packet from the "old" numerical range but just ahead modulo 2^16.
        self.feed(engine, 3)  # distance 8 -> accepted
        self.assertEqual(engine.stats.out_of_window, 0)
        self.assertEqual(engine.status()["buffered"], 1)
        # A genuinely stale packet far behind is a duplicate, not accepted.
        self.feed(engine, near_wrap - 100)
        self.assertEqual(engine.stats.duplicates, 1)
        await engine.aclose()

    async def test_stale_packets_count_as_duplicates(self) -> None:
        engine = self.make_engine()
        self.feed(engine, 10)
        self.feed(engine, 11)
        self.feed(engine, 9)  # behind pending seq 12
        self.assertEqual(engine.stats.duplicates, 1)
        await engine.aclose()

    async def test_invalid_datagram_is_counted(self) -> None:
        engine = self.make_engine()
        engine.handle_packet(b"not-rtp", "A")
        self.assertEqual(engine.stats.invalid, 1)
        self.assertEqual(engine.stats.received, 0)
        await engine.aclose()


class TestGapTimeout(EngineTestBase):
    async def test_gap_filled_before_timeout_is_recovered(self) -> None:
        engine = self.make_engine()
        self.feed(engine, 0)
        self.feed(engine, 2)  # opens gap at 1, timer armed
        self.assertTrue(engine.status()["gap_wait_active"])
        self.feed(engine, 1, "B")  # other path fills it in time
        self.assertEqual(self.forwarded_seqs(), [0, 1, 2])
        self.assertEqual(engine.stats.gaps_recovered, 1)
        self.assertEqual(engine.stats.skipped, 0)
        self.assertFalse(engine.status()["gap_wait_active"])
        await engine.aclose()

    async def test_gap_times_out_and_skips(self) -> None:
        engine = self.make_engine()
        self.feed(engine, 0)
        self.feed(engine, 2)  # first later packet anchors the wait
        loop = asyncio.get_running_loop()
        start = loop.time()
        # Wait until the timer-driven skip happens, measuring *when*.
        while not engine.recent_skipped():
            await asyncio.sleep(0.005)
            self.assertLess(loop.time() - start, 0.5,
                            "skip never happened without further input")
        elapsed = loop.time() - start
        self.assertEqual(self.forwarded_seqs(), [0, 2])
        self.assertEqual(engine.recent_skipped(), [1])
        self.assertEqual(engine.stats.gaps_timed_out, 1)
        # Fires at the configured ~40 ms budget (with scheduler slack),
        # proving the wait is timer-driven rather than input-driven.
        self.assertGreaterEqual(elapsed, 0.035)
        self.assertLess(elapsed, 0.15)
        await engine.aclose()

    async def test_later_arrivals_do_not_extend_wait(self) -> None:
        engine = self.make_engine()
        self.feed(engine, 0)
        self.feed(engine, 2)  # anchors deadline at t0 + 40ms
        await asyncio.sleep(0.025)
        self.feed(engine, 3)  # must NOT push the deadline to t0+65ms
        await asyncio.sleep(0.03)  # total ~55ms: original deadline passed
        self.assertIn(1, engine.recent_skipped())
        self.assertEqual(self.forwarded_seqs(), [0, 2, 3])
        await engine.aclose()

    async def test_silence_still_skips_on_time(self) -> None:
        # No buffered later packet at all: nothing to wait for, but once a
        # later packet finally arrives after silence the engine keeps going.
        engine = self.make_engine()
        self.feed(engine, 0)
        self.feed(engine, 2)
        # Both paths silent: the timer must fire without any new input.
        await asyncio.sleep(0.1)
        self.assertEqual(engine.recent_skipped(), [1])
        self.feed(engine, 3)
        self.assertEqual(self.forwarded_seqs(), [0, 2, 3])
        await engine.aclose()

    async def test_timeout_skipped_when_head_arrives_at_last_moment(self) -> None:
        engine = self.make_engine()
        self.feed(engine, 0)
        self.feed(engine, 2)
        await asyncio.sleep(0.03)  # just under the 40 ms timeout
        self.feed(engine, 1, "B")
        await asyncio.sleep(0.08)  # let the old deadline pass
        self.assertEqual(engine.recent_skipped(), [])
        self.assertEqual(self.forwarded_seqs(), [0, 1, 2])
        await engine.aclose()

    async def test_skip_list_keeps_last_64_newest_first(self) -> None:
        engine = self.make_engine()
        self.feed(engine, 0)
        # Produce 100 genuine single-packet gaps over several timeout cycles
        # (the 64-position window prevents injecting them all at once):
        # feed k+2 while k+1 is missing, let the 40 ms timer skip it, repeat.
        anchor = 2
        for _ in range(100):
            self.feed(engine, anchor)
            await asyncio.sleep(0.05)  # let the timer skip anchor-1
            anchor += 2
        skipped = engine.recent_skipped()
        self.assertEqual(len(skipped), 64)  # bounded deque
        self.assertEqual(engine.stats.skipped, 100)  # counter is unbounded
        # Newest first: the last skipped number is anchor-3 = 199.
        self.assertEqual(skipped[0], 199)
        self.assertEqual(skipped[-1], 199 - 2 * 63)
        await engine.aclose()


class TestWrapAround(unittest.TestCase):
    def test_seq_delta_math(self) -> None:
        self.assertEqual(seq_delta(5, 0), 5)
        self.assertEqual(seq_delta(0, SEQ_MODULUS - 1), 1)
        self.assertEqual(seq_delta(SEQ_MODULUS - 2, 1), -3)
        self.assertEqual(seq_delta(10, 10), 0)


class TestWrapAroundFlow(EngineTestBase):
    async def test_stream_crosses_65535(self) -> None:
        engine = self.make_engine()
        start = SEQ_MODULUS - 3
        for offset in range(7):  # -3,-2,-1,0,1,2,3 in seq space
            self.feed(engine, (start + offset) % SEQ_MODULUS)
        self.assertEqual(
            self.forwarded_seqs(),
            [(start + offset) % SEQ_MODULUS for offset in range(7)],
        )

    async def test_gap_and_skip_wrap_correctly(self) -> None:
        engine = self.make_engine()
        last = SEQ_MODULUS - 1
        self.feed(engine, last)
        self.feed(engine, 1)  # gap at seq 0 across the wrap
        await asyncio.sleep(0.1)
        self.assertEqual(engine.recent_skipped(), [0])
        self.assertEqual(self.forwarded_seqs(), [last, 1])
        await engine.aclose()


if __name__ == "__main__":
    unittest.main()
