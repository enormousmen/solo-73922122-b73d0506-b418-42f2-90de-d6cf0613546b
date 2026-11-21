"""Core dual-path merge relay.

Business rules (see README for the full spec):

* Two UDP paths carry the same programme. The first packet received anchors
  the stream: its sequence number becomes the first one to forward.
* RTP sequence numbers are 16-bit and may wrap around. All comparisons go
  through modular arithmetic.
* Duplicate packets (same sequence from the other path, or a retransmit) are
  dropped -- the first copy wins.
* Only packets whose forward distance from the pending sequence is within
  ``window`` positions (default 64) are accepted; anything further ahead is
  dropped as it would require buffering beyond the allowed window.
* When a gap appears, the relay waits ``gap_timeout_ms`` (80 ms). The wait is
  anchored to the arrival of the *first later packet*; later arrivals never
  extend it. If both paths stay silent, the timer still fires, the missing
  number is skipped and forwarding continues.
* Skipped sequence numbers are retained (newest first, last 64) and exposed
  through FastAPI.
* Merged packets are handed to :class:`RateLimitedForwarder`, which applies
  the configurable byte-rate token bucket *after* merging, in merge order,
  without touching packet contents. Merge progress (sequence numbers, gap
  waits, skips) is independent of send pacing: waiting for send credit never
  stops ingress or gap handling.

The engine is entirely single-threaded: every method runs inside one asyncio
event loop, so plain containers need no locking.
"""
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from .forwarder import RateLimitedForwarder
from .rtp import is_rtp_packet, seq_delta, sequence_number


@dataclass
class RelayStats:
    received: int = 0            # valid RTP packets offered to the engine
    duplicates: int = 0          # packets whose seq was already buffered/sent
    out_of_window: int = 0       # packets beyond the 64-position window
    invalid: int = 0             # non-RTP datagrams
    forwarded: int = 0           # packets successfully handed to UDP
    skipped: int = 0
    gaps_recovered: int = 0      # gaps closed by a copy from either path
    gaps_timed_out: int = 0      # gaps that ended in a skip
    send_errors: int = 0
    rate_limit_drops: int = 0    # dropped because no send credit/backlog room
    path_received: dict = field(default_factory=lambda: {"A": 0, "B": 0})


class RelayEngine:
    def __init__(self, settings, forward_transport: asyncio.DatagramTransport,
                 loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self._loop = loop or asyncio.get_event_loop()
        self._modulus = settings.seq_modulus
        self._window = settings.window
        self._timeout = settings.gap_timeout_ms / 1000.0

        # seq -> (raw packet, monotonic arrival time)
        self._buffer: dict[int, tuple[bytes, float]] = {}
        self._next_seq: Optional[int] = None
        self._gap_deadline: Optional[float] = None
        self._gap_timer: Optional[asyncio.TimerHandle] = None
        self._gap_active = False  # a gap wait is currently in progress

        # Bounded deque keeps only the most recent 64 skipped numbers,
        # newest first.
        self._skipped: deque[int] = deque(maxlen=64)
        self.stats = RelayStats()

        self._forwarder = RateLimitedForwarder(
            forward_transport,
            (settings.forward_host, settings.forward_port),
            self.stats,
            rate_bps=settings.rate_bps,
            burst_bytes=settings.burst_bytes,
            max_backlog_bytes=settings.max_backlog_bytes,
            loop=self._loop,
        )

    # ------------------------------------------------------------------ API

    def handle_packet(self, data: bytes, path: str) -> None:
        """Entry point used by both UDP ingress transports."""
        if not is_rtp_packet(data):
            self.stats.invalid += 1
            return
        seq = sequence_number(data)
        self.stats.received += 1
        self.stats.path_received[path] = self.stats.path_received.get(path, 0) + 1

        # First packet anchors the stream and is forwarded immediately.
        if self._next_seq is None:
            self._next_seq = seq
            self._deliver(seq, data)
            self._pump()
            return

        distance = seq_delta(seq, self._next_seq, self._modulus)
        if distance < 0:
            # Already sent, or a stale copy behind the pending sequence.
            self.stats.duplicates += 1
            return
        if distance >= self._window:
            # Further ahead than the allowed 64 positions.
            self.stats.out_of_window += 1
            return
        if seq in self._buffer:
            self.stats.duplicates += 1
            return

        self._buffer[seq] = (data, self._loop.time())
        self._pump()

    def recent_skipped(self) -> list[int]:
        return list(self._skipped)

    def status(self) -> dict:
        status = {
            "next_seq": self._next_seq,
            "buffered": len(self._buffer),
            "gap_wait_active": self._gap_active,
            "gap_deadline_in_ms": (
                round((self._gap_deadline - self._loop.time()) * 1000, 1)
                if self._gap_deadline is not None else None
            ),
            "recent_skipped": self.recent_skipped(),
            # Convenience mirror of stats.rate_limit_drops next to backlog.
            "rate_limit_drops": self.stats.rate_limit_drops,
            "stats": self.stats.__dict__.copy(),
        }
        status.update(self._forwarder.status())
        return status

    async def aclose(self) -> None:
        self._cancel_timer()
        self._gap_active = False
        self._forwarder.close()

    # -------------------------------------------------------------- internals

    def _deliver(self, seq: int, data: bytes) -> None:
        """Hand an in-order merged packet to the pacing forwarder.

        The pending sequence advances immediately: pacing decisions belong to
        the forwarder and must never stall dedup, gap handling or ingress.
        """
        try:
            self._forwarder.offer_packet(data)
        except Exception:  # pragma: no cover - defensive, never kill the loop
            self.stats.send_errors += 1
        finally:
            self._next_seq = (seq + 1) % self._modulus

    def _pump(self) -> None:
        """Forward everything available from the pending sequence upward,
        then (re)arm the gap wait when the head is missing."""
        while self._next_seq in self._buffer:
            if self._gap_active:
                # The missing head arrived before its deadline: the other
                # path (or a retransmit) filled the gap.
                self.stats.gaps_recovered += 1
                self._gap_active = False
            data, _ = self._buffer.pop(self._next_seq)
            self._deliver(self._next_seq, data)

        if self._buffer:
            # Head is missing and later packets exist. Anchor the wait to the
            # earliest arrival; later packets never move the deadline.
            earliest = min(arrival for _, arrival in self._buffer.values())
            self._reschedule(earliest + self._timeout)
            self._gap_active = True
        else:
            # Nothing pending. This also cancels a timer whose gap was just
            # resolved, so the callback cannot fire and skip a live number.
            self._cancel_timer()
            self._gap_active = False

    def _reschedule(self, deadline: float) -> None:
        if self._gap_timer is not None:
            self._gap_timer.cancel()
        self._gap_deadline = deadline
        delay = max(0.0, deadline - self._loop.time())
        self._gap_timer = self._loop.call_later(delay, self._on_gap_timeout)

    def _on_gap_timeout(self) -> None:
        self._gap_timer = None
        self._gap_deadline = None

        if self._next_seq is not None and self._next_seq in self._buffer:
            # Filled at the last moment before the callback ran.
            self._pump()
            return

        self._gap_active = False
        self.stats.gaps_timed_out += 1
        # Skip every contiguous missing number up to the first buffered one.
        # With no buffered packets at all (both paths silent) this skips the
        # single missing number and the engine goes back to idle, ready to
        # keep pumping when the next packet arrives.
        while self._next_seq not in self._buffer:
            self._skipped.appendleft(self._next_seq)
            self.stats.skipped += 1
            self._next_seq = (self._next_seq + 1) % self._modulus
            if not self._buffer:
                break
        self._pump()

    def _cancel_timer(self) -> None:
        if self._gap_timer is not None:
            self._gap_timer.cancel()
            self._gap_timer = None
        self._gap_deadline = None
