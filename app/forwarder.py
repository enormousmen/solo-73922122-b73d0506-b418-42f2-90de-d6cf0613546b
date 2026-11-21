"""Rate-limited forwarding of already-merged RTP datagrams.

The merge engine decides *which* packet is next; this module decides *when*
the original datagram may be handed to the playout socket. It uses a token
bucket charged in **bytes** (the length of the original RTP packet):

* ``rate_bps`` tokens are added per second; the bucket never holds more than
  ``burst_bytes``. Idle time or a late event-loop wake therefore cannot
  accumulate more than one burst of credit.
* A packet is handed to UDP only when the bucket holds at least its full
  length. Otherwise it waits in a FIFO so merge order and packet contents are
  preserved bit-for-bit. A later, smaller packet never bypasses the head.
* The FIFO is bounded by ``max_backlog_bytes``; a new packet that would not
  fit is dropped (a ``rate_limit_drops`` event, never a merge ``skip``).
  ``0`` selects the default bound: one second of rate credit, at least one
  burst -- always finite.
* A packet larger than ``burst_bytes`` can never be afforded and is dropped
  immediately.
* A timer wakes the loop exactly when the head packet becomes affordable, so
  the queue keeps draining even after the upstream stops sending. Ingress and
  gap timers are untouched while send credit is awaited.

Each new offer first drains the existing FIFO with whatever credit is
available, then decides whether the new packet can be sent at once or must be
queued. With ``rate_bps == 0`` the shaper is disabled and ``offer_packet``
sends synchronously, preserving the historical unlimited behaviour.
"""
from __future__ import annotations

import asyncio
from collections import deque
from typing import Optional, Tuple

# Tolerance for floating-point token arithmetic.
_EPSILON = 1e-9


class RateLimitedForwarder:
    def __init__(self, transport: asyncio.DatagramTransport,
                 target: Tuple[str, int], stats,
                 rate_bps: int = 0, burst_bytes: int = 0,
                 max_backlog_bytes: int = 0,
                 loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self._transport = transport
        self._target = target
        self._stats = stats
        self._loop = loop or asyncio.get_event_loop()

        self._rate = float(rate_bps)
        self._burst = float(max(burst_bytes, 0))
        # 0 means "auto": allow at most one second worth of send credit (but
        # never less than one burst) to wait, so pacing still queues by
        # default while remaining strictly bounded.
        if max_backlog_bytes > 0:
            self._max_backlog = max_backlog_bytes
        else:
            self._max_backlog = int(max(self._burst, self._rate))
        self._enabled = rate_bps > 0

        # Start with a full bucket: an idle relay may send one burst
        # immediately, but never more.
        self._tokens = self._burst if self._enabled else 0.0
        self._last_refill = self._loop.time()

        self._queue: deque[bytes] = deque()
        self._queued_bytes = 0
        self._timer: Optional[asyncio.TimerHandle] = None

    # ------------------------------------------------------------------ API

    def offer_packet(self, data: bytes) -> None:
        """Offer the next in-order packet for forwarding.

        Called by the merge engine in strict merge order. Existing backlog is
        drained first; only then is the new packet sent or queued.
        """
        if not self._enabled:
            self._send(data)
            return

        now = self._loop.time()
        self._refill(now)
        self._drain()

        size = len(data)
        if size > self._burst:
            # Can never become affordable: drop instead of stalling the FIFO
            # behind a permanently blocked head.
            self._stats.rate_limit_drops += 1
            self._reschedule()
            return

        if not self._queue and self._tokens + _EPSILON >= size:
            self._tokens -= size
            self._send(data)
            self._reschedule()
            return

        if self._queued_bytes + size > self._max_backlog:
            # No room to wait; drop this new packet only.
            self._stats.rate_limit_drops += 1
            self._reschedule()
            return

        self._queue.append(data)
        self._queued_bytes += size
        self._reschedule()

    def status(self) -> dict:
        return {
            "enabled": self._enabled,
            "rate_bps": int(self._rate),
            "burst_bytes": int(self._burst),
            "max_backlog_bytes": self._max_backlog,
            "backlog_bytes": self._queued_bytes,
            "backlog_packets": len(self._queue),
        }

    def close(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    # -------------------------------------------------------------- internals

    def _refill(self, now: float) -> None:
        elapsed = now - self._last_refill
        if elapsed > 0.0:
            self._tokens = min(self._burst,
                               self._tokens + elapsed * self._rate)
        self._last_refill = now

    def _drain(self) -> None:
        """Send queued packets from the head while current credit allows.

        The FIFO is never reordered: when the head does not fit, everything
        behind it waits as well.
        """
        while self._queue:
            size = len(self._queue[0])
            if self._tokens + _EPSILON < size:
                break
            data = self._queue.popleft()
            self._queued_bytes -= size
            self._tokens -= size
            self._send(data)
        self._reschedule()

    def _reschedule(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not self._queue:
            return
        need = len(self._queue[0]) - self._tokens
        # _drain() guarantees need > 0 here; the floor only protects against
        # float rounding causing a zero-delay busy loop.
        delay = max(need / self._rate, 1e-6)
        self._timer = self._loop.call_later(delay, self._on_timer)

    def _on_timer(self) -> None:
        self._timer = None
        if not self._enabled:
            return
        self._refill(self._loop.time())
        self._drain()

    def _send(self, data: bytes) -> None:
        """Hand a raw datagram to UDP; only success counts as forwarded."""
        try:
            self._transport.sendto(data, self._target)
        except Exception:  # pragma: no cover - defensive, never kill the loop
            self._stats.send_errors += 1
        else:
            self._stats.forwarded += 1
