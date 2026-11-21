"""Runtime configuration loaded from environment variables.

All knobs have sensible defaults so ``python -m app.main`` works out of the
box for the local demo.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"environment variable {name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    # Two ingress UDP ports carrying the same programme.
    port_a: int
    port_b: int
    # Where the recovered, in-order stream is sent (the playout machine).
    forward_host: str
    forward_port: int
    # FastAPI control/query endpoint.
    api_host: str
    api_port: int
    # Only packets within this many positions of the pending sequence are kept.
    window: int
    # Gap wait budget, milliseconds, starting at the first later packet.
    gap_timeout_ms: int
    # Post-merge send pacing, charged in bytes of the original RTP packets.
    # rate_bps == 0 disables the shaper (unlimited, legacy behaviour).
    rate_bps: int = 0
    # Maximum send credit the bucket may hold (a single packet larger than
    # this is dropped).
    burst_bytes: int = 0
    # Maximum bytes allowed to wait for send credit; a new packet that does
    # not fit is dropped (counted separately from merge skips). 0 = derive a
    # finite default from the rate (one second of credit, at least a burst).
    max_backlog_bytes: int = 0
    # RTP sequence numbers are 16-bit.
    seq_modulus: int = 1 << 16

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            port_a=_env_int("UDP_PORT_A", 9100),
            port_b=_env_int("UDP_PORT_B", 9101),
            forward_host=os.environ.get("FORWARD_HOST", "127.0.0.1"),
            forward_port=_env_int("FORWARD_PORT", 9200),
            api_host=os.environ.get("API_HOST", "0.0.0.0"),
            api_port=_env_int("API_PORT", 8000),
            window=_env_int("RELAY_WINDOW", 64),
            gap_timeout_ms=_env_int("GAP_TIMEOUT_MS", 80),
            rate_bps=_env_int("RELAY_RATE_BPS", 0),
            burst_bytes=_env_int("RELAY_BURST_BYTES", 0),
            max_backlog_bytes=_env_int("RELAY_MAX_BACKLOG_BYTES", 0),
        )

    def validate(self) -> None:
        for name, value in {
            "UDP_PORT_A": self.port_a,
            "UDP_PORT_B": self.port_b,
            "FORWARD_PORT": self.forward_port,
            "API_PORT": self.api_port,
        }.items():
            if not 1 <= value <= 65535:
                raise ValueError(f"{name} must be in 1..65535, got {value}")
        if self.port_a == self.port_b:
            raise ValueError("UDP_PORT_A and UDP_PORT_B must be different")
        if self.window < 2 or self.window >= self.seq_modulus:
            raise ValueError("RELAY_WINDOW must be in 2..65535")
        if self.gap_timeout_ms <= 0:
            raise ValueError("GAP_TIMEOUT_MS must be positive")
        if self.rate_bps < 0:
            raise ValueError("RELAY_RATE_BPS must be >= 0 (0 disables pacing)")
        if self.burst_bytes < 0:
            raise ValueError("RELAY_BURST_BYTES must be >= 0")
        if self.max_backlog_bytes < 0:
            raise ValueError("RELAY_MAX_BACKLOG_BYTES must be >= 0")
        if self.rate_bps > 0:
            if self.burst_bytes <= 0:
                raise ValueError(
                    "RELAY_BURST_BYTES must be positive when RELAY_RATE_BPS > 0")
            # RELAY_MAX_BACKLOG_BYTES may be 0: the forwarder then derives a
            # finite bound of max(burst, one second of rate).
