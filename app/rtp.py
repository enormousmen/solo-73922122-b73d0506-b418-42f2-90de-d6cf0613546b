"""Minimal RTP header helpers (RFC 3550).

We only need the 16-bit sequence number; everything else in the header is
forwarded untouched.
"""
from __future__ import annotations

RTP_VERSION = 2
SEQ_MODULUS = 1 << 16


def is_rtp_packet(data: bytes) -> bool:
    """Cheap structural check: version must be 2 and a full header present."""
    if len(data) < 12:
        return False
    return (data[0] >> 6) == RTP_VERSION


def sequence_number(data: bytes) -> int:
    """Extract the big-endian 16-bit RTP sequence number."""
    return (data[2] << 8) | data[3]


def build_packet(seq: int, payload_size: int = 160, timestamp_step: int = 160,
                 ssrc: int = 0x11223344) -> bytes:
    """Build a well-formed RTP packet for the demo/traffic generator."""
    header = bytearray(12)
    header[0] = 0x80  # V=2, no padding/extension/CSRC
    header[1] = 0x60  # PT=96, marker bit cleared
    header[2] = (seq >> 8) & 0xFF
    header[3] = seq & 0xFF
    ts = (seq * timestamp_step) & 0xFFFFFFFF
    header[4:8] = ts.to_bytes(4, "big")
    header[8:12] = ssrc.to_bytes(4, "big")
    # Deterministic payload echoing the sequence number, handy for sink checks.
    payload = bytes([seq & 0xFF]) * payload_size
    return bytes(header) + payload


def seq_delta(seq: int, base: int, modulus: int = SEQ_MODULUS) -> int:
    """Forward distance from ``base`` to ``seq`` on a wrapping sequence space.

    Returns a value in ``(-modulus/2, modulus/2]``; positive means ``seq`` is
    ahead of ``base``, negative means it is behind (stale).
    """
    half = modulus // 2
    return (seq - base + half) % modulus - half
