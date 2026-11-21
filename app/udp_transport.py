"""UDP ingress transport: two ports feeding one :class:`RelayEngine`."""
from __future__ import annotations

import asyncio
from typing import Optional

from .relay import RelayEngine


class UdpIngressProtocol(asyncio.DatagramProtocol):
    def __init__(self, engine: RelayEngine, path: str) -> None:
        self._engine = engine
        self._path = path
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        self._engine.handle_packet(data, self._path)

    def error_received(self, exc: Exception) -> None:
        # ICMP port-unreachable etc. must not kill the relay; just log once
        # the application logger is wired up.
        import logging
        logging.getLogger("rtp-relay").warning(
            "UDP error on path %s: %r", self._path, exc)


async def open_ingress(engine: RelayEngine, port: int, path: str,
                       host: str = "0.0.0.0") -> asyncio.DatagramTransport:
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: UdpIngressProtocol(engine, path),
        local_addr=(host, port),
    )
    return transport


async def open_forwarder() -> asyncio.DatagramTransport:
    """Unconnected egress socket used to send recovered packets onward.

    Bound to an ephemeral local port so the event loop can determine the
    address family; it is still unconnected and may sendto any IPv4 target.
    """
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        asyncio.DatagramProtocol,
        local_addr=("0.0.0.0", 0),
    )
    return transport
