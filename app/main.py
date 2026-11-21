"""Entry point: run the relay and the FastAPI server in one asyncio loop."""
from __future__ import annotations

import asyncio
import logging

import uvicorn

from .api import create_app
from .config import Settings
from .relay import RelayEngine
from .udp_transport import open_forwarder, open_ingress

logger = logging.getLogger("rtp-relay")


async def serve(settings: Settings) -> None:
    settings.validate()

    forward_transport = await open_forwarder()
    engine = RelayEngine(settings, forward_transport)

    ingress_a = await open_ingress(engine, settings.port_a, "A")
    ingress_b = await open_ingress(engine, settings.port_b, "B")
    logger.info("Ingress listening on UDP %d (path A) and %d (path B)",
                settings.port_a, settings.port_b)
    logger.info("Forwarding recovered stream to %s:%d (UDP)",
                settings.forward_host, settings.forward_port)

    app = create_app(engine)
    server = uvicorn.Config(app, host=settings.api_host, port=settings.api_port,
                            log_level="info", lifespan="off")
    server_task = asyncio.create_task(uvicorn.Server(server).serve())

    try:
        await server_task
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Shutting down")
        server_task.cancel()
        await engine.aclose()
        ingress_a.close()
        ingress_b.close()
        forward_transport.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    try:
        asyncio.run(serve(settings))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
