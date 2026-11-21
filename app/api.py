"""FastAPI query surface."""
from __future__ import annotations

from fastapi import FastAPI

from .relay import RelayEngine


def create_app(engine: RelayEngine) -> FastAPI:
    app = FastAPI(
        title="Dual-path RTP relay",
        description="双路 RTP 丢包补位中继：查询跳过序号与运行状态",
        version="1.0.0",
    )

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.get("/skipped")
    async def skipped() -> dict:
        """The most recent 64 skipped RTP sequence numbers, newest first."""
        return {"skipped": engine.recent_skipped()}

    @app.get("/status")
    async def status() -> dict:
        """Full runtime snapshot (counters, pending sequence, buffer size)."""
        return engine.status()

    return app
