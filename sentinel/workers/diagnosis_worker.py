"""Diagnosis worker — Kafka consumer for `incident.enriched`.

Stub for Work Area A. Real consumer lands in Work Area G/I.
The entrypoint exists so docker-compose can declare a `worker` service
that boots cleanly and stays alive.
"""

from __future__ import annotations

import asyncio
import signal
import sys


async def _main() -> None:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _shutdown() -> None:
        stop.set()

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown)

    sys.stdout.write("sentinel-worker: idle (stub) — waiting for shutdown\n")
    sys.stdout.flush()
    await stop.wait()


def run() -> None:
    asyncio.run(_main())


if __name__ == "__main__":  # pragma: no cover
    run()
