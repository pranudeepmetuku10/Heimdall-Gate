"""Entrypoint for the ingestion service.

Usage:
    python -m ingest                 # loop mode (default)
    python -m ingest --once          # one sweep then exit
    python -m ingest --inject-bad    # publish a single bad-record to DLQ
    python -m ingest --once --max-cities=1 --max-pages-per-city=1

Exit codes:
    0   clean shutdown
    1   unhandled exception (logged)
    2   bad CLI args
"""

from __future__ import annotations

import argparse
import logging
import sys

from config.logging import configure_logging
from config.settings import get_settings
from ingest.runner import Runner

log = logging.getLogger("heimdall.ingest")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m ingest")
    p.add_argument(
        "--once",
        action="store_true",
        help="Run a single sweep and exit (default: loop forever).",
    )
    p.add_argument(
        "--inject-bad",
        action="store_true",
        help="Publish a single intentionally invalid record to the DLQ topic "
             "and exit. Used by the smoke test.",
    )
    p.add_argument(
        "--max-cities",
        type=int,
        default=None,
        help="Override HEIMDALL_CITIES count (takes the first N).",
    )
    p.add_argument(
        "--max-pages-per-city",
        type=int,
        default=None,
        help="Override HEIMDALL_MAX_PAGES_PER_CITY for this run.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)

    if not args.inject_bad:
        try:
            settings.require_yelp_key()
        except RuntimeError as exc:
            log.error("config.invalid %s", exc)
            return 2

    runner = Runner(settings)
    runner.install_signal_handlers()

    try:
        if args.inject_bad:
            log.info("inject_bad.start")
            runner._producer.inject_bad_for_test()  # noqa: SLF001
            runner._producer.flush()                # noqa: SLF001
            log.info("inject_bad.done")
            return 0

        cities = (
            settings.cities[: args.max_cities]
            if args.max_cities is not None
            else None
        )

        if args.once:
            runner.run_once(
                cities=cities,
                max_pages_per_city=args.max_pages_per_city,
            )
        else:
            runner.run_forever()
        return 0
    except Exception:  # noqa: BLE001
        log.exception("ingest.fatal")
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())
