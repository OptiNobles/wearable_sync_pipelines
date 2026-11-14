"""Helpers shared by the job entry points."""

import argparse
import logging
import re
import sys

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


def configure_logging() -> None:
    # No-op when logging is already configured; needed in every process, including the
    # separate one that runs foreachBatch handlers on serverless (Spark Connect).
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )


def identifier(value: str) -> str:
    """argparse type for Unity Catalog names that are interpolated into SQL."""
    if not _IDENTIFIER.match(value):
        raise argparse.ArgumentTypeError(f"invalid Unity Catalog name: {value!r}")
    return value
