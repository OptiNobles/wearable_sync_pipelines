import os
import shutil
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# The time zone the gold job runs with (bundle variable 'timezone').
TIMEZONE = "Europe/Warsaw"


@pytest.fixture(scope="session")
def spark() -> Iterator["SparkSession"]:
    """Local Spark (needs Java 17) for tests of the gold transformations.

    One session for the whole test run: starting the JVM takes seconds.
    """
    from pyspark.sql import SparkSession

    # Fail with a clear message rather than PySpark's JAVA_GATEWAY_EXITED; never skip,
    # so a green run always includes the gold tests.
    java_home = os.environ.get("JAVA_HOME")
    if not shutil.which("java") and not (java_home and Path(java_home, "bin").is_dir()):
        pytest.fail(
            "The gold tests need Java 17 for local Spark (java on PATH or JAVA_HOME); "
            "see README.md, Development.",
            pytrace=False,
        )

    # Spark starts Python workers with this interpreter (the test virtual environment).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("wearable_sync_pipelines-tests")
        .config("spark.sql.session.timeZone", TIMEZONE)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
