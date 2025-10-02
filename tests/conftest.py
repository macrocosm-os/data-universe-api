from pathlib import Path
from dotenv import load_dotenv

import pytest

from s3_storage_api.logging_config import configure_logging

REPO_ROOT = Path(__file__).parent.parent
TESTS_ROOT = REPO_ROOT / "tests"

TEST_DOTENV = REPO_ROOT / ".test.env"

if TEST_DOTENV.exists():
    load_dotenv(TEST_DOTENV, override=False)


@pytest.fixture(autouse=True)
def configure_structlog_for_pytest(caplog):
    configure_logging(dev=True)
