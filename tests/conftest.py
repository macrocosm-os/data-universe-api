from pathlib import Path
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).parent.parent
TESTS_ROOT = REPO_ROOT / 'tests'

TEST_DOTENV = REPO_ROOT / '.test.env'

if TEST_DOTENV.exists():
    load_dotenv(TEST_DOTENV, override=False)
