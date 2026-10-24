import os
import sys
import tempfile
from pathlib import Path

# Isolated SQLite database for the test run; no API key so the agent uses fakes.
_tmp = tempfile.mkdtemp()
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/test.db"
os.environ["ANTHROPIC_API_KEY"] = ""
os.environ["AS_OF_DATE"] = "2026-10-20"
os.environ["ADMIN_TOKEN"] = "test-admin"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.db import Base, SessionLocal, engine  # noqa: E402
from app.main import app  # noqa: E402
from app.seed import seed  # noqa: E402


@pytest.fixture()
def session():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with SessionLocal() as s:
        seed(s)
        yield s


@pytest.fixture()
def client(session):
    with TestClient(app) as c:
        yield c
