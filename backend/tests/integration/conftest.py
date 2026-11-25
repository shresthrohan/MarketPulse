"""Integration tests run inside the backend container against the live compose stack:
`docker compose exec backend pytest tests/integration`."""

import pytest
from app.main import app
from fastapi.testclient import TestClient


@pytest.fixture(scope="session")
def client():
    # One client = one event loop for the whole session (asyncpg connections are loop-bound).
    with TestClient(app) as c:
        yield c
