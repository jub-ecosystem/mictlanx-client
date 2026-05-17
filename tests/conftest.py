import os
import dotenv

# Load .env.test before any mictlanx import so module-level os.environ reads see the values.
MICTLANX_ENV_FILE = os.environ.get("MICTLANX_ENV_FILE", ".env.test")
if os.path.exists(MICTLANX_ENV_FILE):
    dotenv.load_dotenv(MICTLANX_ENV_FILE)

import pytest
import uuid
import httpx
from mictlanx.asyncx import AsyncClient
from mictlanx.caching import CacheFactory, NoCache
from mictlanx.interfaces.responses import Metadata
from mictlanx.services.peer import AsyncPeer

# ── Integration fixtures ────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def async_client():
    """Session-scoped AsyncClient instance shared across all tests."""
    uri       = os.environ.get("MICTLANX_URI", "mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0")
    client_id = os.environ.get("CLIENT_ID", "client-0")
    log_path  = os.environ.get("MICTLANX_LOG_PATH", "/mictlanx/client")

    client = AsyncClient(
        client_id       = client_id,
        uri             = uri,
        debug           = True,
        max_workers     = 8,
        log_output_path = log_path
    )
    return client

@pytest.fixture
def unique_id() -> str:
    """Per-test unique string for bucket/ball/key isolation."""
    return uuid.uuid4().hex

@pytest.fixture
def peer() -> AsyncPeer:
    return AsyncPeer(
        peer_id  = "mictlanx-peer-0",
        ip_addr  = "localhost",
        port     = int(os.environ.get("MICTLANX_TEST_PEER_PORT", 25000)),
        protocol = "http",
    )

# ── Cache fixtures ──────────────────────────────────────────────────────────

@pytest.fixture
def sample_metadata() -> Metadata:
    """Minimal valid Metadata instance for cache tests."""
    return Metadata(
        key          = "test_key",
        size         = 10,
        checksum     = "sha256_hash",
        tags         = {"env": "test"},
        content_type = "application/octet-stream",
        producer_id  = "user_123",
        ball_id      = "segment_abc",
    )

@pytest.fixture
def lru_cache():
    """Fresh LRU cache with 100 bytes capacity."""
    return CacheFactory.create("LRU", capacity_storage=100)

@pytest.fixture
def lfu_cache():
    """Fresh LFU cache with 100 bytes capacity."""
    return CacheFactory.create("LFU", capacity_storage=100)

@pytest.fixture
def no_cache():
    """No-op cache instance."""
    return NoCache()

# ── Error / HTTP fixtures ───────────────────────────────────────────────────

@pytest.fixture
def mock_request() -> httpx.Request:
    """Reusable HTTP request object for error mapping tests."""
    return httpx.Request("GET", "https://api.mictlanx.com/data")