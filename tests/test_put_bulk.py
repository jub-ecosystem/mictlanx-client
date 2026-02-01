import os
import asyncio
import dotenv
import pytest
from option import Some
from mictlanx import AsyncClient
import mictlanx.interfaces as InterfaceX
from uuid import uuid4

# Load environment variables from .env at the start
MICTLANX_ENV_PATH = os.environ.get("MICTLANX_ENV_PATH", ".env")
if os.path.exists(MICTLANX_ENV_PATH):
    dotenv.load_dotenv(MICTLANX_ENV_PATH)

# --- Configuration Constants ---
URI = os.environ.get(
    "MICTLANX_URI",
    "mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0"
)
LOG_PATH = os.environ.get("MICTLANX_LOG_PATH", "/mictlanx/client")
CLIENT_ID = os.environ.get("CLIENT_ID", "client-0")

# --- Fixtures ---

@pytest.fixture(scope="session")
def async_client():
    """
    Session-scoped fixture that provides a single AsyncClient instance
    for all tests.
    """
    print("init async client", URI)
    client = AsyncClient(
        client_id       = CLIENT_ID,
        uri             = URI,
        debug           = True,
        max_workers     = 2,
        log_output_path = LOG_PATH
    )
    # If the client had a .connect() or .close() method,
    # `yield` would be used here to manage setup and teardown.
    return client



@pytest.mark.asyncio
async def test_put_bulk(async_client: AsyncClient):
    """Tests the bulk put functionality with multiple small files."""
    bucket_id = f"test-bulk-bucket-{uuid4().hex}"
    keys      = [f"bulk_test_file_{i}" for i in range(5)]
    contents  = [f"Content of file {i}".encode('utf-8') for i in range(5)]
    bulk_id   = "01"
    max_concurrency = 3

    balls = []
    for key, content in zip(keys, contents):
        balls.append(
            InterfaceX.BallK(
                bucket_id       = bucket_id,
                ball_id         = f"ball_{key}",
                key             = key,
                source          = content,
                chunk_size      = "1MB",
                tags            = {"test_bulk": "true"},
                rf              = 1,
                timeout         = 30,
                max_tries       = 3,
                max_concurrency = 2,
                max_backoff     = 60
            )
        )

    result = await async_client.put_bulk(bulk_id=bulk_id,balls=balls, max_concurrency=max_concurrency)
    print("RESULT", result)
    assert result.is_ok, f"Bulk put failed: {result.unwrap_err()}"

    x = await async_client.await_bulk(bulk_id=bulk_id,remove_on_completion=False)
    assert x.is_ok, f"Awaiting bulk put failed: {x.unwrap_err()}"
    response = x.unwrap()
    print("BULK PUT RESPONSE:", response)
    assert len(response["successes"]) == len(balls), "Not all files were uploaded successfully."
    assert len(response["failures"]) == 0, "Some files failed to upload."