import os
import asyncio
import dotenv
import pytest
import humanfriendly as HF
from pathlib import Path  # Required for tmp_path
from option import Some
from mictlanx import AsyncClient
from mictlanx.utils.segmentation import Chunks, Chunk
import uuid

# Load environment variables from .env at the start
MICTLANX_ENV_PATH = os.environ.get("MICTLANX_ENV_PATH", ".env.test")
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
    client = AsyncClient(
        client_id=CLIENT_ID,
        uri=URI,
        debug=True,
        max_workers=2,
        log_output_path=LOG_PATH
    )
    # If the client had a .connect() or .close() method,
    # `yield` would be used here to manage setup and teardown.
    return client

@pytest.fixture(scope="function")
def bucket_id_param(request: pytest.FixtureRequest):
    """Fixture to get --bucketid from the command line."""
    DEFAULT_BUCKET_ID = uuid.uuid4().hex
    return request.config.getoption("--bucketid", default=DEFAULT_BUCKET_ID)

@pytest.fixture(scope="function")
def key_param(request: pytest.FixtureRequest):
    """Fixture to get --key from the command line."""
    DEFAULT_KEY = uuid.uuid4().hex
    return request.config.getoption("--key", default=DEFAULT_KEY)

@pytest.fixture
def small_temp_file(tmp_path: Path) -> Path:
    """Creates a small temporary file with binary content."""
    file_path = tmp_path / "test_asset.tmp"
    # Write sample content
    file_path.write_bytes(b"This is a test file for mictlanx." * 100)
    return file_path

@pytest.fixture
def large_temp_file(tmp_path: Path) -> Path:
    """Creates a 50MiB "sparse" temporary file."""
    file_path = tmp_path / "f50mb_sparse.dat"
    size_in_bytes = 50 * 1024 * 1024  # 50 MiB

    with open(file_path, 'wb') as f:
        f.seek(size_in_bytes - 1)
        f.write(b'\0')
    return file_path

# --- Tests ---

@pytest.mark.asyncio
async def test_put_chunks(async_client, small_temp_file, bucket_id_param, key_param):
    """Tests uploading a file using Chunks.from_file."""
    bucket_id = str(bucket_id_param)
    key = str(key_param)
    path = str(small_temp_file)  # Use the temporary file
    rf = 1
    chunk_size = "256kb"
    
    chunks_maybe = Chunks.from_file(
        path=path,
        group_id=key,
        chunk_size=Some(HF.parse_size(chunk_size))
    )
    
    assert chunks_maybe.is_some, "Failed to create Chunks from file"
    chunks = chunks_maybe.unwrap()

    x = await async_client.put_chunks(
        bucket_id=bucket_id,
        key=key,
        rf=rf,
        chunks=chunks,
        max_tries=1
    )
    print(f"put_chunks result: {x}")
    assert x.is_ok, f"put_chunks failed: {x.unwrap_err()}"

@pytest.mark.asyncio
async def test_put_file(async_client, large_temp_file, bucket_id_param, key_param):
    """Tests uploading a large file using client.put_file."""
    key        = str(key_param)
    path       = str(large_temp_file)  # Use the large temporary file
    rf         = 1
    bucket_id  = str(bucket_id_param)
    chunk_size = "25MB"

    x = await async_client.put_file(
        bucket_id=bucket_id,
        chunk_size=chunk_size,
        key=key,
        rf=rf,
        path=path,
        max_tries=10
    )
    print(f"put_file result: {x}")
    assert x.is_ok, f"put_file failed: {x.unwrap_err()}"

@pytest.mark.asyncio
async def test_put(async_client, small_temp_file, bucket_id_param, key_param):
    """Tests uploading in-memory data using client.put."""
    key = str(key_param)
    path = str(small_temp_file)  # Use the small file
    rf = 1
    bucket_id = str(bucket_id_param)
    chunk_size = "25MB"  # Chunk size > data size is a good test case

    with open(path, "rb") as f:
        data = f.read()

    x = await async_client.put(
        bucket_id=bucket_id,
        chunk_size=chunk_size,
        key=key,
        rf=rf,
        value=data,
        max_tries=1,
        tags={
            "test": "kjhshjfhjsfsf",
            "value": "9823984892342"
        }
    )
    print(f"put result: {x}")
    assert x.is_ok, f"put failed: {x.unwrap_err()}"

@pytest.mark.asyncio
async def test_put_single_chunk(async_client, small_temp_file, bucket_id_param, key_param):
    """Tests uploading a single Chunk."""
    key = str(key_param)
    rf = 1
    bucket_id = str(bucket_id_param)
    chunk_size = "25MB"
    path = str(small_temp_file)  # Use the small file
    index = 0

    with open(path, "rb") as f:
        data = f.read()
        chunk = Chunk(
            group_id=key, 
            index=index, 
            data=data, 
            chunk_id=Some(f"{key}_{index}"), 
            metadata={"num_chunks": "1"}
        )
        
        x = await async_client.put_single_chunk(
            bucket_id=bucket_id,
            chunk_size=chunk_size,
            ball_id=key,
            rf=rf,
            chunk=chunk,
            max_tries=1,
            tags={
                "name": small_temp_file.name,
                "from": "test_put_single_chunk"
            }
        )
        print(f"put_single_chunk result: {x}")
        assert x.is_ok, f"put_single_chunk failed: {x.unwrap_err()}"