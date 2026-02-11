
import pytest
import os
import asyncio
import pytest_asyncio
from mictlanx import AsyncClient
import time as T
from mictlanx.utils.compression import CompressionAlgorithm
import uuid
from pathlib import Path  
import dotenv 
dotenv.load_dotenv(".env.test")
# --- Fixtures ---

@pytest.fixture(scope="session")
@pytest.mark.asyncio
def async_client():
    """
    Session-scoped fixture to create and tear down the AsyncClient.
    """
    uri = os.environ.get("MICTLANX_URI", "mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0")
    client_id = os.environ.get("CLIENT_ID", "client-0")
    log_path = os.environ.get("MICTLANX_LOG_PATH", "/mictlanx/client")
    
    client = AsyncClient(
        client_id=client_id,
        uri=uri,
        debug=True,
        max_workers=8,
        log_output_path=log_path
    )
    
    # Yield the client to the tests
    return client


@pytest.fixture
def unique_id() -> str:
    """Provides a unique string for test isolation."""
    return str(uuid.uuid4())

@pytest_asyncio.fixture
# @pytest.mark.asyncio
async def setup_test_object(async_client: AsyncClient, unique_id: str):
    """
    Fixture to PUT a test object and yield its info.
    This ensures data exists for the GET tests and is cleaned up afterward.
    """
    bucket_id = f"pytest-get-bucket-{unique_id}"
    ball_id = f"pytest-get-ball-{unique_id}"
    # Use data that will be chunked
    test_data = (f"This is the test data for {ball_id}. " * 500).encode('utf-8')
    
    print(f"\n[Fixture Setup] Putting object '{ball_id}' in bucket '{bucket_id}'")
    
    put_result = await async_client.put(
        bucket_id=bucket_id,
        key=ball_id,
        value=test_data,
        rf=1,
        chunk_size="10kb"  # Use small chunks to test chunking
    )
    
    assert put_result.is_ok, f"FIXTURE SETUP FAILED: Could not put object: {put_result.unwrap_err()}"
    
    # Yield the necessary info to the tests
    yield (bucket_id, ball_id, test_data)
    
    # Teardown: Clean up the bucket
    print(f"\n[Fixture Teardown] Deleting bucket '{bucket_id}'")
    try:
        delete_result = await async_client.delete_bucket(bucket_id=bucket_id, force=True)
        assert delete_result.is_ok, f"FIXTURE TEARDOWN FAILED: {delete_result.unwrap_err()}"
    except Exception as e:
        print(f"FIXTURE TEARDOWN EXCEPTION: {e}")

# --- Tests ---

@pytest.mark.asyncio
async def test_get_chunk(async_client: AsyncClient, setup_test_object):
    """Tests fetching a single specific chunk."""
    print(setup_test_object)
    bucket_id, ball_id, original_data = setup_test_object
    
    print(f"\n[test_get_chunk] Getting chunk 1 from '{ball_id}'")
    
    res = await async_client.get_chunk(
        bucket_id=bucket_id,
        ball_id=ball_id,
        index=1,  # Get the second chunk (index 1)
        backoff_factor=4,
        max_retries=5
    )
    
    assert res.is_ok, f"get_chunk failed: {res.unwrap_err()}"
    
    # Assert that we got the correct slice of data
    (chunk,_) = res.unwrap()
    # chunk_size = 10 * 1024  # 10kb, matching the put
    # expected_data_slice = original_data[chunk_size:chunk_size*2]
    
    chunk_size = len(chunk.data)
    assert chunk_size == 10000, "Chunk size is incorrect"
    # assert len(chunk.data) == expected_data_slice, "Chunk data does not match original data slice"
    assert chunk.index == 1, "Chunk index is incorrect"


@pytest.mark.asyncio
async def test_get_to_file(async_client: AsyncClient, setup_test_object, tmp_path: Path):
    """Tests downloading an object directly to a file."""
    bucket_id, ball_id, original_data = setup_test_object
    output_dir = tmp_path / "test_downloads"
    output_dir.mkdir()
    output_file_path = output_dir / "test_download.dat"
    
    print(f"\n[test_get_to_file] Getting '{ball_id}' to file: {output_file_path}")
    
    x_result = await async_client.get_to_file(
        bucket_id=bucket_id,
        ball_id=ball_id,
        output_path=str(output_dir),
        fullname=output_file_path.name,
        max_paralell_gets=4,
        chunk_size="1mb", # Get chunk size can be different from put
        force=True
    )
    
    assert x_result.is_ok, f"get_to_file failed: {x_result.unwrap_err()}"
    
    # Verify the file content
    assert output_file_path.exists(), "Downloaded file does not exist"
    downloaded_data = output_file_path.read_bytes()
    assert downloaded_data == original_data, "File content does not match original data"


@pytest.mark.asyncio
async def test_get(async_client: AsyncClient, setup_test_object):
    """Tests getting a full object into memory."""
    bucket_id, ball_id, original_data = setup_test_object
    
    print(f"\n[test_get] Getting '{ball_id}' into memory")
    
    x_result = await async_client.get(
        bucket_id=bucket_id,
        key=ball_id,
        max_paralell_gets=4,
        chunk_size="1mb",
        force=True
    )
    
    assert x_result.is_ok, f"get failed: {x_result.unwrap_err()}"
    
    downloaded_data = x_result.unwrap()
    assert downloaded_data.data == original_data, "In-memory data does not match original data"


@pytest.mark.asyncio
async def test_get_chunks_generator_ordered(async_client: AsyncClient, setup_test_object):
    """
    Tests iterating through the get_chunks async generator
    with order=True, expecting chunks in sequence.
    """
    bucket_id, ball_id, original_data = setup_test_object

    print(f"\n[test_get_chunks_generator_ordered] Streaming chunks for '{ball_id}' with order=True")
    
    x_gen = async_client.get_chunks(
        bucket_id=bucket_id,
        key=ball_id,
        max_parallel_gets=4,
        chunk_size="1mb",
        backoff_factor=1.5,
        order=True  # <-- Test the new parameter
    )
    
    # Since order=True, we can just append to a list
    downloaded_chunks_list = []
    expected_chunk_index = 0
    
    # The generator yields (Metadata, memoryview) tuples
    async for (chunk_metadata, chunk_data) in x_gen:
        
        # Get the index from the metadata tags
        index_str = chunk_metadata.tags.get("index", -1)
        assert index_str != -1, "Chunk is missing 'index' tag in its metadata"
        
        index = int(index_str)
        print(f"Received chunk index {index}")

        # Check that the chunks are arriving in the correct order
        assert index == expected_chunk_index, f"Chunks arrived out of order. Expected {expected_chunk_index}, got {index}"
        
        downloaded_chunks_list.append(bytes(chunk_data))
        expected_chunk_index += 1

    # Reassemble the data and verify it
    assert len(downloaded_chunks_list) > 0, "No chunks were downloaded"
    
    downloaded_data = b"".join(downloaded_chunks_list)
    assert downloaded_data == original_data, "Reassembled data from chunks does not match original"

