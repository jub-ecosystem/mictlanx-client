import pytest
import pytest_asyncio
from mictlanx import AsyncClient
from pathlib import Path
from mictlanx.interfaces import Ball
import dotenv
dotenv.load_dotenv(".env.test")
# --- Fixtures ---

@pytest_asyncio.fixture
async def setup_test_object(async_client: AsyncClient, unique_id: str):
    """PUT a test object and yield its info + Ball metadata for assertions."""
    bucket_id = f"pytest-get-bucket-{unique_id}"
    ball_id   = f"pytest-get-ball-{unique_id}"
    test_data = (f"This is the test data for {ball_id}. " * 100).encode("utf-8")

    print(f"\n[Fixture Setup] Putting object '{ball_id}' in bucket '{bucket_id}'")

    put_result = await async_client.put(
        bucket_id  = bucket_id,
        key        = ball_id,
        value      = test_data,
        rf         = 1,
        chunk_size = "10kb",
    )
    assert put_result.is_ok, f"FIXTURE SETUP FAILED: {put_result.unwrap_err()}"
    yield (bucket_id, ball_id, test_data, put_result.unwrap())

    print(f"\n[Fixture Teardown] Deleting bucket '{bucket_id}'")
    delete_result = await async_client.delete_bucket(bucket_id=bucket_id, force=True)
    assert delete_result.is_ok, f"FIXTURE TEARDOWN FAILED: {delete_result.unwrap_err()}"

# --- Tests ---

@pytest.mark.asyncio
async def test_get_chunk(async_client: AsyncClient, setup_test_object):
    """Tests fetching a single specific chunk."""
    bucket_id, ball_id, _, _ = setup_test_object
    # ball:Ball = ball_metadata
    # num_chunks = ball.len_chunks()
    # print(f"\n[test_get_chunk] Ball '{ball_id}' has {num_chunks} chunks")

    res = await async_client.get_chunk(
        bucket_id      = bucket_id,
        ball_id        = ball_id,
        index          = 0,
        backoff_factor = 4,
        max_retries    = 5,
    )

    assert res.is_ok, f"get_chunk failed: {res.unwrap_err()}"
    chunk, _ = res.unwrap()
    print(f"Received chunk of size {len(chunk.data)} bytes, index={chunk.index}")
    assert chunk.index == 0


@pytest.mark.asyncio
async def test_get_to_file(async_client: AsyncClient, setup_test_object, tmp_path: Path):
    """Tests downloading an object directly to a file."""
    bucket_id, ball_id, original_data, _ = setup_test_object
    output_dir       = tmp_path / "test_downloads"
    output_dir.mkdir()
    output_file_path = output_dir / "test_download.dat"

    print(f"\n[test_get_to_file] Getting '{ball_id}' to file: {output_file_path}")

    x_result = await async_client.get_to_file(
        bucket_id         = bucket_id,
        ball_id           = ball_id,
        output_path       = str(output_dir),
        fullname          = output_file_path.name,
        max_paralell_gets = 4,
        chunk_size        = "1mb",
        force             = True,
    )

    assert x_result.is_ok, f"get_to_file failed: {x_result.unwrap_err()}"
    assert output_file_path.exists(), "Downloaded file does not exist"
    assert output_file_path.read_bytes() == original_data, "File content does not match"


@pytest.mark.asyncio
async def test_get(async_client: AsyncClient, setup_test_object):
    """Tests getting a full object into memory."""
    bucket_id, ball_id, original_data, _ = setup_test_object

    print(f"\n[test_get] Getting '{ball_id}' into memory")

    x_result = await async_client.get(
        bucket_id         = bucket_id,
        key               = ball_id,
        max_paralell_gets = 4,
        chunk_size        = "1mb",
        force             = True,
    )

    assert x_result.is_ok, f"get failed: {x_result.unwrap_err()}"
    assert x_result.unwrap().data == original_data, "In-memory data does not match"


@pytest.mark.asyncio
async def test_get_chunks_generator_ordered(async_client: AsyncClient, setup_test_object):
    """Tests iterating the get_chunks async generator with order=True."""
    bucket_id, ball_id, original_data, ball_metadata = setup_test_object

    print(f"\n[test_get_chunks_generator_ordered] Streaming '{ball_id}'")

    downloaded_chunks_list = []
    expected_chunk_index   = 0

    async for chunk_metadata, chunk_data in async_client.get_chunks(
        bucket_id         = bucket_id,
        key               = ball_id,
        max_parallel_gets = 4,
        chunk_size        = "1mb",
        backoff_factor    = 1.5,
        order             = True,
    ):
        index_str = chunk_metadata.tags.get("index", -1)
        assert index_str != -1, "Chunk is missing 'index' tag"
        index = int(index_str)
        assert index == expected_chunk_index, f"Out-of-order chunk: expected {expected_chunk_index}, got {index}"
        downloaded_chunks_list.append(bytes(chunk_data))
        expected_chunk_index += 1

    assert len(downloaded_chunks_list) > 0, "No chunks were downloaded"
    assert b"".join(downloaded_chunks_list) == original_data, "Reassembled data does not match"
