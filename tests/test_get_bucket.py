import os
import uuid
import pytest
import dotenv
import asyncio
from typing import List, Dict, Any
import pytest_asyncio 

from mictlanx.asyncx import AsyncClient
import mictlanx.interfaces as InterfaceX

dotenv.load_dotenv()

# --- Fixtures ---

@pytest_asyncio.fixture(scope="session")  
async def async_client():
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
    yield client
    
    # Teardown: This code runs after all tests in the session are complete
    print("\n[Pytest] Tearing down AsyncClient...")
    if hasattr(client, 'aclose'):
        await client.aclose()
    print("[Pytest] Client teardown complete.")

@pytest.fixture
def unique_id() -> str:
    """Provides a unique string for test isolation."""
    return str(uuid.uuid4())

@pytest_asyncio.fixture
async def setup_test_bucket_with_balls(async_client: AsyncClient, unique_id: str):
    """
    Fixture to create a new bucket and PUT 3 test objects into it.
    This ensures data exists for the get_bucket_metadata test.
    """
    bucket_id = f"pytest-meta-bucket-{unique_id}"
    
    # Define 3 objects with different paths and extensions
    test_balls = [
        {"key": "file_a", "data": b"data for file a"},
        {"key": "pic_b", "data": b"data for file b"},
        {"key": "report_c", "data": b"data for file c"},
    ]
    
    print(f"\n[Fixture Setup] Putting {len(test_balls)} objects into bucket '{bucket_id}'")
    
    put_tasks = []
    for ball in test_balls:
        fullname =f"{ball['key']}.{'png' if ball['key'] == 'pic_b' else 'txt'}"
        put_tasks.append(
            async_client.put(
                bucket_id=bucket_id,
                key=ball["key"],
                value=ball["data"],
                rf=1,
                chunk_size="10kb",
                tags={
                    "extension": "png" if ball["key"] == "pic_b" else "txt",
                    "bucket_relative_path": "images/" if ball["key"] == "pic_b" else "docs/",
                    "fullname": fullname,
                    "full_path": f"images/{fullname}" if ball["key"] == "pic_b" else f"docs/{fullname}",
                },
            )
        )
    
    put_results = await asyncio.gather(*put_tasks)
    
    # Check that all puts were successful
    for i, result in enumerate(put_results):
        assert result.is_ok, \
            f"FIXTURE SETUP FAILED: Could not put object {test_balls[i]['key']}: {result.unwrap_err()}"
    
    # Yield the info to the test
    yield (bucket_id, test_balls)
    
    # Teardown: Clean up the bucket
    print(f"\n[Fixture Teardown] Deleting bucket '{bucket_id}'")
    try:
        delete_result = await async_client.delete_bucket(bucket_id=bucket_id, force=True)
        assert delete_result.is_ok, f"FIXTURE TEARDOWN FAILED: {delete_result.unwrap_err()}"
    except Exception as e:
        print(f"FIXTURE TEARDOWN EXCEPTION: {e}")

# --- Test ---

@pytest.mark.asyncio
async def test_get_balls_by_bucket_id(async_client: AsyncClient, setup_test_bucket_with_balls):
    """
    Tests getting all object metadata from a specific bucket and verifies
    the contents are correct.
    """
    bucket_id, test_balls = setup_test_bucket_with_balls
    
    print(f"\n[test_get_balls_by_bucket_id] Getting metadata for bucket '{bucket_id}'")
    
    # 1. EXECUTE: Get the bucket metadata
    bucket_result = await async_client.get_bucket_metadata(bucket_id=bucket_id)
    
    # 2. ASSERT: Check that the call was successful
    assert bucket_result.is_ok, f"get_bucket_metadata failed: {bucket_result.unwrap_err()}"
    
    bucket_contents = bucket_result.unwrap()
    
    # 3. ASSERT: Check the number of items returned
    assert len(bucket_contents) == len(test_balls), \
        f"Expected {len(test_balls)} objects, but got {len(bucket_contents)}"

    # Do a more detailed check on one item to verify path/extension parsing
    # Let's find 'pic_b.png'
    pic_b_meta = next((b for b in bucket_contents if b.ball_id == "pic_b"), None)
    
    assert pic_b_meta is not None, "Could not find 'pic_b' in results"
    assert pic_b_meta.extension == "png"
    assert pic_b_meta.bucket_relative_path == "images/"
    assert pic_b_meta.fullname == "pic_b.png"
    assert pic_b_meta.full_path == "images/pic_b.png"
    
    print("[test_get_balls_by_bucket_id] All metadata assertions passed.")