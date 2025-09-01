import sys
from mictlanx.services import AsyncPeer
import hashlib
import asyncio
import argparse

async def example_run():
    parser = argparse.ArgumentParser(
        description="Example script to upload data with task_id, and key"
    )

    # Define arguments
    parser.add_argument("--task_id", required=True, help="Task ID from put_metadata (step 1)")
    parser.add_argument("--key", required=True, help="Key the ball")
    
    args = parser.parse_args()

    peer = AsyncPeer(
        peer_id     = "mictlanx-peer-0",
        ip_addr     = "localhost",
        port        = 24000,
        protocol    = "http",
        api_version = 4,
    )
    task_id = args.task_id          # logical namespace
    key     = args.key       # your logical object name
    body    = b"Hello from AsyncPeer"
    put_data_result = await peer.put_data(
        task_id      = task_id,
        key          = key,
        value        = body,
        content_type = "text/plain",
    )
    if put_data_result.is_err:
        print("PUT_DATA failed:", put_data_result.unwrap_err())
        return
    print("PUT_DATA SUCCESSFULLY")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(example_run())