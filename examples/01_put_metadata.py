import sys
from mictlanx.services import AsyncPeer
import hashlib
import asyncio
import argparse

async def example_run():
    parser = argparse.ArgumentParser(
        description="Example script to upload metadata with bucket, key, and ball_id"
    )

    # Define arguments
    parser.add_argument("--bucket_id", required=True, help="ID of the bucket")
    parser.add_argument("--key", required=True, help="Key or path of the object")
    parser.add_argument("--ball_id", required=True, help="ID of the ball")
    # parser.add_argument("--rf", type=int, default=1, help="Replication factor (default=1)")
    
    args = parser.parse_args()

    peer = AsyncPeer(
        peer_id     = "mictlanx-peer-0",
        ip_addr     = "localhost",
        port        = 24000,
        protocol    = "http",
        api_version = 4,
    )
    bucket_id = args.bucket_id          # logical namespace
    key       = args.key       # your logical object name
    ball_id   = args.ball_id
    body      = b"Hello from AsyncPeer"
    checksum  = hashlib.sha256(body).hexdigest()  # integrity guard
    meta_res = await peer.put_metadata(
        key          = key,
        size         = len(body),
        checksum     = checksum,
        producer_id  = "client-0",
        content_type = "text/plain",
        ball_id      = ball_id,
        bucket_id    = bucket_id,
        tags         = {"fullname": "hello.txt", "extension": "txt"},
    )
    if meta_res.is_err:
        print("PUT_METADATA failed:", meta_res.unwrap_err())
        return

    task_id = meta_res.unwrap().task_id
    print("task_id:", task_id)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(example_run())