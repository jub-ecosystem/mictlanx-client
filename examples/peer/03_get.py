import sys
from mictlanx.services import AsyncPeer
import asyncio
import argparse
import json as J

async def example_run():
    parser = argparse.ArgumentParser(
        description="Example script to  download metadata and data with bucket, key, and ball_id"
    )

    # Define arguments
    parser.add_argument("--bucket_id", required=True, help="ID of the bucket")
    parser.add_argument("--key", required=True, help="Key or path of the object")
    parser.add_argument("--peer_port", required=False, help="Port of the peer to connect to")
    # parser.add_argument("--rf", type=int, default=1, help="Replication factor (default=1)")
    
    args = parser.parse_args()

    peer = AsyncPeer(
        peer_id     = "mictlanx-peer-0",
        ip_addr     = "localhost",
        port        = int(args.peer_port) if args.peer_port else 25000,
        protocol    = "http",
        api_version = 4,
    )
    bucket_id = args.bucket_id          # logical namespace
    key       = args.key       # your logical object name
    meta_res = await peer.get_metadata(
        bucket_id=bucket_id,
        key          = key,
    )
    if meta_res.is_err:
        print("GET_METADATA failed:", meta_res.unwrap_err())
        return

    metadata = meta_res.unwrap()
    print("METADATA:", J.dumps(metadata.metadata.__dict__, indent=4))

    data_result = await peer.get_streaming(bucket_id=bucket_id,key=key)
    if data_result.is_err:
        print("GET_DATA failed:", data_result.unwrap_err())
        return
    print("*"*20)
    r = data_result.unwrap()
    print("DATA:",r.read() )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(example_run())