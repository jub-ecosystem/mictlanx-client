import asyncio
from io import BytesIO
from PIL import Image
import matplotlib.pyplot as plt
import argparse
from mictlanx.services import AsyncPeer
from mictlanx.interfaces import Metadata
from typing import List
# assume: peer is an AsyncPeer already configured

async def get_frame_bytes(peer:AsyncPeer, bucket_id:str, key:str):
    data_result = await peer.get_streaming(bucket_id=bucket_id, key=key)
    if data_result.is_err:
        raise RuntimeError(data_result.unwrap_err())
    stream = data_result.unwrap()      # e.g., a stream-like object
    blob = stream.read()               # if your stream is async, use: await stream.read()
    return blob

async def preview_frames(peer:AsyncPeer, bucket_id:str, balls_in_order:List[Metadata], delay_ms:int=40):
    plt.ion()  # interactive mode
    fig = plt.figure(figsize=(4,4))
    ax = fig.add_subplot(111)
    img_artist = None

    for metadata in balls_in_order:
        data = await get_frame_bytes(peer, bucket_id, metadata.key)
        frame = Image.open(BytesIO(data)).convert("RGB")

        if img_artist is None:
            img_artist = ax.imshow(frame)
            ax.set_title(metadata, fontsize=8)
            ax.axis("off")
        else:
            img_artist.set_data(frame)
            ax.set_title(metadata, fontsize=8)

        plt.pause(max(0.001, delay_ms / 1000.0))  # e.g., 40ms per frame

    plt.ioff()
    plt.show()

async def example_run():
    parser = argparse.ArgumentParser(
        description="Example script to download frames"
    )
    parser.add_argument("--bucket_id",default="videos", required=True, help="ID of the bucket")
    parser.add_argument("--ball_id",default="video123", required=True, help="Key or path of the object")
    parser.add_argument("--delay_ms" ,default="20",help="Delay between each frame")
    
    args = parser.parse_args()

    peer = AsyncPeer(
        peer_id     = "mictlanx-peer-0",
        ip_addr     = "localhost",
        port        = 24000,
        protocol    = "http",
        api_version = 4,
    )

    bucket_id = args.bucket_id          # logical namespace
    ball_id      = args.ball_id    
    res = await peer.get_by_ball_id(bucket_id=bucket_id,ball_id=ball_id)
    if res.is_err:
        raise Exception(f'Failed to get frames: {res.unwrap_err()}')
    
    ball_metadata = res.unwrap()
    balls         = sorted(ball_metadata.balls,key=lambda x: int(x.tags.get("frame_number","0")))
    await preview_frames(peer=peer, bucket_id=bucket_id,balls_in_order=balls,delay_ms=int(args.delay_ms))

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(example_run())