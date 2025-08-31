import sys
from mictlanx.services import AsyncPeer
import hashlib
import asyncio
import argparse
import os
import re
from pathlib import Path


FRAME_RE = re.compile(r"frame[_\-]?(\d+)", re.IGNORECASE)


def find_frames(frames_dir: Path) -> list[Path]:
    """
    Find files that look like frames under frames_dir and return them sorted by index.
    Matches: frame_000.gif, frame-12.png, frame_156_delay-0.04s.gif (any extension).
    """
    candidates = []
    for p in frames_dir.iterdir():
        if not p.is_file():
            continue
        m = FRAME_RE.search(p.stem)  # check the name without extension
        if m:
            idx = int(m.group(1))
            candidates.append((idx, p))
    candidates.sort(key=lambda t: t[0])
    return [p for _, p in candidates]

async def upload_frame(peer: AsyncPeer, bucket_id: str, ball_id: str, key: str, path: Path):
    body         = path.read_bytes()
    checksum     = hashlib.sha256(body).hexdigest()
    content_type = "application/octet-stream"
    tags = {
        "fullname": path.name,
        "extension": (path.suffix.lstrip(".") or "bin"),
    }

    # If we can parse a frame number, add it to tags for ordering
    m = FRAME_RE.search(path.stem)
    if m:
        tags["frame_number"] = m.group(1)

    meta_res = await peer.put_metadata(
        key=key,
        size=len(body),
        checksum=checksum,
        producer_id="client-0",
        content_type=content_type,
        ball_id=ball_id,
        bucket_id=bucket_id,
        tags=tags,
    )
    if meta_res.is_err:
        raise RuntimeError(f"PUT_METADATA failed for {path.name}: {meta_res.unwrap_err()}")

    task_id = meta_res.unwrap().task_id

    put_data_result = await peer.put_data(
        task_id=task_id,
        key=key,
        value=body,
        content_type=content_type,
    )
    if put_data_result.is_err:
        raise RuntimeError(f"PUT_DATA failed for {path.name}: {put_data_result.unwrap_err()}")



async def example_run():
    parser = argparse.ArgumentParser(
        description="Example script to upload metadata with bucket, key, and ball_id"
    )

    # Define arguments
    parser.add_argument(
        "--bucket_id",
        required=True, 
        default="videos",
        help="ID of the bucket"
    )
    parser.add_argument(
        "--ball_id",
        default="video123",
        required=True, 
        help="ID of the ball"
    )
    parser.add_argument(
        "--frames_dir",
        default  = "examples/data/frames",
        required = True,
        help     = "Directory containing frame_XXX files (default: ./examples/data/frames)"
    )
    # parser.add_argument("--rf", type=int, default=1, help="Replication factor (default=1)")
    
    args = parser.parse_args()

    peer = AsyncPeer(
        peer_id     = "mictlanx-peer-0",
        ip_addr     = "localhost",
        port        = 24000,
        protocol    = "http",
        api_version = 4,
    )
    bucket_id  = args.bucket_id          # logical namespace
    ball_id    = args.ball_id

    frames_dir = Path(args.frames_dir)
    if not frames_dir.exists():
        raise FileNotFoundError(f"Frames directory not found: {frames_dir}")
    frames = find_frames(frames_dir)
    if not frames:
        raise RuntimeError(f"No frames found under {frames_dir} (expected names like frame_000.gif)")
    
    
    print(f"Found {len(frames)} frame(s) in {frames_dir}")
    uploaded = 0
    for path in frames:
        # Keep the numeric index in the key; preserve extension
        m = FRAME_RE.search(path.stem)
        idx = int(m.group(1)) if m else uploaded
        key = f"{ball_id}_{idx:03d}{path.suffix.lower()}"
        try:
            await upload_frame(peer, bucket_id, ball_id, key, path)
            uploaded += 1
            print(f"[{uploaded}/{len(frames)}] PUT OK -> key={key}")
        except Exception as e:
            print(f"ERROR uploading {path.name}: {e}")
            return
    print(f"All frames uploaded. bucket_id={bucket_id}, ball_id={ball_id}")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(example_run())