import os
import sys
from durations import Duration
from mictlanx.v4.ipc.server import Server
from mictlanx.utils.index import Utils

if __name__ == "__main__":
    s = Server(
        bucket_id          = "MICTLANX_GLOBAL_BUCKET",
        max_memory         = os.environ.get("MICTLANX_MAX_MEMORY","512MB"),
        chunk_size         = os.environ.get("MICTLANX_CHUNK_SIZE","1MB"),
        heartbeat_interval = os.environ.get("MICTL`ANX_HEARTBEAT_INTERVAL","1s"),
        to_cloud_interval  = os.environ.get("MICTLANX_TO_CLOUD_INTERVAL","10s"),
        to_disk_interval   = os.environ.get("MICTLANX_TO_DISK_INTERVAL","5s"),
        peers              = list(Utils.peers_from_str(os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:7000 mictlanx-peer-1:localhost:7001")))
    )
    try:
        s.start()
        sys.exit(0)
    except Exception as e:
        print(e)
        sys.exit(1)
    finally:
        s.shutdown()