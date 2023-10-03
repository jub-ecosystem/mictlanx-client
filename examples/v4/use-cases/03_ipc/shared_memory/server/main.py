import os
import sys
from durations import Duration
from mictlanx.v4.ipc.server import Server

if __name__ == "__main__":
    s = Server(
        max_memory         = os.environ.get("MICTLANX_MAX_MEMORY","512MB"),
        chunk_size         = os.environ.get("MICTLANX_CHUNK_SIZE","1MB"),
        heartbeat_interval = Duration(os.environ.get("MICTLANX_HEARTBEAT_INTERVAL","1s")).to_seconds()
    )
    try:
        s.start()
        sys.exit(0)
    except Exception as e:
        print(e)
        sys.exit(1)
    finally:
        s.shutdown()
        
#     x = input("Type something to shutdown the server")
#     s.shutdown()
    # c = Client(queue_id="mictlanx_1")
    # c.handshake()
    # T.sleep(1000)