import os
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from mictlanx.logger.log import Log
import pandas as pd
import time as T
import hashlib as H
from option import Some,NONE

L = Log(
    name          = "test",
    create_folder = False,
    to_file       = False,
    error_log     = False
)

if __name__ == "__main__":
    trace_path = os.environ.get("TRACE_PATH","/home/nacho/Programming/Python/mictlanx-analysis/traces/805f401c.csv")
    # Space-separated string that contains basic info of the peers.
    peers_str = os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:7000 mictlanx-peer-1:localhost:7001")
    # Parse the peers_str to a Peer object
    peers = list(Utils.peers_from_str(peers_str))
    # Create an instance of MictlanX - Client 
    L.debug("TRACE_PATH={}".format(trace_path))
    # L.debug(p)
    c = Client(
        # Unique identifier of the client
        client_id   = os.environ.get("MICTLANX_CLIENT_ID","client-0"),
        # Storage peers
        peers       = peers,
        # Number of threads to perform I/O operations
        max_workers = int(os.environ.get("MICTLANX_MAX_WORKERS","2")),
        # This parameters are optionals only set to True if you want to see some basic metrics ( this options increase little bit the overhead please take into account).
        debug       = False,
        daemon      = False, 
        # ____________
    )
    for i in range(10):
        fut = c.put(
            value=b"HOLAAAAAAAAAA"*i,
            key="f{}".format(i),
            disabled=True,
            peer_id= Some("mictlanx-peer-1"),
            # bucket_id="MICTLANX_GLOBAL_BUCKET"
        )
        result = fut.result()
        print(result)
        T.sleep(2)

    # trace = pd.read_csv(trace_path)
    # for index, row in trace.iterrows():
    #     key            = row["http_request_uri"]
    #     operation_type = row["http.request.method"]        
    #     iat            = row["interarrival_time"]
    #     # if iat > 10:
    #         # iat = 10

    #     if operation_type == "PUT":
    #         print("PUT",key)
    #     else:
    #         print("GET",key)
    #     T.sleep(iat)