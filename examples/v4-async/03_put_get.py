
import time as T
import numpy as np
import os 
from concurrent.futures import ThreadPoolExecutor
from mictlanx.v4.client import AsyncClient,Peer

client=  AsyncClient(
    client_id="client-0",
    peers= [
        Peer(peer_id="mictlanx-peer-0", ip_addr="localhost", port=7000),
        Peer(peer_id="mictlanx-peer-1", ip_addr="localhost", port=7001),
    ],
    bucket_id="catalogo10MB121A",
    debug= True,
    max_workers=10,
    lb_algorithm="2CHOICES_UF",
)
# futures = []
client.start()

path = "/source/f10mb"
bucket_id = "test-bucket-1"

i=0
with ThreadPoolExecutor(max_workers=4) as tp:
    for (root,_,fullnames) in os.walk(path):
        for fullname in fullnames:
            key="k{}".format(i)
            full_path = "{}/{}".format(root,fullname)
            x= client.put(
                bucket_id=bucket_id,
                key = key,
                path=full_path,
                chunk_size="1MB",
            )
            gets_counter = np.random.randint(low=1, high=10)
            if x.is_ok:
                task_id = x.unwrap()
                for j in range(gets_counter):
                    print("Get[{}]".format(j), task_id)
                    tp.submit(client.get, bucket_id = bucket_id,key=key)
            i+=1

            # print(root,fullname)
    T.sleep(1000)