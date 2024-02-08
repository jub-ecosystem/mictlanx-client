import time as T
import os 
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

for i in range(100):
    x = client.get(bucket_id="test-bucket-1",key="k{}".format(i))
    if x.is_ok:
        task_id = x.unwrap()
        print("GET_TASK_ID",task_id)

# path = "/source/f10mb"
# bucket_id = "test-bucket-0"

# for (root,_,fullnames) in os.walk(path):
#     for fullname in fullnames:
#         full_path = "{}/{}".format(root,fullname)
#         x= client.put(
#             bucket_id=bucket_id,
#             key="",
#             path=full_path,
#             chunk_size="1MB",
#         )
#         if x.is_ok:
#             x_response = x.unwrap()
#             print("TASK_ID",x_response)

        # print(root,fullname)
T.sleep(1000)