# Getting Started 🚀

1. Import Necessary Modules:
```python
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.index import Router
```
2. Initialize client
```python
client = Client(
    client_id    = "mictlanx-client-0"
    routers        = [
        Router.from_str("mictlanx-router-0:localhost:60666").unwrap()
    ],
)
```
3. Put a file at ```path``` in MictlanX with the ```bucket_id@key``` identifier, if you dont specify the key the client use the sha256 checksum as a key.
```python
path = "/source/out.csv"

x = client.put_file_chunked(
    bucket_id=bucket_id,
    path=path,
    chunk_size="1MB",
    tags={
        "example":"getting-started"
    },
    content_type="text/csv",
    replication_factor=3
)
```