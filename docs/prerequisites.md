# Prerequisites 🧾
You must meet the prerequisites to run successfully the MictlanX Client: 

1. Clone this repository to run the examples. 
```sh
git clone git@github.com:nachocodexx/mictlanx-client.git && \
cd mictlanx-client
```
2.  Installing dependencies using the following command:
```sh
pip3 install -r ./requirements.txt
```
3. You should create a folder to save the client's log, the default path is at ```/mictlanx/client```: 
   
```bash
export CLIENT_LOG_PATH=/mictlanx/client
sudo mkdir -p $CLIENT_LOG_PATH && \
sudo chmod 774 -R $CLIENT_LOG_PATH && \
sudo chown $USER:$USER $CLIENT_LOG_PATH
```

:warning: Make sure to assign the right permissions.

## First steps ⚙️
Run the examples in this repository located at the folder path```examples/```. First you should configure the client using the ```.env``` file. 
```shell
MICTLANX_ROUTERS="mictlanx-router-0:localhost:60666"
MICTLANX_PROTOCOL="http"
MICTLANX_MAX_WORKERS=4
MICTLANX_API_VERSION=4
MICTLANX_SUMMONER_IP_ADDR="localhost"
MICTLANX_SUMMONER_PORT="15000"
MICTLANX_SUMMONER_API_VERSION="3"
MICTLANX_SUMMONER_SUBNET="10.0.0.0/25"
```
⚠️If you want to configure at fine-grain level you should use the python interface. See [Advance usage](#)

If you don't have a virtual spaces up an running, you can use the following test virtual space with maxium payload of 100MB that means that you cannot upload files greater than 100MB, replace the ```MICTLANX_PEERS``` and ```MICTLANX_PROTOCOL```:

```sh
MICTLANX_ROUTERS="mictlanx-router-0:alpha.tamps.cinvestav.mx/v0/mictlanx/router:-1"
MICTLANX_PROTOCOL="https"
```


Next, you can perform basic ```PUT``` and ```GET``` operations, first we are going to perform a ```PUT``` using the following command:

```sh
export BUCKET_ID=mictlanx
export SOURCE_FILE_PATH=/source/01.pdf

python ./examples/v4/01_put.py $BUCKET_ID $SOURCE_FILE_PATH
```

⚠️ Make sure that you assign a path of an existing file.

The result in the terminal looks like this:
```json
{
	"timestamp": "2024-02-10 18:24:09,296",
	"level": "INFO",
	"logger_name": "client-example-0",
	"thread_name": "MainThread",
	"event": "PUT.CHUNKED",
	"bucket_id": "bucket-0",
	"key": "bac9b6c65bb832e7a23f936f8b1fdd00051913fc0c483cf6a6f63f89e6588b80",
	"size": 110857,
	"response_time": 0.002903461456298828,
	"peer_id": "mictlanx-peer-0"
} 
```
Copy the key of the file to download later

✨ The logs are stored in  ```CLIENT_LOG_PATH``` if you don't set a value for the ```CLIENT_LOG_PATH``` the default value is ```/mictlanx/client```.

Next you can access your data, but first, we can get the metadata of the file:

```sh
export KEY=bac9b6c65bb832e7a23f936f8b1fdd00051913fc0c483cf6a6f63f89e6588b80
export MICTLANX_PROTOCOL=http
export ROUTER_URL=localhost:60666

curl -X GET $MICTLANX_PROTOCOL://$ROUTER_URL/api/v4/buckets/$BUCKET_ID/metadata/$KEY
```

✨ You also can copy the url in a browser to see the metadata. ⚠️Remeber change the variables for the actual value for example click to see the metadata [https://alpha.tamps.cinvestav.mx/v0/mictlanx/router/api/v4/buckets/mictlanx/metadata/bac9b6c65bb832e7a23f936f8b1fdd00051913fc0c483cf6a6f63f89e6588b80](https://alpha.tamps.cinvestav.mx/v0/mictlanx/peer0/api/v4/buckets/mictlanx/metadata/bac9b6c65bb832e7a23f936f8b1fdd00051913fc0c483cf6a6f63f89e6588b80)

Run the following command toget data using your ```KEY``` and your ```BUCKET_ID```:

```sh
export BUCKET_ID=mictlanx
export KEY=bac9b6c65bb832e7a23f936f8b1fdd00051913fc0c483cf6a6f63f89e6588b80
export NUM_GETS=10
python3 ./examples/v4/02_get.py $BUCKET_ID $KEY $NUM_GETS
```

You're gonna see in the terminal something like this:

```json
{ 
	"timestamp": "2024-02-11 08:53:42,961",
	"level": "INFO",
    "logger_name": "client-example-0",
    "thread_name": "mictlanx-worker_0",
    "event": "GET",
    "bucket_id": "mictlanx",
    "key": "bac9b6c65bb832e7a23f936f8b1fdd00051913fc0c483cf6a6f63f89e6588b80",
	"size": 110857,
    "response_time": 1.1288352012634277,
    "metadata_service_time": 0.4961841106414795,
    "peer_id": "mictlanx-peer-1"
}
```
