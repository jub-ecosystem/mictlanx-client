<p align="center">
  <img width="200" src="./assets/logo.png" />
</p>

<div align=center>
<a href="https://test.pypi.org/project/mictlanx/"><img src="https://img.shields.io/badge/build-0.0.106-2ea44f?logo=Logo&logoColor=%23000" alt="build - 0.0.106"></a>
</div>
<div align=center>
	<h1>MictlanX: <span style="font-weight:normal;"> Cient for Storage Function as a Service </span></h1>
</div>

<!-- #  MictlanX  -->
**MictlanX** is a prototype storage system developed for my PhD thesis - titled as *Reactive elastic replication strategy for ephemeral computing*.  For now the source code is kept private, and it is for the exclusive use of the *Muyal-ilal* research group. 


## Getting started 🚀

I'm very glad to introduce the ```v4``` of *MictlanX* which has lots of improvements compare with the old versions:

- Multi-threading client
- Improving put and get operations over chunks.
- Access to your data from whatever peer of your choice.
- Backend improvements decentralized peer-to-peer storage.
<!-- - Access and identity control (with synchronization shared state) -->
  
To summarize the improvements in this version. The use of extra nodes proxy and replica manager was removed, this nodes are indispensable in ```v3``` . Now the storage nodes are not dumb anymore. They communicate each other to balance, distribute, synchronized and manage the global state. 


The data granularity in ```MictlanX``` is represented in the Fig. 1  showing how ```MictlanX``` group the data in buckets that store multiple balls. The ball can be interpreted as a memoryview (zero-copy buffer), file that is save on disk or an object that is saved in the Cloud and can be transformed by its methods (e.g encryption, access policies, etc). 
<!-- can be an entire file or chunks that are pieces of data. -->
<div align="center">
  <div>
	<img width="600" src="./assets/02.png" />
  </div>
  <div>
	<span>Fig 1. Data granularity.</span>
  </div>
</div>

The buckets are an special logic type of storage in ```MictlanX```. The buckets are placed in a virtual storage space (VSS) that enhanced some properties of the data like availability, security and fault-tolerant.


<!-- A conceptual representation is shown in the image below, 1) The user or application that product a $F$ batch that is represented as a set of files $F=\{f_1, f_2,...,f_n\}$. The files can be written in the system using the  ```MictlanX - Client```.  -->

<p align="center">
  <img width="500" src="./assets/01.png" />
</p>

### Availability policies ❗(coming soon)

## Prerequisites 🧾
You must meet the prerequisites to run successfully the MictlanX Client: 

1. Clone this repository to run the examples. 
	```sh
	git clone git@github.com:nachocodexx/mictlanx-client.git && cd mictlanx-client
	```
2.  Installing dependencies using the following command:
	```sh
	pip3 install -r ./requirements.txt
	```
3. You should create a folder to save the client's log, the default path is at ```/mictlanx/client```: 
   
	```bash
	export CLIENT_LOG_PATH=/mictlanx/client

	sudo mkdir -p $CLIENT_LOG_PATH && sudo chmod 774 -R $CLIENT_LOG_PATH && sudo chown $USER:$USER $CLIENT_LOG_PATH
	```
	:warning: Make sure to assign the right permissions.
## First steps ⚙️
Run the examples in this repository located at the folder path```examples/```. First you should configure the client using the ```.env``` file. 
```shell
MICTLANX_PEERS="mictlanx-peer-0:localhost:7000 mictlanx-peer-1:localhost:7001 mictlanx-peer-2:localhost:7002"
MICTLANX_PROTOCOL="http"
# If you don't have a virtual spaces up an running, you can use the following test virtual space with maxium payload of 100MB that means that you cannot upload files greater than 100MB.
# MICTLANX_PEERS="mictlanx-peer-0:alpha.tamps.cinvestav.mx/v0/mictlanx/peer0:-1 mictlanx-peer-1:alpha.tamps.cinvestav.mx/v0/mictlanx/peer1:-1"
# MICTLANX_PROTOCOL="https"
MICTLANX_MAX_WORKERS=4
MICTLANX_API_VERSION=4
MICTLANX_SUMMONER_IP_ADDR="localhost"
MICTLANX_SUMMONER_PORT="15000"
MICTLANX_SUMMONER_API_VERSION="3"
MICTLANX_SUMMONER_SUBNET="10.0.0.0/25"
```
⚠️If you want to configure at fine-grain level you should use the python interface. See [Advance usage](#)


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

Next you can perform access over your data, first we can get the metadata of the file:

```sh
export KEY=bac9b6c65bb832e7a23f936f8b1fdd00051913fc0c483cf6a6f63f89e6588b80
export MICTLANX_PROTOCOL=http
export PEER_URL=localhost:7000

curl -X GET $MICTLANX_PROTOCOL://$PEER_URL/api/v4/buckets/$BUCKET_ID/metadata/$KEY
```

✨ You also can copy the url in a browser to see the metadata.


<p align="right">(<a href="#top">back to top</a>)</p>

## Advance usage 🦕

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

 Ignacio Castillo - [@NachoCastillo]() - jesus.castillo.b@cinvestav.mx

<p align="right">(<a href="#top">back to top</a>)</p>
