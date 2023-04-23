<p align="center">
  <img width="200" src="./assets/logo.png" />
</p>

<div align=center>
<a href="https://test.pypi.org/project/mictlanx/"><img src="https://img.shields.io/badge/build-0.0.30-2ea44f?logo=Logo&logoColor=%23000" alt="build - 0.0.30"></a>
</div>
<div align=center>
	<h1>MictlanX: <span style="font-weight:normal;">Elastic storage for ephemeral computing</span></h1>
</div>


<!-- #  MictlanX  -->
**MictlanX** is a prototype storage system developed for my PhD thesis - titled as *Reactive elastic replication strategy for ephemeral computing*.  For now the source code is kept private, and it is for the exclusive use of the *AdaptiveZ* research group. 

## MictlanX - Client 
To perform operation in MictlanX you need to use a special client to reduce the service comunication complexity.  This repository presents a easy to use client.

### Getting started 
1)  Installing client by pip
	```sh
	pip install -i https://test.pypi.org/simple/ mictlanx
	```
2) Clone this repository to run the examples. 
	```sh
	git clone git@github.com:nachocodexx/mictlanx-client.git
	```
3) Set the environment variables located at exmaples/v3/.env
4) Run the example
	```sh
	cd examples/v3 
	python3 ./01_put.py <KEY> <PATH>
	```  
	A real example of the above command could be the following:
	```sh
	python3 examples/v3/01_put.py pjTp3x ./data/02.pdf
	# Ok(<mictlanx.v3.interfaces.storage_node.PutResponse object at..>)
	```
	After you successfully run this examples you can go to the following url: ```http://<PROXY_IP_ADDR>:<PROXY_PORT>/api/v3/<KEY>``` 
	
	**MictlanX** must receive an authorization token in every request in order to perform PUT and GET operations. :warning: Please generate a more secure password and set the environment var in examples/v3/.env. 

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
