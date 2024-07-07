# Architecture 🏗️
The MictlanX architecture is designed to handle the organization and interaction of its main components within a virtual storage space (VSS) that includes both client-side and server-side elements. Here's a detailed description of the architecture based on the provided diagram:

## Data granularity 

```MictlanX - Client``` is an easy to use python library that allows users or application to communicate with a MictlanX Storage System. First we need to understand the data granularity in ```MictlanX``` is represented in the Fig. 1  showing how it grouped the data in buckets that store multiple balls. The ball can be interpreted as a memoryview (zero-copy buffer), file that is save on disk or an object that is saved in the Cloud and can be transformed by its methods (e.g encryption, access policies, etc).

<!-- can be an entire file or chunks that are pieces of data. -->

<div align="center">
  <div>
	<img width="450" src="/assets/02.png" />
  </div>
  <div align="center">
	<span>Fig 1. Data granularity.</span>
  </div>
</div>

## Conceptual representation - Layered Architecture
A conceptual representation is shown in the image below: 

- **MictlanX - Client**: Client is a critical component of the MictlanX storage system that facilitates both the production and consumption of data. It interfaces directly with users or applications to handle data input and output operations, ensuring efficient data management. 
- **Producer**: It is composed of a fifo queue that keep track of the put operations. This operation are balanced to a available router. The metadata is serialized and data is segemented to send over the network to a router node.
- **Consumer**: Get the encrypted chunks from the VSS to decrypted, deserialize and integrated to retrive the data. The consumer has a caching that keeps in memory in form of chunks the most accessed data.
- **Virtual Storage Space (VSS)**: Virtual Storage Space in the MictlanX system acts as an abstracted layer that encompasses various components such as Xolo-Guard, Router System, and the Decentralized Storage Pool (DSP). This abstraction provides a unified and flexible view of storage resources, enabling efficient data management and retrieval.
	- *Xolo* - Guard:  Acts as a security and integrity enforcement layer.
	- *Router system*: Manages the distribution and routing of data. Routes data chunks to appropriate storage peers within the DSP, balancing load and enhancing access times.
	- *Decentrialized Storage Pool (DPS)*: Provides a distributed storage infrastructure. Stores data chunks across multiple storage peers (p1, p2, p3, p4), ensuring redundancy and high availability.

<p align="center">
  <img width="850" src="/assets/01.png" />
  <div align="center">
	<span>Fig 2. MictlanX Architecture the organization of the main components..</span>
  </div>
</p>

## Data flow and operations
Data Flow and Operations in the MictlanX storage system refer to the processes and pathways through which data is produced, processed, stored, and retrieved. These operations ensure efficient data management, high availability, and performance.


#### Put Operations:
- *Local Put (Red Solid Arrows)*: Enqueue data files directly in the client's queue. They are read from local disk.

- *Network Put (Red Dashed Arrows)*: Sends data chunks to be stored in the decentralized storage peers (p1, p2, p3).
#### Read Operations:
- *Local Read (Blue Solid Arrows)*: Retrieves data from the client's local storage.

- *Network Read (Blue Dashed Arrows)*: Retrieves data from the decentralized storage peers if not available locally.

- *Encrypted Chunks*: Data chunks are encrypted before being stored or transmitted to ensure data security.

- *Deploy (Gray Dashed Arrows)*: Indicates the deployment of storage peers.