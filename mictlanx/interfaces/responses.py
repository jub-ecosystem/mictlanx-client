from typing import Dict ,Any,TypeVar,List
from dataclasses import dataclass
from pydantic import BaseModel,Field

T = TypeVar("T")


# class ReplicationEvent(BaseModel):


    
class Metadata(BaseModel):
    """Metadata record for a single stored chunk.

    The ``key`` field uniquely identifies a chunk within a bucket.  A group
    of chunks sharing the same ``ball_id`` forms a logical ball.
    """

    key:str # Unique identifier
    size:int # Size in bytes of the data
    checksum:str # Sha256 checksum
    tags:Dict[str,str] # User-defined metadata
    content_type:str # Define the type of the content
    producer_id:str # Unique identifier of the user that allocate the data. 
    ball_id:str # Unique identifier used for segmentation purposes ball_id -> [chunk1, chunk2,...,chunkN]
    bucket_id:str = Field(default="") # Unique identifier used for MictlanX Sync
    is_disabled:bool=Field(default=False)

class ChunkMetadata(BaseModel):
    """Lightweight metadata summary for a single chunk (id, size, checksum, tags)."""

    id:str
    size:int
    checksum:str
    # index:int
    group_id:str
    tags:Dict[str,str]

   
@dataclass
class AsyncGetResponse:
    """Result of an async get operation: reassembled data and per-chunk metadata."""

    data:memoryview
    metadatas:List[Metadata]

class BallMetadata(BaseModel):
    """Aggregated metadata for an entire ball (all chunks combined)."""

    bucket_id:str
    ball_id:str
    size:str
    size_bytes:int
    checksum:str
    chunks:List[Metadata]
    

@dataclass
class PeerStatsResponse:
    """Disk and ball statistics returned by a single peer's stats endpoint."""

    peer_id:str
    used_disk:int
    total_disk:int 
    available_disk:int
    disk_uf:float
    balls:List[Metadata] 
    peers:List[str]
    def __eliminate_duplicates(self,metadata_list: List[Metadata]) -> List[Metadata]:
        seen = set()
        unique_metadata = []
        for metadata in metadata_list:
            identifier = (metadata.bucket_id, metadata.key)
            if identifier not in seen:
                seen.add(identifier)
                unique_metadata.append(metadata)
        return unique_metadata
    def to_dict(self):
        """Serialise this response to a plain dictionary.

        Returns:
            Dict with all fields; ``balls`` list is converted via
            ``model_dump()``.
        """
        # Use asdict for simple fields, but manually convert the Metadata objects.
        data = self.__dict__.copy()
        data["balls"] = [ball.model_dump() for ball in self.balls]
        return data
    @staticmethod
    def empty()->'PeerStatsResponse':
        """Return a zeroed ``PeerStatsResponse`` used as the identity for aggregation.

        Returns:
            A ``PeerStatsResponse`` with all counters and lists set to zero/empty
            and ``peer_id`` set to ``"global"``.
        """
        return PeerStatsResponse(
            peer_id="global",
            available_disk=0,
            balls=[],
            disk_uf=0,
            peers=[],
            total_disk=0,
            used_disk=0
        )
    def __add__(self,other:'PeerStatsResponse')->'PeerStatsResponse':
        return PeerStatsResponse(
            peer_id="global",
            used_disk= self.used_disk + other.used_disk,
            total_disk= self.total_disk + other.total_disk,
            available_disk=self.available_disk + other.available_disk,
            balls= self.__eliminate_duplicates(self.balls + other.balls),
            disk_uf= (self.disk_uf + other.disk_uf)/2,
            peers= list(set(self.peers + other.peers))
        )


class ElasticResponse(BaseModel):
    """Response from a router pool-resize (elastic scaling) operation."""

    pool_size: int
    response_time: float


class ReplicationResponse(BaseModel):
    """Acknowledgement returned after a replication event is triggered."""

    replication_event_id: str
    response_time: float


class DeleteBucketResponse(BaseModel):
    """Summary of a bucket-delete operation across all peers."""

    bucket_id: str
    deleted: int
    failed: int
    total: int
    keys: List[str]
    response_time: float


class BallContext(BaseModel):
    """Peer locations and size for a single ball within a VSS state snapshot."""

    locations: List[str]
    size: int


class PeerData(BaseModel):
    """Network address record for a single storage peer node."""

    node_id: str
    ip_addr: str
    port: int


class PeerCurrentState(BaseModel):
    """Snapshot of a peer's current node list and ball locations."""

    nodes: List[PeerData]
    balls: Dict[str, BallContext]


class BallBasicData(BaseModel):
    """Minimal ball descriptor: bucket, key, and size."""

    bucket_id: str
    key: str
    size: int


class StoragePeerResponse(BaseModel):
    """Capacity and address summary for a storage peer as reported by the router."""

    id: str
    disk: int
    memory: int
    ip_addr: str
    port: int
    weight: float
    used_disk: int
    used_memory: int


class GetSizeByKey(BaseModel):
    """Size query result: resolved peer and byte count for a given key."""

    bucket_id: str
    key: str
    peer_id: str
    size: int


class ReplicateResponse(BaseModel):
    """Result of a single peer-to-peer chunk replication operation."""

    peer_id: str
    replica_peer_id: str
    bucket_id: str
    key: str
    size: int
    ok: bool
    response_time: int


class DeletedResponse(BaseModel):
    """Generic delete acknowledgement with a count of removed entries."""

    n_deletes: int
    key_or_ball_id: str


class DeletedByBallIdResponse(BaseModel):
    """Delete acknowledgement scoped to a ``ball_id``."""

    n_deletes: int
    ball_id: str


class DeletedByKeyResponse(BaseModel):
    """Delete acknowledgement scoped to a single chunk ``key``."""

    n_deletes: int
    key: str


class DeletedBallResponse(BaseModel):
    """Delete acknowledgement for all chunks belonging to a ball."""

    n_deletes: int
    ball_id: str


class BucketDeleteResponse(BaseModel):
    """Outcome of deleting all objects within a bucket."""

    n_deleted_objects: int
    response_time: float


class PeerPutChunkedResponse(BaseModel):
    """Response from a peer after it successfully stored one chunk."""

    node_id: str
    combined_key: str
    bucket_id: str
    key: str
    size: int
    throughput: float
    service_time: float


class RouterPutChunkedResponse(BaseModel):
    """Response from the router after it has distributed one chunk to peers."""

    peer_ids:List[str]
    combined_key:str
    bucket_id:str
    key:str
    size:int


class PutChunkedResponse(BaseModel):
    """Aggregated response after all chunks of a ball have been uploaded."""

    bucket_id: str
    key: str
    size: int
    replicas: List[str]
    throughput: float
    response_time: float


class GetBucketMetadataResponse(BaseModel):
    """Metadata list for all balls stored on a single peer in a given bucket."""

    peer_id: str
    balls: List[Metadata]

    def __str__(self):
        return f"Bucket(n={len(self.balls)})"


class GetRouterBucketMetadataResponse(BaseModel):
    """Aggregated bucket metadata response as returned by the router."""

    bucket_id: str
    peer_ids: List[str] = []
    balls: List[Metadata] = []
    extra: Dict[str, Any] = {}

    def __str__(self):
        return f"Bucket(n={len(self.balls)})"


class GetMetadataResponse(BaseModel):
    """Full metadata response for a single chunk, including routing info."""

    service_time: int
    peer_id: str
    local_peer_id: str
    metadata: Metadata


class GetUFSResponse(BaseModel):
    """Disk utilisation factor (UFS) summary for a peer or VSS."""

    total_disk: int
    used_disk: int
    disk_uf: float

    def __str__(self):
        return f"GetUFSResponse(total_disk={self.total_disk}, used_disk={self.used_disk}, disk_uf={self.disk_uf})"


class PeerPutMetadataResponse(BaseModel):
    """Acknowledgement from a peer after metadata for a task has been registered."""

    key: str
    service_time: int
    task_id: str
    node_id: str


class PutMetadataResponse(BaseModel):
    """Aggregated metadata-registration response from the router."""

    key: str
    service_time: float
    tasks_ids: List[str] = []
    bucket_id: str = ""
    replicas: List[str] = []
    extra: Dict[str, Any] = {}


class PutDataResponse(BaseModel):
    """Response from a peer after raw chunk data has been accepted and validated."""

    service_time: int
    throughput: float


class PutResponse(BaseModel):
    """Final put response returned to the caller after all chunks are stored."""

    key:str
    response_time:float
    replicas:List[str]
    throughput:float


@dataclass
class GetToFileResponse:
    """Result of a get-to-file operation: destination path and routing metadata."""

    path:str
    metadata: Metadata
    response_time:float
    peer_id:str


@dataclass
class UpdateResponse:
    """Result of an update operation including replication details."""

    updated:bool
    bucket_id:str
    key:str
    replicas:List[str]
    throughput:float
    response_time:float


class SummonResponse(BaseModel):
    """Response from the Summoner after a peer container is started."""

    container_id:str
    service_time:int
    ip_addr:str
    port:int


class SummonServiceResponse(BaseModel):
    """Service descriptor returned when a summoned peer is registered."""

    id:str
    container_id: str
    created_at:int
    client_id:str


class GroupedBallResponse(BaseModel):
    """A ball with all its constituent chunk metadata, grouped by ``ball_id``."""

    bucket_id:str
    ball_id:str
    size:int
    balls:List[Metadata]