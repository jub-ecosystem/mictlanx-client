import os
# import  aiofiles
from typing import List,Dict,Generator,Iterator
from option import Option,NONE,Some
import time as T
from xolo.utils.utils import Utils as XoloUtils
import humanfriendly as HF
import mictlanx.interfaces.responses as ResponseModels



class Ball:
    """Logical storage object composed of one or more chunk metadata records.

    A ``Ball`` groups all ``Metadata`` chunks that belong to the same logical
    object (identified by ``ball_id``) inside a bucket.  Call :meth:`build`
    after all chunks have been added to populate the derived fields
    (checksum, size, paths, timestamps).
    """

    def __init__(self,bucket_id:str,chunks:List[ResponseModels.Metadata]=[],ball_id:str="",checksum:str="" , bucket_relative_path:str="",fullname:str=""):
        self.bucket_id            = bucket_id
        self.ball_id              = ball_id
        self.checksum             = checksum
        self.size                 = 0
        self.chunks               = chunks.copy()
        self.bucket_relative_path = bucket_relative_path
        self.full_path            = ""
        self.extension            = ""
        self.filename             = ""
        self.updated_at           = -1
        self.fullname             = fullname
    def __len__(self):
        """Return the number of chunks currently associated with this ball."""
        sizes = map(lambda c: c.size, self.chunks)
        return sum(sizes)
        # return len(s)
    
    def __str__(self):
        return f"Ball(id={self.ball_id}, size = {self.size})"
    def len_chunks(self):
        """Return the number of chunk metadata records currently held.

        Returns:
            Integer chunk count.
        """
        return len(self.chunks)

    def add_chunk(self, chunk:ResponseModels.Metadata):
        """Append a chunk to this ball if it is not already present.

        Deduplication is based on matching ``key`` and ``checksum``.

        Args:
            chunk: ``Metadata`` record for the chunk to add.
        """
        exists = next(filter(lambda x: x.key == chunk.key and chunk.checksum ==x.checksum, self.chunks),-1)
        if exists == -1:
            self.chunks.append(chunk)

    def build(self):
        """Derive ball-level fields from the collected chunk metadata.

        Populates ``checksum``, ``ball_id``, ``bucket_relative_path``,
        ``fullname``, ``full_path``, ``extension``, ``filename``, ``size``,
        and ``updated_at`` by reading the first chunk's tags and summing
        sizes across all chunks.  No-op when ``chunks`` is empty.
        """
        if len(self.chunks) >0:
            c = self.chunks[0] 
            self.checksum             = c.tags.get("full_checksum","")
            self.ball_id              = c.ball_id
            self.bucket_relative_path = c.tags.get("bucket_relative_path","")
            self.fullname             = c.tags.get("fullname","")
            self.full_path            = c.tags.get("full_path","")
            self.extension            = c.tags.get("extension","")
            self.filename             = c.tags.get("filename","")
            # self.updated_at
        self.size = 0
        sum_updated_at = 0
        for c in self.chunks:
            self.size += c.size
            sum_updated_at += int(c.tags.get("updated_at",0))
        self.updated_at = int(sum_updated_at / len(self.chunks))
    
    def merge(self, other: 'Ball'):
        """Merge chunks from another ``Ball`` into this one, skipping duplicates.

        Deduplication is checksum-based.

        Args:
            other: The ``Ball`` whose chunks should be merged in.
        """
        existing_ids = {c.checksum for c in self.chunks}
        for chunk in other.chunks:
            if chunk.checksum not in existing_ids:
                self.chunks.append(chunk)

class Bucket:
    """Namespace grouping many :class:`Ball` objects under a single ``bucket_id``."""

    def __init__(self,bucket_id:str,balls:Dict[str, Ball]):
        self.bucket_id = bucket_id
        self.balls=balls.copy()

    def size_bytes(self)->int:
        """Return the total size of all balls in this bucket, in bytes.

        Returns:
            Aggregate size in bytes.
        """
        size = 0
        for b in self:
            size += b.size
        return size

    def size(self)->str:
        """Return a human-readable string of the total bucket size.

        Returns:
            Size string formatted by ``humanfriendly`` (e.g. ``"1.2 GB"``).
        """
        size = self.size_bytes()
        return HF.format_size(size)
            
    def __len__(self)->int:
        return len(self.balls)
    def __iter__(self) -> Iterator['Ball']:
        return iter(self.balls.values() )

class PeerStats(object):
    """In-memory statistics tracker for a single storage peer.

    Tracks cumulative put/get counters, disk usage, per-key access
    frequencies, and inter-arrival times.  Intended for use by the
    client-side load balancer and monitoring utilities.
    """

    def __init__(self,peer_id:str):
        self.__peer_id                 = peer_id
        self.total_disk:int            = 0
        self.used_disk                 = 0
        self.put_counter:int           = 0
        self.get_counter:int           = 0 
        self.balls                     = set()
        # 
        self.put_last_arrival_time     = -1
        self.put_sum_interarrival_time = 0
        
        self.get_last_arrival_time     = -1
        self.get_sum_interarrival_time = 0
        self.last_access_by_key:Dict[str,int]  = {}
        self.get_counter_per_key:Dict[str,int] = {}

    def put_frequency(self):
        """Return the fraction of all operations that were puts.

        Returns:
            Float in ``[0.0, 1.0]``, or ``0`` when no operations have occurred.
        """
        x =  self.global_counter()
        if  x == 0:
            return 0
        return self.put_counter / x

    def get_frequency(self):
        """Return the fraction of all operations that were gets.

        Returns:
            Float in ``[0.0, 1.0]``, or ``0`` when no operations have occurred.
        """
        x =  self.global_counter()
        if  x == 0:
            return 0
        return self.get_counter / x

    def get_frecuency_per_ball(self):
        """Return per-key get frequency relative to total get operations.

        Returns:
            Dict mapping each key to its fraction of total gets (``[0.0, 1.0]``).
        """
        res = {}
        for key, getcounter in self.get_counter_per_key.items():
            if self.get_counter == 0:
                res[key] = 0
            else:
                res[key] = getcounter / self.get_counter
        return res

    def top_N_by_freq(self,N:int):
        """Return the N most-frequently-accessed keys.

        Args:
            N: Number of top entries to return.

        Returns:
            List of ``(key, frequency)`` tuples sorted by frequency descending.
        """
        xs        = self.get_frecuency_per_ball()
        sorted_xs = list(sorted(xs.items(), key=lambda item: item[1], reverse=True))
        return sorted_xs[:N]

    def get_id(self):
        """Return the peer identifier.

        Returns:
            The ``peer_id`` string passed at construction.
        """
        return self.__peer_id


    
    def put(self,key:str, size:int):
        """Record a put operation for the given key.

        Args:
            key: The object key that was written.
            size: Size in bytes of the written object.
        """
        self.put_counter+=1
        if key not in self.balls:
            self.get_counter_per_key[key] = 0
            self.used_disk+=size
        self.balls.add(key)

    def get(self, key:str, size:int):
        """Record a get operation for the given key.

        Args:
            key: The object key that was read.
            size: Size in bytes of the read object.
        """
        arrival_time = T.time()
        self.get_counter += 1
        self.last_access_by_key.setdefault(key,arrival_time)
        if key not in self.get_counter_per_key:
            self.get_counter_per_key[key] = 1
        else:
            self.get_counter_per_key[key] += 1
        self.balls.add(key)

    def delete(self,key:str,size:int):
        """Record a delete operation and update disk usage.

        Args:
            key: The object key that was deleted.
            size: Size in bytes of the deleted object.
        """
        self.balls.discard(key)
        if self.used_disk >=size:
            self.used_disk-=size
        del self.get_counter_per_key[key]

    def calculate_disk_uf(self,size:int = 0 ):
        """Calculate the disk utilisation factor after a hypothetical write.

        Args:
            size: Hypothetical additional bytes to consider. Defaults to 0.

        Returns:
            Float in ``[0.0, 1.0]`` representing disk fullness.
        """
        return  1 - ((self.total_disk - (self.used_disk + size))/self.total_disk)

    def available_disk(self):
        """Return the number of free bytes on this peer's disk.

        Returns:
            Available bytes (``total_disk - used_disk``).
        """
        return self.total_disk - self.used_disk

    def global_counter(self):
        """Return the total number of put and get operations recorded.

        Returns:
            Integer sum of ``put_counter`` and ``get_counter``.
        """
        return self.put_counter + self.get_counter
    
    def __str__(self):
        

        return "PeerStats(peer_id={}, total_disk={}, used_disk={}, available_disk={}, disk_uf={}, puts={}, gets={}, globals={}, put_feq={}, get_feq={}, topN={})".format(
            self.__peer_id,
            self.total_disk,
            self.used_disk,
            self.available_disk(),
            self.calculate_disk_uf(),
            self.put_counter,
            self.get_counter,
            self.global_counter(),
            self.put_frequency(),
            self.get_frequency(),
            self.top_N_by_freq(3)
        )

def check_destroyed(func):
    def wrapper(self,*args, **kwargs):
        if self._Ball__destroyed:
            raise Exception("{} was destroyed".format(self.key))
        result = func(self,*args, **kwargs)
        return result

    return wrapper

class BallX(object):
    """A ball that can live on disk or in memory with lifecycle management.

    ``BallX`` wraps raw bytes or a file path and provides helpers to move
    data between memory and disk, compute checksums, and safely destroy the
    object.  Methods decorated with :func:`check_destroyed` raise an
    exception if called after :meth:`destroy`.
    """

    def __init__(self,size:int, checksum:str,key:str="", path:Option[str]= NONE, value:bytes = bytes(),tags:Dict[str,str]={}, content_type:str="application/octet-stream") :
        self.size             = size
        self.content_type     = content_type
        self.key              = checksum if key =="" else key
        self.checksum         = checksum
        self.path:Option[str] = path
        self.__mictlanx_path  = "/mictlanx/client/.data/{}".format(self.checksum)
        self.value            = value
        self.tags             = tags
        self.__destroyed      = False
    def __resolve_path(self,path:Option[str]=NONE)->str:
        return path.unwrap_or(self.path.unwrap_or(self.__mictlanx_path))
    
    def from_bytes(key:str, value:bytes)->"Ball":
        """Construct a ``Ball`` from raw bytes and compute its SHA-256 checksum.

        Args:
            key: Logical key for the ball.
            value: Raw bytes payload.

        Returns:
            A new ``Ball`` instance with ``checksum`` and ``size`` populated.
        """
        size = len(value)
        content_type="application/octet-stream"

        checksum = XoloUtils.sha256(value=value)
        return Ball(key=key, size=size, checksum=checksum,value=value,content_type=content_type)

    def from_path(path:str,key:str="")->"Ball":
        """Construct a ``Ball`` from a file path and compute its SHA-256 checksum.

        Args:
            path: Absolute path to the file.
            key: Logical key for the ball. Defaults to ``""``.

        Returns:
            A new ``Ball`` instance with ``checksum`` and ``size`` populated.

        Raises:
            Exception: If the file at ``path`` does not exist.
        """
        if not os.path.exists(path):
            raise Exception("File at {} does not exists".format(path))
        (checksum, size) = XoloUtils.sha256_file(path)
        content_type="application/octet-stream"
        ball = Ball(key=key, checksum=checksum,size=size, path=Some(path),content_type=content_type)
        if os.path.exists(ball._Ball__mictlanx_path):
            ball.path = Some(ball._Ball__mictlanx_path)
        return ball
    
    @check_destroyed
    def to_disk(self,path:Option[str]= NONE, mictlanx_path:bool =True, clean:bool = True)->int:
        """Write the in-memory value to disk.

        Args:
            path: Override destination path.  When ``NONE`` the internal
                MictlanX data path is used (if ``mictlanx_path=True``).
            mictlanx_path: When ``True`` writes to the internal
                ``/mictlanx/client/.data/<checksum>`` location.
            clean: When ``True`` clears ``self.value`` from memory after
                writing. Defaults to ``True``.

        Returns:
            ``0`` on success, ``1`` if the file already exists, ``-1`` if
            there is no data to write.
        """
        size = len(self.value)
        if size ==0:
            return -1
        _path = self.__resolve_path(path= Some (self.__mictlanx_path) if mictlanx_path else path )
        directory= os.path.dirname(_path)
        if not os.path.exists(path=directory):
            os.makedirs(directory)
        
        if os.path.exists(_path):
            return 1
        else:
            with open(_path,"wb") as f:
                f.write(self.value)
            if clean:
                self.clean()
            self.path = Some(self.__mictlanx_path)
            return 0

    @check_destroyed
    def to_memory(self,from_mictlanx:bool = True)->int:
        """Load the ball's data from disk into ``self.value``.

        Args:
            from_mictlanx: When ``True`` reads from the internal MictlanX
                data path. Defaults to ``True``.

        Returns:
            ``0`` on success, ``-1`` if no path is set.
        """
        if from_mictlanx:
            self.read_all()
            
        if self.path.is_none:
            return -1
        else:
            self.value = self.read_all()
            return 0
    
    @check_destroyed
    def clean(self):
        """Release the in-memory bytes without touching the on-disk file."""
        self.value=b""

    @check_destroyed
    def destroy(self):
        """Delete the on-disk file and mark this ball as permanently destroyed.

        After calling ``destroy`` any further method call (except dunder
        methods) will raise an exception via :func:`check_destroyed`.
        """
        self.clean()
        path = self.__resolve_path()
        if os.path.exists(path):
            print("Removed {}".format(path))
        self.__destroyed =True
        
    def read_all(self)->bytes:
        """Read the entire ball from disk into memory.

        Returns:
            Raw bytes of the ball's content.
        """
        with open(self.__resolve_path(path = self.path),"rb") as f:
            return f.read()

    def read_gen(self,chunk_size:int=1024)->Generator[bytes, None, int]:
        """Yield the ball's content from disk in fixed-size chunks.

        Args:
            chunk_size: Number of bytes to read per iteration. Defaults to
                ``1024``.

        Yields:
            ``bytes`` chunks of up to ``chunk_size`` bytes.

        Returns:
            Total number of bytes read.
        """
        with open(self.path,"rb") as f:
            size = 0
            while True:
                data = f.read(chunk_size)
                if not data:
                    return size
                size += len(data)
                yield data
    def __eq__(self, __value: "Ball") -> bool:
        return self.checksum == __value.checksum 

    def __str__(self):
        return "Ball(key={}, checksum={}, size={}, content_type={})".format(self.key,self.checksum,self.size,self.content_type)


