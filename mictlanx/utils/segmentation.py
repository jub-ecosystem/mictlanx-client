from __future__ import annotations
from option import Option,NONE,Some
from typing import Dict,Iterator, List,Any,Callable,Tuple,Generator,AsyncGenerator,Union
import mictlanx.interfaces as InterfaceX
import humanfriendly as HF
import numpy as np
import math
import numpy.typing as npt
import hashlib as H
import os
import pickle as PK


class Chunk(object):
    """A single, self-contained piece of a segmented ball.

    Each chunk carries its raw bytes, a SHA-256 checksum, a zero-based
    ``index``, and a ``group_id`` that ties it back to the parent ball.
    Chunks are the unit of storage on peers.
    """

    def __init__(self,group_id:str,index:int,data:bytes,chunk_id:Option[str]=NONE,metadata:Dict[str,str]={}):
        """Initialise a chunk from raw bytes and compute its SHA-256 checksum.

        Args:
            group_id: Identifier of the parent ball (e.g. the ``ball_id``).
            index: Zero-based position of this chunk within the ball.
            data: Raw byte content of the chunk.
            chunk_id: Optional override for the chunk identifier; defaults
                to the SHA-256 checksum.
            metadata: Additional key/value tags merged with automatic fields
                (``index``, ``chunk_size``, ``group_id``).
        """
        self.group_id = group_id
        self.index    = index
        self.size     = len(data)
        self.data     = data
        self.metadata = {**metadata, "index":str(index), "chunk_size":str(self.size),"group_id":self.group_id}
        hasher = H.sha256()
        hasher.update(self.data)
        self.checksum = hasher.hexdigest()
        self.chunk_id = chunk_id.unwrap_or(self.checksum)
    def __str__(self):
        return "Chunk(chunk_id={}, index={}, size={})".format(self.chunk_id,self.index,self.size)
    @staticmethod
    def from_ndarray(group_id:str,index:int,ndarray:npt.NDArray, metadata:Dict[str,str]={}, chunk_id:Option[str]=NONE):
        """Create a chunk from a NumPy array, storing shape/dtype in metadata.

        The array is serialised to C-order bytes via ``ndarray.tobytes()``.
        Shape, attribute count, record count, and dtype are stored in the
        chunk's metadata so the array can be reconstructed later.

        Args:
            group_id: Parent ball identifier.
            index: Zero-based chunk index.
            ndarray: NumPy array to serialise.
            metadata: Additional metadata tags.
            chunk_id: Optional chunk identifier override.

        Returns:
            A new :class:`Chunk` with serialised array data.
        """
        shape_len = len(ndarray.shape)
        metadata["shape"] = str(ndarray.shape)
        metadata["attributes"] = str(ndarray.shape[1] if shape_len > 1 else 1)
        metadata["records"] = str(ndarray.shape[0])
        metadata["dtype"] = str(ndarray.dtype)
        return Chunk(group_id=group_id,index= index, data = ndarray.tobytes(order="C"), metadata=metadata,chunk_id=chunk_id )

    @staticmethod
    def from_list(group_id:str, index:int,xs:List[Any], metadata:Dict[str,str]={} , chunk_id:Option[str]=NONE):
        """Create a chunk from a Python list by pickling it.

        Args:
            group_id: Parent ball identifier.
            index: Zero-based chunk index.
            xs: List to pickle and store.
            metadata: Additional metadata tags.
            chunk_id: Optional chunk identifier override.

        Returns:
            A new :class:`Chunk` with pickled list data.
        """
        data =PK.dumps(xs)
        return Chunk(group_id=group_id,index= index, data = data, metadata=metadata,chunk_id=chunk_id )
    @staticmethod
    def from_bytes(group_id:str, index:int,data:bytes, metadata:Dict[str,str]={} , chunk_id:Option[str]=NONE):
        """Create a chunk directly from raw bytes.

        Args:
            group_id: Parent ball identifier.
            index: Zero-based chunk index.
            data: Raw bytes payload.
            metadata: Additional metadata tags.
            chunk_id: Optional chunk identifier override.

        Returns:
            A new :class:`Chunk`.
        """
        return Chunk(group_id=group_id,index= index, data = data, metadata=metadata,chunk_id=chunk_id )
    
    def to_list(self)->Option[List[Any]]:
        """Deserialise the chunk's data back into a Python list (via pickle).

        Returns:
            ``Some(list)`` on success, ``NONE`` if the data cannot be unpickled.
        """
        try:
            xs = PK.loads(self.data)
            return Some(xs)
        except Exception:
            return NONE

    def to_ndarray(self)->Option[npt.NDArray]:
        """Reconstruct the NumPy array stored in this chunk.

        Reads ``shape`` and ``dtype`` from ``self.metadata`` to reshape
        the raw bytes back into the original array.

        Returns:
            ``Some(ndarray)`` on success, ``NONE`` on any error.
        """
        try:
            shape   = eval(self.metadata.get("shape"))
            dtype   = self.metadata.get("dtype","float64")
            ndarray = np.frombuffer(self.data,dtype=dtype).reshape(shape)
            return Some(ndarray)
        except Exception:
            return NONE
        
    def to_generator(self, chunk_size:str="256kb")->Generator[bytes,None,None]:
        """Generator that yields chunks of `chunk_size` from `data`."""
        # mv = memoryview(self.data)  # ✅ No data copying
        _cs = HF.parse_size(chunk_size)
        for i in range(0, self.size, _cs):
            yield self.data[i:i + _cs]
    
    async def to_async_generator(self, chunk_size:str="256kb")->AsyncGenerator[bytes,None,None]:
        """Generator that yields chunks of `chunk_size` from `data`."""
        _cs   = HF.parse_size(chunk_size)
        total = 0
        for i in range(0, self.size, _cs):
            part = self.data[i:i + _cs]
            part_size = len(part)
            total+= part_size
            yield part



class Chunks(object):
    """An ordered collection of :class:`Chunk` objects produced by segmenting a ball.

    :class:`Chunks` is the output of all ``from_*`` factory methods.  It
    stores the materialised chunk list and the original data size ``n``
    (in elements or bytes depending on the source).  Use the factory
    methods instead of the constructor directly.
    """

    def __init__(self,chs:Iterator[Chunk],n:int ,strict:bool = False):
        """Materialise an iterator of chunks into an ordered list.

        Args:
            chs: Iterator or generator of :class:`Chunk` objects.
            n: Total size of the original data (bytes or element count).
            strict: When ``True`` small leftover data at the end is kept as a
                separate chunk instead of being merged into the last chunk.
                Defaults to ``False``.
        """
        self.chunks:List[Chunk] = list(chs)
        self.n:int = n 
        self.strict = strict
    
    def sort(self,reverse:bool=False):
        """Sort chunks in-place by their ``index``.

        Args:
            reverse: When ``True`` sort in descending order. Defaults to
                ``False`` (ascending).
        """
        self.chunks.sort(key= lambda chunk: chunk.index,reverse=reverse)
    def __len__(self):
        return len(self.chunks)
    def __iter__(self):
        """
        Returns an iterator object.
        """
        self.current = 0
        return iter(self.chunks)
   
    def __next__(self):
        """
        Returns the next chunk of data.

        :return: A chunk of the data.
        :raises StopIteration: When all chunks are processed.
        """
        if self.current >= len(self.chunks):
            raise StopIteration  # No more chunks left

        chunk = self.chunks[self.current]
        self.current += 1
        return chunk
    
    def len(self)->int:
        """Return the total size of the original data (bytes or element count).

        Returns:
            The ``n`` value passed at construction.
        """
        return self.n

    def iter(self):
        """Return the underlying list of chunks (unsorted).

        Returns:
            ``List[Chunk]`` in insertion order.
        """
        return self.chunks

    def sorted_by(self,filter_by:Callable[[Chunk], Any] = lambda x:x.index,reverse:bool=False)->Iterator[Chunk]:
        """Return chunks sorted by an arbitrary key function.

        Args:
            filter_by: Key function applied to each chunk for sorting.
                Defaults to ``lambda x: x.index``.
            reverse: When ``True`` sort descending. Defaults to ``False``.

        Returns:
            Sorted iterator of :class:`Chunk` objects.
        """
        return sorted(self.chunks, key= filter_by,reverse=reverse)
    
    @staticmethod
    def _iter_to_chunks(
        group_id:str,
        iterable:Any,
        n:int,
        chunk_prefix:Option[str]=NONE,
        chunk_size:Union[Option[int], Option[str]]=NONE,
        num_chunks:int =1,
        strict:bool = False
    ):
        # THE RATIO OF RECORDS PER CHUNK (float)
        data_per_chunk     = chunk_size.unwrap_or(n / num_chunks)
        if isinstance(data_per_chunk, str):
            data_per_chunk = HF.parse_size(data_per_chunk)
        # Check if the data per chunk is lower or equal to the number of total elements. 
        dpc_is_lower_than_n = data_per_chunk <= n
        if not dpc_is_lower_than_n:
            data_per_chunk = n
        # data per chunk but int
        data_per_chunk_int = int(data_per_chunk)
        # Total number of chunked elements (chunked = element that belongs to a specific chunk)
        total_chunked_elements = 0
        # Current chunk index
        i                      = 0 
        # Check that total number of chunked elements is lower than the total number of elements
        chunks = []
        exact_num_chunks = str(math.ceil(n/data_per_chunk))
        while total_chunked_elements < n:
            # Difference between total number of elements and total chunked elements
            diff = n - total_chunked_elements
            # Chunk metadata 
            metadata = {"index": str(i),"num_chunks":exact_num_chunks }
            # Check if diff is lower than -> if it is lower then drain all the iterable. 
            if diff < data_per_chunk:
                current_total_records_sent = data_per_chunk_int*i
                total_chunked_elements     += n - current_total_records_sent
                records_chunk              = iterable[current_total_records_sent:]
                chunk_metadata             = chunks[-1]
          
                if not strict:
                    if isinstance(records_chunk, np.ndarray):
                        chunk_metadata["data"] = np.concatenate([chunk_metadata["data"], records_chunk])
                    else:
                        chunk_metadata["data"] = chunk_metadata["data"]+records_chunk
                else:
                    chunks.append({'group_id':group_id, 'index':i, 'data':records_chunk, 'metadata':metadata})
                

                
                if chunk_prefix.is_some:
                    chunk_metadata["chunk_id"] ="{}_{}".format(chunk_prefix.unwrap(),i-1)
            else:
                total_chunked_elements += data_per_chunk_int
                from_index             = i*data_per_chunk_int
                to_index               = ((i+1)*data_per_chunk_int)
                records_chunk          = iterable[from_index: to_index]
                chunk_metadata = {'group_id':group_id, 'index':i, 'data':records_chunk, 'metadata':metadata}
      
                if chunk_prefix.is_some:
                    chunk_metadata["chunk_id"] ="{}_{}".format(chunk_prefix.unwrap(),i)
                i+=1
                chunks.append(chunk_metadata)
        return chunks
   
    @staticmethod
    def iter_to_chunks(group_id:str,iterable:Any,n:int,chunk_prefix:Option[str]=NONE,chunk_size:Option[int]=NONE,num_chunks:int =1):
        """Yield raw chunk metadata dicts from an indexable iterable.

        Unlike :meth:`_iter_to_chunks`, this method yields lazily and does not
        merge small trailing pieces.

        Args:
            group_id: Parent ball identifier.
            iterable: Any indexable sequence (bytes, list, ndarray).
            n: Total number of elements in ``iterable``.
            chunk_prefix: Optional prefix for ``chunk_id`` values.
            chunk_size: Fixed size per chunk in elements/bytes.
            num_chunks: Target number of chunks (used when ``chunk_size`` is
                ``NONE``). Defaults to ``1``.

        Yields:
            Dicts with keys ``group_id``, ``index``, ``data``, ``metadata``
            and optionally ``chunk_id``.
        """
        # hashing
        # THE RATIO OF RECORDS PER CHUNK (float)
        data_per_chunk     = chunk_size.unwrap_or(n / num_chunks)
        # Check if the data per chunk is lower or equal to the number of total elements. 
        assert(data_per_chunk <= n)
        # data per chunk but int
        data_per_chunk_int = int(data_per_chunk)
        # Total number of chunked elements (chunked = element that belongs to a specific chunk)
        total_chunked_elements = 0
        # Current chunk index
        i                      = 0 
        # Check that total number of chunked elements is lower than the total number of elements
        while total_chunked_elements < n:
            # Difference between total number of elements and total chunked elements
            diff = n - total_chunked_elements
            # Chunk metadata 
            metadata = {"index": str(i)}
            # Check if diff is lower than -> if it is lower then drain all the iterable. 
            if diff < data_per_chunk:
                current_total_records_sent = data_per_chunk_int*i
                total_chunked_elements     += n - current_total_records_sent
                records_chunk              = iterable[current_total_records_sent:]
            else:
                total_chunked_elements += data_per_chunk_int
                from_index             = i*data_per_chunk_int
                to_index               = ((i+1)*data_per_chunk_int)
                records_chunk          = iterable[from_index: to_index]
            # hasher.update(records_chunk)
            chunk_metadata = {'group_id':group_id, 'index':i, 'data':records_chunk, 'metadata':metadata}
            if chunk_prefix.is_some:
                chunk_metadata["chunk_id"] ="{}_{}".format(chunk_prefix.unwrap(),i)

            i+=1
            yield chunk_metadata
    

    @staticmethod
    def from_list(xs:List[Any], group_id:str,chunk_prefix:Option[str]=NONE,chunk_size:Option[int] = NONE,num_chunks:int = 1):
        """Segment a Python list into chunks (pickled per chunk).

        Args:
            xs: Source list to segment.
            group_id: Parent ball identifier.
            chunk_prefix: Optional chunk ID prefix.
            chunk_size: Fixed number of list elements per chunk.
            num_chunks: Target number of chunks when ``chunk_size`` is
                ``NONE``. Defaults to ``1``.

        Returns:
            ``Some(Chunks)`` on success, ``NONE`` on error.
        """
        try:
            n = len(xs)
            def __inner():
                _num_chunks = n if  n < num_chunks else num_chunks
                _xs= Chunks._iter_to_chunks(
                    iterable=xs,
                    group_id = group_id,
                    n = n,
                    num_chunks=_num_chunks,
                    chunk_size=chunk_size,
                    chunk_prefix=chunk_prefix
                )
                for i,x in enumerate(_xs):
                    chunk_id       = Some(x.get("chunk_id",None)).filter(lambda x: x is not None)
                    chunk          = Chunk.from_list(group_id = group_id, index = x["index"], xs=x["data"],metadata = x['metadata'],chunk_id=chunk_id)
                    yield chunk
            return Some(Chunks(chs= __inner() , n = n ))
        except Exception:
            return NONE      
        
    @staticmethod
    def from_ndarray(ndarray:npt.NDArray, group_id:str,chunk_prefix:Option[str]=NONE,chunk_size:Option[int] = NONE,num_chunks:int = 1 )->Option[Chunks]:
        """Segment a NumPy array row-wise into chunks.

        Each chunk stores a slice of rows serialised via
        :meth:`Chunk.from_ndarray`.

        Args:
            ndarray: Source array to segment (first dimension is the row axis).
            group_id: Parent ball identifier.
            chunk_prefix: Optional chunk ID prefix.
            chunk_size: Number of rows per chunk.
            num_chunks: Target number of chunks when ``chunk_size`` is
                ``NONE``. Defaults to ``1``.

        Returns:
            ``Some(Chunks)`` on success, ``NONE`` on error.
        """
        try:
            def __inner():
                n = ndarray.shape[0]
                _num_chunks = n if  n < num_chunks else num_chunks
                xs= Chunks._iter_to_chunks(
                    iterable=ndarray,
                    group_id = group_id,
                    n = n,
                    num_chunks=_num_chunks,
                    chunk_size=chunk_size,
                    chunk_prefix=chunk_prefix
                )
                for i,x in enumerate(xs):
                    chunk_id       = Some(x.get("chunk_id",None)).filter(lambda x: x is not None)
                    chunk          = Chunk.from_ndarray(
                        group_id = group_id,
                        index    = x["index"],
                        ndarray  = x["data"],
                        metadata = x['metadata'],
                        chunk_id = chunk_id
                    )
                    yield chunk
            chs = __inner()
            return Some(Chunks(chs= chs , n = ndarray.shape[0]))
        except Exception:
            return NONE

    @staticmethod
    def from_file(path:str,group_id:str,chunk_size:Option[int] = NONE,num_chunks:int =1)->Option[Chunks]:
        """Read a file from disk and segment it into byte chunks.

        Args:
            path: Absolute path to the file.
            group_id: Parent ball identifier (also used as chunk ID prefix).
            chunk_size: Fixed byte size per chunk.  When ``NONE`` the size is
                derived from ``num_chunks``.
            num_chunks: Target number of chunks when ``chunk_size`` is
                ``NONE``. Defaults to ``1``.

        Returns:
            ``Some(Chunks)`` on success, ``NONE`` if the file is empty or an
            error occurs.
        """
        try:
            file_size:int              = os.path.getsize(path)
            if file_size <= 0:
                return NONE
            if chunk_size.is_some:
                effective_chunk_size = chunk_size.unwrap()
            else:
                if num_chunks <= 0:
                    raise ValueError("num_chunks must be >= 1")
                effective_chunk_size = max(1024, file_size // num_chunks)

            
            def __inner():
                with open(path,"rb") as f:
                    i=0
                    while True:
                        # metadata = {"index":str(i)}
                        metadata = {}
                        cid = Some(f"{group_id}_{i}")
                        data = f.read(effective_chunk_size)
                        if not data:
                            break
                        # metadata["index"]= str(i)
                        yield Chunk(
                            group_id=group_id,
                            chunk_id=cid,
                            index=i,
                            data=data,
                            metadata=metadata
                        )
                        i += 1
            return Some(Chunks(chs=__inner() , n = file_size))
            # return __inner()
        except Exception:
            return NONE

    @staticmethod
    def from_bytes(data:bytes,group_id:str,chunk_size:Option[int] = NONE,num_chunks:int =1,chunk_prefix:Option[str]=NONE)->Option[Chunks]:
        """Segment a raw bytes object into fixed-size chunks.

        Args:
            data: Source bytes to segment.
            group_id: Parent ball identifier.
            chunk_size: Fixed byte size per chunk.
            num_chunks: Target number of chunks when ``chunk_size`` is
                ``NONE``. Defaults to ``1``.
            chunk_prefix: Optional chunk ID prefix.

        Returns:
            ``Some(Chunks)`` always (errors are swallowed into empty chunks).
        """
        def __inner():
            xs = Chunks._iter_to_chunks(
                iterable     = data,
                group_id     = group_id,
                num_chunks   = num_chunks,
                n            = len(data),
                chunk_size   = chunk_size,
                chunk_prefix = chunk_prefix
            ) 
            for x in xs:
                chunk_id       = Some(x.get("chunk_id",None)).filter(lambda x: x is not None)

                chunk = Chunk(group_id = group_id,chunk_id=chunk_id ,data=x["data"],index=x["index"], metadata = x["metadata"])
                yield chunk
        return Some(Chunks(chs = __inner(), n = len(data)))
        
    @staticmethod
    def from_generator(gen:Generator[bytes,None,None], group_id:str,chunk_size:Option[int] = NONE,num_chunks:int =1)->Option[Chunks]:
        """Consume a bytes generator, concatenate, then segment into chunks.

        Args:
            gen: Generator that yields ``bytes`` objects.
            group_id: Parent ball identifier (also used as chunk ID prefix).
            chunk_size: Fixed byte size per chunk.
            num_chunks: Target number of chunks when ``chunk_size`` is
                ``NONE``. Defaults to ``1``.

        Returns:
            ``Some(Chunks)`` on success.
        """
        _gen = b"".join(gen)
        return Chunks.from_bytes(
            data=_gen,
            group_id=group_id,
            chunk_prefix=Some(group_id),
            chunk_size=chunk_size,
            num_chunks=num_chunks,
        )
    

      
    def to_generator(self)->Generator[bytes,None,None]:
        """Yield the raw bytes of each chunk in insertion order.

        Yields:
            ``bytes`` data from each :class:`Chunk`.
        """
        for chunk in self.iter():
            yield chunk.data

    def to_bytes(self)->bytes:
        """Concatenate all chunks into a single bytes object.

        Returns:
            All chunk data joined in insertion order.
        """
        concatenated = bytearray().join(map(lambda x:x.data,self.iter()))
        return memoryview(concatenated).tobytes()

    def to_ndarray(self)->Option[Tuple[npt.NDArray,InterfaceX.ChunkMetadata]]:
        """Reconstruct the original NumPy array from all chunks.

        Chunks are sorted by index before reconstruction.  Requires that
        each chunk was created via :meth:`Chunk.from_ndarray`.

        Returns:
            ``Some((ndarray, ChunkMetadata))`` on success, ``NONE`` on any
            error (e.g. missing shape/dtype tags, inconsistent dtypes).
        """
        try:
            result   = []
            metadata = InterfaceX.ChunkMetadata(id="ID", size=0, checksum="",group_id="",tags={})
            hasher   = H.sha256()
            size     = 0
            for chunk in self.sorted_by(filter_by=lambda chunk:chunk.index):
                if not ("shape" in chunk.metadata or "dtype" in chunk.metadata):
                    return NONE
                dtype   = chunk.metadata.get("dtype","float64")
                shape   = eval(chunk.metadata.get("shape"))
                hasher.update(chunk.data)
                size    += len(chunk.data)
                ndarray = np.frombuffer(chunk.data,dtype= dtype).reshape(shape)
                result.append(ndarray)
            metadata.size     = size
            metadata.checksum = hasher.hexdigest()
            result = np.vstack(result)
            return Some((result,metadata))
        except Exception:
            return NONE


