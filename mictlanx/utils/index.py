from typing import List
import humanfriendly as HF
from concurrent.futures import ThreadPoolExecutor,as_completed
from typing import Generator,Any,Tuple,AsyncGenerator
import os
from xolo.utils.utils import Utils as XoloUtils
from collections import namedtuple
from pathlib import Path
from option import Some,Option,NONE
import re
# from mictlanx.services import AsyncPeer
# from mictlanx.services.sync import Router,Peer

# 
FileInfoBase = namedtuple("FileInfo","path checksum size")

class FileInfo(FileInfoBase):
    """Immutable record holding a file's absolute path, SHA-256 checksum, and size."""

    def update_path_relative_to(self,relative_to:str):
        """Return a new ``FileInfo`` with the path made relative to ``relative_to``.

        Args:
            relative_to: Base directory to relativise the path against.

        Returns:
            A new ``FileInfoBase`` with the relative path.
        """
        _relative_to = Path(relative_to)
        path = Path(self.path).relative_to(_relative_to)
        return FileInfoBase(str(path),self.checksum,self.size)

class Utils(object):
    """Collection of general-purpose static utility helpers."""

    @staticmethod
    def camel_to_snake(x:str):
        """Convert a CamelCase string to UPPER_SNAKE_CASE.

        Args:
            x: Input string in CamelCase (e.g. ``"NotFoundError"``).

        Returns:
            Upper-snake-case string (e.g. ``"NOT_FOUND_ERROR"``).
        """
        x1 = re.sub(r'([a-z])([A-Z])', r'\1_\2', x)
        x2 = re.sub(r'([A-Z])([A-Z][a-z])', r'\1_\2', x1)  # For cases like "HTTPServer"
        return (x2[0] + x2[1:]).upper()

    @staticmethod
    def to_gen_bytes(data:bytes,chunk_size:str="1MB")->Generator[bytes, None,None]:
        """Yield ``data`` in fixed-size byte chunks.

        Args:
            data: Source bytes to split.
            chunk_size: Human-readable chunk size string (e.g. ``"1MB"``).
                Defaults to ``"1MB"``.

        Yields:
            ``bytes`` slices of at most ``chunk_size`` bytes.
        """
        cs = HF.parse_size(chunk_size)
        for i in range(0, len(data), cs):
            yield data[i:i + cs]

    @staticmethod
    async def to_async_gen_bytes(data:bytes,chunk_size:str="1MB")->AsyncGenerator[bytes, None]:
        """Async variant of :meth:`to_gen_bytes` — yields byte chunks asynchronously.

        Args:
            data: Source bytes to split.
            chunk_size: Human-readable chunk size string. Defaults to ``"1MB"``.

        Yields:
            ``bytes`` slices of at most ``chunk_size`` bytes.
        """
        cs = HF.parse_size(chunk_size)
        for i in range(0, len(data), cs):
            yield data[i:i + cs]

    @staticmethod
    def split_path(path: str,is_file:bool = True) -> Tuple[str, str, str]:
        """Split a filesystem path into its directory, stem, and extension.

        Args:
            path: Absolute or relative path to split.
            is_file: When ``False`` the path is treated as a directory and
                the stem/extension are returned as empty strings.
                Defaults to ``True``.

        Returns:
            Tuple of ``(parent_directory, filename_without_extension, extension)``
            where ``extension`` has no leading dot.
        """
        path = Path(path)

        # is_file = os.path.isfile(path=path)
        if not is_file:
            return (str(path),"","")  # Not a file with extension

        parent = str(path.parent)
        filename = path.stem      # name without extension
        extension = path.suffix.replace(".","")   # includes the dot, e.g. '.txt'

        return parent, filename, extension

    @staticmethod
    def extract_path_info(path:str)->Tuple[str,str,str]:
        """Extract the full filename, base name, and extension from a path.

        Args:
            path: Path string to inspect.

        Returns:
            Tuple of ``(fullname, filename_without_extension, extension)``
            where ``extension`` has no leading dot.
        """
        fullname = os.path.basename(path)
        fullname_spliited = fullname.split(".")
        ext = ""
        if len(fullname_spliited) >1:
            ext = fullname_spliited[1]
        filename = fullname_spliited[0]
        return fullname,filename,ext
            
        # fullname = 
    @staticmethod
    def sanitize_str(x: str) -> str:
        """Sanitise a string to a safe alphanumeric-with-dashes identifier.

        Strips characters that are not alphanumeric, ``-``, or ``_``;
        collapses repeated separators; and trims leading/trailing dashes.

        Args:
            x: Input string to sanitise.

        Returns:
            Sanitised string, or ``""`` if the result does not match the
            safe identifier pattern.
        """
        pattern = re.compile(r'^[a-zA-Z0-9]+([a-zA-Z0-9]|[-_](?=[a-zA-Z0-9]))*$')
        sanitized = ''.join(re.findall(r'[a-zA-Z0-9-_]', x))
        sanitized = re.sub(r'[-_]{2,}', '-', sanitized)
        sanitized = re.sub(r'^-|-$', '', sanitized)
        if pattern.match(sanitized):
            return sanitized
        else:
            return ''

    @staticmethod
    def get_or_default(iterator:List[Any],i:int=0,default = None)->Option[Any]:
        """Return the element at index ``i`` from ``iterator``, wrapped in ``Option``.

        Args:
            iterator: Sequence to index into.
            i: Index to retrieve. Defaults to ``0``.
            default: Fallback value when the element cannot be retrieved.
                When ``None`` the fallback is ``NONE``. Defaults to ``None``.

        Returns:
            ``Some(element)`` on success, ``Some(default)`` or ``NONE`` on
            failure/out-of-bounds.
        """
        n = len(iterator)
        try:
            if n ==0:
                return NONE if default is None else Some(default)
            elif n == 1:
                return Some(iterator[0])
            elif i >= n:
                return NONE if default is None else Some(default)
            else:
                return Some(iterator[i])
        except Exception:
            return NONE if default is None else Some(default)

    @staticmethod
    def get_checksums_and_sizes(path:str,max_workers:int = 2)->Generator[FileInfo,None,None]:
        """Walk a directory tree and yield a :class:`FileInfo` for every file.

        SHA-256 checksums are computed in parallel using a thread pool.

        Args:
            path: Root directory to walk.
            max_workers: Number of threads for concurrent checksum
                computation. Defaults to ``2``.

        Yields:
            :class:`FileInfo` named tuples (path, checksum, size).
        """
        futures = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as tp:
            for (root,_, fullnames) in os.walk(path):
                for fullname in fullnames:
                    file_path = "{}/{}".format(root,fullname)
                    fut = tp.submit(XoloUtils.extract_path_sha256_size, path = file_path )
                    futures.append(fut)
            for future in as_completed(futures):
                result =FileInfo(*future.result())
                yield result



    @staticmethod
    def file_to_chunks_gen(path:str, chunk_size:str="1MB"):
        """Yield a file's contents in fixed-size byte chunks.

        Args:
            path: Path to the file to read.
            chunk_size: Human-readable chunk size string (e.g. ``"1MB"``).
                Defaults to ``"1MB"``.

        Yields:
            ``bytes`` slices of at most ``chunk_size`` bytes.
        """
        _chunk_size = HF.parse_size(chunk_size)
        with open(path,"rb") as f:
            while True:
                value:bytes                     = f.read(_chunk_size)
                if not value:
                    break
                yield value
                
    @staticmethod
    def calculate_disk_uf(total:int,used:int,size:int = 0 ):
        """Calculate the disk utilisation factor after a hypothetical write.

        Args:
            total: Total disk capacity in bytes.
            used: Currently used bytes.
            size: Hypothetical additional bytes. Defaults to ``0``.

        Returns:
            Float in ``[0.0, 1.0]`` representing disk fullness.
        """
        return  1 - ((total - (used + size))/total)
