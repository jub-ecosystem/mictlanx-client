from typing import List
from typing import Iterator
import hashlib as H
import humanfriendly as HF
from concurrent.futures import ThreadPoolExecutor,as_completed
from typing import Generator,Any,Tuple
import os
from xolo.utils.utils import Utils as XoloUtils
from collections import namedtuple
from pathlib import Path
from option import Some,Option,NONE
import re
from mictlanx.v4.interfaces.index import Router,Peer

# 
FileInfoBase = namedtuple("FileInfo","path checksum size")
class FileInfo(FileInfoBase):
    def upadate_path_relative_to(self,relative_to:str):
        _relative_to = Path(relative_to)
        path = Path(self.path).relative_to(_relative_to)
        return FileInfoBase(str(path),self.checksum,self.size)

class Utils(object):

    @staticmethod
    def to_gen_bytes(data:bytes,chunk_size:str="1MB")->Generator[bytes, None,None]:
        cs = HF.parse_size(chunk_size)
        for i in range(0, len(data), cs):
            yield data[i:i + cs]

    @staticmethod
    def extract_path_info(path:str)->Tuple[str,str,str]:
        fullname = os.path.basename(path)
        fullname_spliited = fullname.split(".")
        ext = ""
        if len(fullname_spliited) >1:
            ext = fullname_spliited[1]
        filename = fullname_spliited[0]
        return fullname,filename,ext
            
        # fullname = 
    @staticmethod
    def sanitize_str(x:str):
        _x = re.sub(r'[^a-z0-9_]', '', x.lower().strip())
        return _x

    @staticmethod
    def get_or_default(iterator:List[Any],i:int=0,default = None)->Option[Any]:
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
        except Exception as e:
            return NONE if default is None else Some(default)

    @staticmethod
    def get_checksums_and_sizes(path:str,max_workers:int = 2)->Generator[FileInfo,None,None]:
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
        _chunk_size = HF.parse_size(chunk_size)
        with open(path,"rb") as f:
            while True:
                value:bytes                     = f.read(_chunk_size)
                if not value:
                    break
                yield value
                
    @staticmethod
    def peers_from_str(peers_str:str,separator:str=" ")->Iterator[Peer]:
        splitted = peers_str.split(separator)
        splitted = map(lambda x: x.split(":"), splitted)
        return map(lambda x: Peer(peer_id=x[0],ip_addr=x[1], port=int(x[2])), splitted) 

    # mictlanx-peer-0:alpha.tamps.cinvestav.mx/v0/mictlanx:-1
    @staticmethod
    def peers_from_str_v2(peers_str:str,separator:str=" ",protocol:str="http")->Iterator[Peer]:
        splitted = peers_str.split(separator)
        splitted = map(lambda x: x.split(":"), splitted)
        def __inner(x:List[str]):
            return  Peer(peer_id=x[0],ip_addr=x[1], port=int(x[2]) ,protocol=protocol)
        return map(__inner, splitted) 

    @staticmethod
    def routers_from_str(routers_str:str,separator:str=" ",protocol:str="http")->Iterator[Router]:
        splitted = routers_str.split(separator)
        splitted = map(lambda x: x.split(":"), splitted)
        def __inner(x:List[str]):
            return  Router(
                router_id=x[0],
                protocol=protocol,
                ip_addr=x[1],
                port=int(x[2]),
            )
        return map(__inner, splitted) 
    
    @staticmethod
    def calculate_disk_uf(total:int,used:int,size:int = 0 ):
        return  1 - ((total - (used + size))/total)


if __name__ =="__main__":
    x = Utils.sanitize_str(x="^^##^Y#@#@3211X.x-@aks-d_")
    print(x)
    # xs = map(lambda x: x. upadate_path_relative_to(relative_to="/sink/client1/bucket1"),Utils.get_checksums_and_sizes(path="/sink/client1"))
    # for (path,checksum,size) in xs :
        # print(path,checksum,size)
    # peers_strs = "mictlanx-peer-0:alpha.tamps.cinvestav.mx/v0/mictlanx/peer0:-1 mictlanx-peer-1:alpha.tamps.cinvestav.mx/v0/mictlanx/peer1:-1"
    # peers = Utils.peers_from_str_v2(peers_str=peers_strs,protocol="https")
    # for p in peers:
    #     print(p.base_url())