from typing import List
from mictlanx.v4.interfaces.index import Peer
from typing import Iterator,Tuple
import hashlib as H

# 
class Utils(object):
    # 
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
    def calculate_disk_uf(total:int,used:int,size:int = 0 ):
        return  1 - ((total - (used + size))/total)


if __name__ =="__main__":
    peers_strs = "mictlanx-peer-0:alpha.tamps.cinvestav.mx/v0/mictlanx/peer0:-1 mictlanx-peer-1:alpha.tamps.cinvestav.mx/v0/mictlanx/peer1:-1"
    peers = Utils.peers_from_str_v2(peers_str=peers_strs,protocol="https")
    for p in peers:
        print(p.base_url())