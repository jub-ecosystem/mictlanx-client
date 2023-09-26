from mictlanx.v4.interfaces.index import Peer
from typing import Iterator,Tuple
import hashlib as H

# 
class Utils(object):
    # 
    def peers_from_str(peers_str:str,separator:str=" ")->Iterator[Peer]:
        splitted = peers_str.split(separator)
        splitted = map(lambda x: x.split(":"), splitted)
        return map(lambda x: Peer(peer_id=x[0],ip_addr=x[1], port=int(x[2])), splitted) 
    # move to Xolo.
    # def sha256(value:bytes)->str:
    #     h = H.sha256()
    #     h.update(value)
    #     return h.hexdigest()
    # def sha256_file(path:str)->Tuple[str,int]:
    #     h = H.sha256()
    #     size = 0
    #     with open(path,"rb") as f:
    #         while True:
    #             data = f.read()
    #             if not data:
    #                 return (h.hexdigest(),size)
    #             size+= len(data)
    #             h.update(data)
    
        
                # pass

