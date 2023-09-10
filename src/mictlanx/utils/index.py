from mictlanx.v4.interfaces.index import Peer
from typing import Iterator

class Utils(object):
    def peers_from_str(peers_str:str,separator:str=" ")->Iterator[Peer]:
        splitted = peers_str.split(separator)
        splitted = map(lambda x: x.split(":"), splitted)
        return map(lambda x: Peer(peer_id=x[0],ip_addr=x[1], port=int(x[2])), splitted)
