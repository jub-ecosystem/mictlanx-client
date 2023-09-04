from typing import List
class Peer(object):
    def __init__(self, node_id:str, ip_addr:str, port:int):
        self.node_id = node_id
        self.ip_addr = ip_addr
        self.port = port
    def empty():
        return Peer(node_id="",ip_addr="",port=-1)
    def get_addr(self)->str :
        return "{}:{}".format(self.ip_addr,self.port)
    def http_url(self):
        return "http://{}:{}".format(self.ip_addr,self.port)
    def __str__(self):
        return "Peer(id = {}, ip_addr={}, port={})".format(self.node_id, self.ip_addr,self.port)


class BallContext(object):
    def __init__(self,size:int, locations:List[str]):
        self.size = size
        self.locations = locations