from abc import ABC
from option import Option,NONE
class Service(ABC):
    def __init__(self,ip_addr:str, port:int, api_version:Option[int]=NONE):
        self.ip_addr:str     = ip_addr
        self.port:int        = port
        self.api_version:int = api_version.unwrap_or(3)
        self.base_url = "http://{}:{}/api/v{}".format(
            self.ip_addr,
            self.port,
            self.api_version
        )