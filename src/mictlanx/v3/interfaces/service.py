from abc import ABC
class Service(ABC):
    def __init__(self,**kwargs):
        self.api_version:int = kwargs.get("api_version")
        self.ip_addr:str     = kwargs.get("ip_addr")
        self.port:int        = kwargs.get("port")
    # def base_url(self,**kwrargs):
        self.base_url = "http://{}:{}/api/v{}".format(
            self.ip_addr,
            self.port,
            self.api_version
        )