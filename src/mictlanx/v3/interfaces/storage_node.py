from mictlanx.v3.interfaces.payloads import Payload
class PutPayload(Payload):
    def __init__(self,*args,**kwargs):
        super(PutPayload,self).__init__(*args,**kwargs)
        self.bytes = kwargs.get("bytes")
class PutResponse(object):
    def __init__(self,*args,**kwargs):
        self.key          = kwargs.get("key")
        self.url          = kwargs.get("url")
        self.throughput   = kwargs.get("throughput")
        self.size         = kwargs.get("size")
        self.service_time = kwargs.get("service_time")
    def __str__(self):
        return "PutPayload(key={}, service_time={})".format(self.key,self.service_time)