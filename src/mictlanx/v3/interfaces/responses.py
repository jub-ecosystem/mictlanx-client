
class GenerateTokenResponse(object):
    def __init__(self,**kwargs):
        self.client_id    = kwargs.get("client_id")
        self.token        = kwargs.get("token")
        self.jti          = kwargs.get("jti")
        self.service_time = kwargs.get("service_time")
    def __str__(self):
        return "GenerateTokenResponse(client_id={})".format(self.client_id)


class GetInMemoryResponse(object):
    def __init__(self,**kwargs):
        self.node_id       = kwargs.get("key")
        self.response_time = kwargs.get("response_time")
        self.bytes         = kwargs.get("bytes")
# class GetFileResponse(object):
#     def __init__(self,**kwargs):
#         self.node_id       = kwargs.get("key")
#         self.response_time = kwargs.get("response_time")
#         self.file          = kwargs.get("file")