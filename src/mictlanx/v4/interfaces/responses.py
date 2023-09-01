class PutMetadataResponse(object):
    def __init__(self, key:str, service_time:int, task_id:str):
        self.key = key 
        self.service_time = service_time 
        self.task_id = task_id 
class PutDataResponse(object):
    def __init__(self,service_time:int,throughput:float):
        self.service_time = service_time 
        self.throughput = throughput
class PutResponse(object):
    def __init__(self,response_time:int,throughput:float):
        self.response_time = response_time
        self.throughput    = throughput