from .responses import *
from .index import *

@dataclass
class ElasticResponse:
    pool_size:int
    response_time:float
@dataclass
class ReplicationResponse:
    replication_event_id:str
    response_time:float
    