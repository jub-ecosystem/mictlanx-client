from typing import List,Dict,Optional,Any
import humanfriendly as HF
from pydantic import BaseModel,Field,model_validator
from enum import IntEnum


class ExposedPort(BaseModel):
    ip_addr:Optional[str] = None 
    host_port:int
    container_port:int
    protocol:Optional[str]=None
    
    

class MountType(IntEnum):
    BIND   = 0
    VOLUME = 1
    TMPFS  = 2
    NPIPE  = 3
    EMPTY  = 4
class MountX(BaseModel):
    # adapt fields as in your project
    mount_type: MountType
    source: str
    target: str




class SummonContainerPayload(BaseModel):
    # required
    container_id: str
    image: str
    hostname: str
    exposed_ports: List[ExposedPort]
    envs: Optional[Dict[str, str]]={}
    memory: Optional[int] = HF.parse_size("1GB")  # default to 1GB if not provided
    cpu_count: Optional[int]=1
    mounts: Optional[List[MountX]]=[]
    network_id: str

    # optional with defaults/behavior
    selected_node: Optional[str] = None
    labels: Dict[str, str] = Field(default_factory=dict)
    force: Optional[bool] = True                    # original: force.unwrap_or(True)
    ip_addr: Optional[str] = None         # original: unwrap_or(container_id)
    shm_size: Optional[str] = None        # human-size string, parsed on dump

    @model_validator(mode="after")
    def _apply_defaults_and_merge_labels(self) -> "SummonContainerPayload":
        # ip_addr defaults to container_id if not provided
        if self.ip_addr is None:
            object.__setattr__(self, "ip_addr", self.container_id)

        # merge labels with {"target": "mictlanx"} (yours added at dump-time; we can merge now)
        merged = dict(self.labels)
        merged.setdefault("target", "mictlanx")
        object.__setattr__(self, "labels", merged)

        return self

    def to_payload_dict(self) -> Dict[str, Any]:
        """
        Emit the dict your original `to_dict()` produced.
        (Note: original code did not include `ip_addr`; we keep that behavior.)
        """
        mounts_dump = [m.model_dump() for m in self.mounts]

        data: Dict[str, Any] = {
            "container_id": self.container_id,
            "image": self.image,
            "hostname": self.hostname,
            "exposed_ports": [p.model_dump() for p in self.exposed_ports],
            "envs": self.envs,
            "memory": self.memory,
            "cpu_count": self.cpu_count,
            "mounts": mounts_dump,
            "network_id": self.network_id,
            "labels": self.labels,
            "force": self.force,
        }

        if self.selected_node is not None:
            data["selected_node"] = self.selected_node

        if self.shm_size is not None:
            # replace with: HF.parse_size(self.shm_size) if you have that helper
            data["shm_size"] = HF.parse_size(self.shm_size)

        return data


