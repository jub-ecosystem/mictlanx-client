from mictlanx.services import AsyncRouter
# from mictlanx.interfaces import AsyncRouter
from typing import List,Dict

class RouterLoadBalancer():
    """Least-connections load balancer across a set of :class:`AsyncRouter` nodes.

    Tracks a per-router operation counter and always routes new requests to
    the router with the fewest in-flight operations.
    """

    def __init__(self, routers:List[AsyncRouter]):
        """Initialise the load balancer with a pool of routers.

        Args:
            routers: List of :class:`AsyncRouter` objects to balance across.
        """
        self.routers:Dict[str, AsyncRouter] = dict(map(lambda x :(x.router_id, x), routers))
        self.operation_counter:Dict[str, int] = dict(list(map(lambda x:(x[0],0), self.routers.items())))

    def get_router(self,router_id:str ="",bucket_id:str="",key:str=""):
        """Select a router for the next operation.

        If ``router_id`` is provided and matches a known router it is returned
        directly.  Otherwise the router with the fewest operations is chosen.

        Args:
            router_id: Optional explicit router identifier. Defaults to ``""``.
            bucket_id: Reserved for future key-affinity routing. Defaults to
                ``""``.
            key: Reserved for future key-affinity routing. Defaults to ``""``.

        Returns:
            The selected :class:`AsyncRouter`.
        """
        if router_id in self.routers:
            self.operation_counter.setdefault(router_id,0)
            self.operation_counter[router_id]+=1
            return self.routers[router_id]
        min_router = min(self.operation_counter.items(), key=lambda x : x[1])
        if min_router:
            router_id = min_router[0]
            self.operation_counter.setdefault(router_id,0)
            self.operation_counter[router_id]+=1
            return self.routers[router_id]
        else:
            x= list(self.routers.values())[0]
            return x


