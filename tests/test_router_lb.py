import pytest
from mictlanx.asyncx.lb import RouterLoadBalancer
import mictlanx.interfaces as InterfaceX



@pytest.mark.skip("")
def test_get_router():
    rlb = RouterLoadBalancer(routers=[
        InterfaceX.Router(ip_addr="",protocol="",router_id="r0",port=0) , 
        InterfaceX.Router(ip_addr="",protocol="",router_id="r1",port=0)
    ])

    for i in range(10):
        sr = rlb.get_router()
        print(i,sr)