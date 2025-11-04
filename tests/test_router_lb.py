import pytest
from mictlanx.asyncx.lb import RouterLoadBalancer
from mictlanx.services.router import AsyncRouter

# --- Fixtures ---

@pytest.fixture
def r0() -> AsyncRouter:
    """Provides a reusable router instance for router 'r0'."""
    return AsyncRouter(ip_addr="localhost", protocol="http", router_id="r0", port=1000)

@pytest.fixture
def r1() -> AsyncRouter:
    """Provides a reusable router instance for router 'r1'."""
    return AsyncRouter(ip_addr="localhost", protocol="http", router_id="r1", port=2000)

@pytest.fixture
def rlb_two_routers(r0: AsyncRouter, r1: AsyncRouter) -> RouterLoadBalancer:
    """Returns a RouterLoadBalancer instance with two routers, r0 and r1."""
    return RouterLoadBalancer(routers=[r0, r1])

# --- Tests ---

def test_get_specific_router(rlb_two_routers: RouterLoadBalancer, r0: AsyncRouter, r1: AsyncRouter):
    """
    Tests that requesting a specific router_id returns that router
    and increments its counter.
    """
    # Request r1 specifically
    router = rlb_two_routers.get_router(router_id="r1")
    
    # Assert we got the correct router
    assert router == r1
    assert router.router_id == "r1"
    
    # Assert the counters are correct
    assert rlb_two_routers.operation_counter["r1"] == 1
    assert rlb_two_routers.operation_counter["r0"] == 0

def test_get_router_load_balancing(rlb_two_routers: RouterLoadBalancer, r0: AsyncRouter, r1: AsyncRouter):
    """
    Tests the default load balancing behavior.
    It should distribute requests evenly (round-robin).
    """
    rlb = rlb_two_routers
    router_ids = []

    # Call the load balancer 6 times
    for _ in range(6):
        router = rlb.get_router()
        router_ids.append(router.router_id)
            
    # Assertions
    # It should have called r0 3 times and r1 3 times
    assert router_ids.count("r0") == 3
    assert router_ids.count("r1") == 3
    
    # The final counter state should be balanced
    assert rlb.operation_counter["r0"] == 3
    assert rlb.operation_counter["r1"] == 3

def test_get_router_mixed_requests(rlb_two_routers: RouterLoadBalancer, r0: AsyncRouter, r1: AsyncRouter):
    """
    Tests that load balancing correctly picks the least-used router
    after specific requests have unbalanced the load.
    """
    rlb = rlb_two_routers

    # Request r0 3 times specifically
    rlb.get_router(router_id="r0")
    rlb.get_router(router_id="r0")
    rlb.get_router(router_id="r0")
    
    # Check intermediate state
    assert rlb.operation_counter["r0"] == 3
    assert rlb.operation_counter["r1"] == 0
    
    # Now get a load-balanced router
    router = rlb.get_router()
    
    # It MUST be r1, as it has the minimum (0)
    assert router == r1
    
    # Check final state
    assert rlb.operation_counter["r0"] == 3
    assert rlb.operation_counter["r1"] == 1

def test_get_router_empty_list():
    """
    Tests that calling get_router on a load balancer with no routers
    raises a ValueError (from min() on an empty sequence).
    """
    rlb = RouterLoadBalancer(routers=[])
    
    # Assert it raises ValueError when calling min() on the empty counter dict
    with pytest.raises(ValueError):
        rlb.get_router()