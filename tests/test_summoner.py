import os
import pytest
import time
from mictlanx.services import Summoner
from mictlanx.services.models.summoner import SummonContainerPayload, ExposedPort, MountX, MountType
from option import Some, NONE

import dotenv

# Load environment variables
dotenv.load_dotenv(".env.test")

MICTLANX_SUMMONER_PORT = int(os.environ.get("MICTLANX_SUMMONER_PORT", "15000"))

# --- Fixtures ---

@pytest.fixture(scope="module")
def summoner():
    """
    Creates a single Summoner instance for the test session.
    """
    return Summoner(
        ip_addr="localhost",
        api_version=Some(3),
        network=NONE,
        port=MICTLANX_SUMMONER_PORT,
        protocol="http"
    )
@pytest.fixture(scope="module")
def invalid_summoner():
    """
    Creates a Summoner instance pointing to an invalid port to test error handling.
    """
    return Summoner(
        ip_addr     = "localhost",
        api_version = Some(3),
        network     = NONE,
        port        = 9999,          # Assuming nothing is running on this port
        protocol    = "http"
    )


@pytest.fixture
def generic_payload():
    """
    Returns a basic payload for an Nginx container.
    Using 'nginx:alpine' as it is small and reliable for testing.
    """
    container_id = f"test-nginx-{int(time.time())}"
    return SummonContainerPayload(
        container_id  = container_id,
        image         = "nginx:alpine",
        hostname      = "test-host",
        exposed_ports = [
            ExposedPort(host_port=0, container_port=80)  # 0 usually implies random/auto in many systems
        ],
        envs          = {"TEST_ENV": "true"},
        memory        = 100_000_000,            # ~100MB
        cpu_count     = 1,
        network_id    = "mictlanx",
        mounts        = [],
        force         = True,
        ip_addr       = None,                   # will default to container_id
        shm_size      = None,
        labels        = {},
        selected_node = None
    )

# --- Basic Connectivity Tests ---

def test_health_check(summoner: Summoner):
    """Verifies the service is up and reachable."""
    print(f"\n[Connecting to {summoner.base_url}]")
    res = summoner.health_check()
    assert res.is_ok, f"Health check failed: {res.unwrap_err()}"
    assert res.unwrap() is True

def test_stats(summoner: Summoner):
    """Verifies we can retrieve service statistics."""
    res = summoner.stats()
    
    assert res.is_ok, f"Stats request failed: {res.unwrap_err()}"
    data = res.unwrap()
    print(f"\nStats received: {len(data)} services found.")
    assert isinstance(data, dict)

# --- Lifecycle Tests (Create & Delete) ---

def test_summon_and_delete_generic_container(summoner: Summoner, generic_payload):
    """
    Full lifecycle test:
    1. Summon a container.
    2. Assert success.
    3. Delete the container.
    """
    print(f"\nSummoning container: {generic_payload.container_id}")
    
    # 1. Summon
    res_summon = summoner.summon(payload=generic_payload, mode="docker")
    
    assert res_summon.is_ok, f"Summon failed: {res_summon.unwrap_err()}"

    summon_response = res_summon.unwrap()
    assert summon_response is not None
    print(f"Container summoned successfully. Response: {summon_response}")

    # Optional: Brief sleep to let the container spin up if you wanted to check 'stats' immediately after
    # time.sleep(1)

    # 2. Delete (Cleanup)
    print(f"Deleting container: {generic_payload.container_id}")
    res_delete = summoner.delete_container(
        container_id=generic_payload.container_id, 
        mode="docker"
    )

    assert res_delete.is_ok, f"Delete failed: {res_delete.unwrap_err()}"
    print("Container deleted successfully.")

def test_summon_peer_lifecycle(summoner: Summoner):
    """
    Tests the specific 'summon_peer' helper method.
    This uses the 'storage_peer_image' defined in your class.
    """
    peer_id = f"test-peer-{int(time.time())}"
    
    print(f"\nSummoning Peer: {peer_id}")
    
    # 1. Summon Peer
    res_summon = summoner.summon_peer(
        container_id=peer_id,
        port=0, # Random port
        memory="128MB",
        disk="500MB",
        workers=1
    )

    assert res_summon.is_ok, f"Summon Peer failed: {res_summon.unwrap_err()}"
    
    response = res_summon.unwrap()
    assert response is not None
    print(f"Peer summoned. Response: {response}")

    # 2. Delete Peer
    print(f"Deleting Peer: {peer_id}")
    res_delete = summoner.delete_container(container_id=peer_id, mode="docker")
    
    assert res_delete.is_ok, f"Delete failed: {res_delete.unwrap_err()}"


def test_invalid_summoner_health_check(invalid_summoner: Summoner, generic_payload):
    """Tests that an invalid summoner correctly reports health check failure."""
    res = invalid_summoner.health_check()
    assert res.is_err, "Health check should have failed but succeeded."
    res = invalid_summoner.stats()
    assert res.is_err, "Stats request should have failed but succeeded."
    res = invalid_summoner.summon_peer(container_id="test-peer-invalid", port=0)
    assert res.is_err, "Summon Peer should have failed but succeeded."
    res = invalid_summoner.delete_container(container_id="test-peer-invalid", mode="docker")
    assert res.is_err, "Delete should have failed but succeeded."
    res = invalid_summoner.summon(payload=generic_payload, mode="docker")
    assert res.is_err, "Summon should have failed but succeeded."

def test_summoner_ip(summoner: Summoner, generic_payload):
    """Tests the IP address assignment logic in the summoner."""
    # Case 1: IP matches container_id
    res = summoner._Summoner__get_available_ip_addr(generic_payload)
    assert res.is_some, f"Expected Some but got None. Payload: {generic_payload}"
    generic_payload.ip_addr = "0.0.0.0"
    res = summoner._Summoner__get_available_ip_addr(generic_payload)
    assert res.is_some, f"Expected Some but got None. Payload: {generic_payload}"
    generic_payload.ip_addr = "X"
    res = summoner._Summoner__get_available_ip_addr(generic_payload)
    assert res.is_some, f"Expected Some but got None. Payload: {generic_payload}"
    # print(res)
    # generic_payload.ip_addr = generic_payload.container_id

    # Case 2: IP is