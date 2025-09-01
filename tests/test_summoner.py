import pytest
from mictlanx.services import Summoner
from mictlanx.services.models.summoner import SummonContainerPayload,ExposedPort,MountX,MountType
from option import Some,NONE

@pytest.fixture()
def summoner():
    return Summoner(
        ip_addr     = "localhost",
        api_version = Some(3),
        network     = NONE,
        port        = 15000,
        protocol    = "http"
    )

@pytest.mark.skip("")
def test_health_check(summoner:Summoner):
    res = summoner.health_check()
    assert res.is_ok

@pytest.mark.skip("")
def test_stats(summoner:Summoner):
    res = summoner.stats()
    print(res)
    assert res.is_ok

@pytest.mark.skip("")
def test_summon_container(summoner:Summoner):

    container_id = "mictlanx-peer-y"
    container_port =  6701
    data_path = "/app/mictlanx/data"
    logs_path = "/app/mictlanx/logs"
    payload = SummonContainerPayload(
        container_id  = container_id,
        cpu_count     = 1,
        envs          = {
            "NODE_ID": container_id,
            "IP_ADDRESS": container_id,
            "SERVER_IP_ADDR": "0.0.0.0",
            "NODE_PORT": str(container_port),
            "NODE_DISK_CAPACITY": "10000000000",
            "NODE_MEMORY_CAPACITY": "1000000000",
            # optional paths
            "BASE_PATH": "/app/mictlanx",
            "LOCAL_PATH": "/app/mictlanx/local",
            "DATA_PATH": data_path,
            "LOG_PATH": logs_path,
            # extra flags
            "ELASTIC": "true",
            "TRAVERSE_AND_CLEAN": "true",
            "MIN_INTERVAL_TIME": "5",
            "MAX_INTERVAL_TIME": "20",
            "WORKERS": "2",
            "CLIENT_REQUEST_TIMEOUT": "300",
            "CLIENT_DISCONNECT_TIMEOUT": "30",
            "DISK_CHECK_INTERVAL_TIME": "30",
        },
        exposed_ports = [ExposedPort(
            # ip_addr        = None,
            container_port = container_port,
            host_port      = container_port,
            # protocol       = NONE,
        )],
        force         = True,
        hostname      = container_id,
        image         = "nachocode/mictlanx:peer-0.0.160-alpha.1",
        ip_addr       = container_id,
        labels        = {},
        memory        = 1_000_000_000,
        mounts        = [
            MountX(
                mount_type = MountType.BIND,
                source     = f"/mictlanx/{container_id}/data",
                target     = data_path
            ),
            MountX(
                mount_type = MountType.BIND,
                source     = f"/mictlanx/{container_id}/logs",
                target     = logs_path
            ),
        ],
        network_id    = "mictlanx",
        selected_node = None,
        shm_size      = None,
    )


    res = summoner.summon(payload=payload)
    assert res.is_ok



@pytest.mark.skip("")
def test_delete_container(summoner:Summoner):
    peer_id = "new_storage_peer"
    res = summoner.summon_peer(
        container_id=peer_id
    )
    assert res.is_ok
    res = summoner.delete_container(container_id=peer_id)
    assert res.is_ok
    
