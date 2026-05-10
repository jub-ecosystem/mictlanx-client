import pytest
import os
from mictlanx.asyncx import AsyncClient
import dotenv
from mictlanx.services.peer import AsyncPeer 

MICTLANX_ENV_FILE = os.environ.get("MICTLANX_ENV_FILE", ".env.test")

if os.path.exists(MICTLANX_ENV_FILE):
    dotenv.load_dotenv(MICTLANX_ENV_FILE)
# --- Fixtures ---

@pytest.fixture(scope="session")
@pytest.mark.asyncio
def async_client():
    """
    Session-scoped fixture to create and tear down the AsyncClient.
    This ensures the client is created only once per test session.
    """
    # Configuration is loaded once and used to create the client
    uri = os.environ.get("MICTLANX_URI", "mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0")
    client_id = os.environ.get("CLIENT_ID", "client-0")
    log_path = os.environ.get("MICTLANX_LOG_PATH", "/mictlanx/client")
    
    client = AsyncClient(
        client_id=client_id,
        uri=uri,
        debug=True,
        max_workers=2,
        log_output_path=log_path
    )
    return client

@pytest.fixture
def peer() -> AsyncPeer:
    # Adjust the IP/port/protocol to point to your running storage service
    return AsyncPeer(
        peer_id = "mictlanx-peer-0",
        ip_addr = "localhost",
        port    = int(os.environ.get("MICTLANX_TEST_PEER_PORT", 25000)),
        protocol = "http"
    )