import pytest
import httpx
import socket
import respx
from unittest.mock import MagicMock
from mictlanx.errors import (
    MictlanXError, NotFoundError, RequestTimeoutError,
    DNSResolutionError, ValidationError, MaxAvailabilityReachedError,
    AuthenticationError, PermissionError, FileAlreadyExists,
    NetworkError, ConnectFailedError, GetChunkError, PutChunksError,
    IntegrityError, UnknownError, BadParametersError, UpstreamProtocolError,
)

### --- Test Cases ---

def test_custom_error_instantiation():
    """Test that custom errors carry the correct default codes."""
    exc = NotFoundError("Missing file")
    assert exc.status_code == 404  # Based on your base class default
    assert exc.error_code == 404
    assert str(exc) == "NotFoundError (status=404,code=404): Missing file"

def test_from_exception_with_standard_httpx_error(mock_request):
    """Test mapping of httpx.ConnectTimeout to RequestTimeoutError."""
    original_exc = httpx.ConnectTimeout("Connection timed out", request=mock_request)
    mapped_exc = MictlanXError.from_exception(original_exc)
    
    assert isinstance(mapped_exc, RequestTimeoutError)
    assert "Timeout connecting to https://api.mictlanx.com:443" in mapped_exc.message

def test_from_exception_with_dns_failure(mock_request):
    """Test deep root cause extraction (socket.gaierror)."""
    dns_root = socket.gaierror(-2, 'Name or service not known')
    # Simulate an httpx error caused by a DNS failure
    original_exc = httpx.ConnectError("Failed", request=mock_request)
    original_exc.__cause__ = dns_root
    
    mapped_exc = MictlanXError.from_exception(original_exc)
    
    assert isinstance(mapped_exc, DNSResolutionError)
    assert "DNS resolution failed" in mapped_exc.message

@respx.mock
def test_from_exception_with_http_status_error():
    """Test parsing of FastAPI-style JSON error bodies."""
    # Simulate a 400 Bad Request with a JSON body
    url = "https://api.mictlanx.com/fail"
    respx.get(url).mock(return_value=httpx.Response(
        400, 
        json={"detail": {"msg": "Invalid UUID", "code": 400}}
    ))
    
    with httpx.Client() as client:
        response = client.get(url)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            mapped_exc = MictlanXError.from_exception(e)
    
    assert isinstance(mapped_exc, ValidationError)
    assert mapped_exc.message == "Invalid UUID"

def test_from_exception_with_custom_attribute_object():
    """Test objects that have status_code and detail attributes (FastAPI style)."""
    mock_exc = MagicMock()
    mock_exc.status_code = 404
    mock_exc.detail = "Not found locally"
    mock_exc.code = 404
    
    mapped_exc = MictlanXError.from_exception(mock_exc)
    
    assert isinstance(mapped_exc, NotFoundError)
    assert mapped_exc.message == "Not found locally"

def test_root_cause_utility():
    """Test the static _root_cause walker."""
    e1 = Exception("Root")
    e2 = Exception("Middle")
    e2.__cause__ = e1
    e3 = Exception("Top")
    e3.__context__ = e2

    assert MictlanXError._root_cause(e3) == e1

# ── All error subclasses ────────────────────────────────────────────────────

@pytest.mark.parametrize("cls,status,code,default_fragment", [
    (MaxAvailabilityReachedError, 409,  666,  "Maximum availability"),
    (AuthenticationError,         401,  401,  "Authentication failed"),
    (PermissionError,             403,  403,  "Permission denied"),
    (FileAlreadyExists,           405,  405,  "File already exists"),
    (NetworkError,                1000, 1000, "Network error"),
    (ConnectFailedError,          1001, 1001, "Connection failed"),
    (GetChunkError,               503,  503,  "Get chunk failed"),
    (PutChunksError,              502,  502,  "Put chunks failed"),
    (IntegrityError,              501,  501,  "Integrity check failed"),
    (UnknownError,                500,  500,  "unknown error"),
    (BadParametersError,          400,  400,  "Bad parameters"),
    (UpstreamProtocolError,       1005, 1005, "Upstream protocol error"),
    (DNSResolutionError,          1002, 1002, "DNS resolution failed"),
])
def test_error_subclass_defaults(cls, status, code, default_fragment):
    """Every subclass carries correct default codes and message fragment."""
    exc = cls()
    assert exc.status_code == status, f"{cls.__name__}.status_code"
    assert exc.error_code == code, f"{cls.__name__}.error_code"
    assert default_fragment.lower() in str(exc).lower(), f"{cls.__name__} str()"
    assert isinstance(exc, MictlanXError)

def test_error_subclass_custom_message():
    """Custom message is stored and surfaced in str()."""
    exc = IntegrityError("checksum mismatch for ball-1")
    assert "checksum mismatch" in exc.message
    assert exc.status_code == 501

def test_connect_failed_is_network_error():
    """ConnectFailedError and DNSResolutionError are NetworkError subtypes."""
    assert issubclass(ConnectFailedError, NetworkError)
    assert issubclass(DNSResolutionError, NetworkError)

# ── from_exception — additional httpx types ─────────────────────────────────

def test_from_exception_read_timeout(mock_request):
    exc = httpx.ReadTimeout("timed out", request=mock_request)
    mapped = MictlanXError.from_exception(exc)
    assert isinstance(mapped, RequestTimeoutError)
    assert "Timeout" in mapped.message

def test_from_exception_write_timeout(mock_request):
    exc = httpx.WriteTimeout("timed out", request=mock_request)
    mapped = MictlanXError.from_exception(exc)
    assert isinstance(mapped, RequestTimeoutError)

def test_from_exception_proxy_error(mock_request):
    exc = httpx.ProxyError("proxy failed", request=mock_request)
    mapped = MictlanXError.from_exception(exc)
    assert isinstance(mapped, NetworkError)
    assert "Proxy error" in mapped.message

def test_from_exception_remote_protocol_error(mock_request):
    exc = httpx.RemoteProtocolError("bad protocol", request=mock_request)
    mapped = MictlanXError.from_exception(exc)
    assert isinstance(mapped, UpstreamProtocolError)

def test_from_exception_connection_refused(mock_request):
    exc = httpx.ConnectError("refused", request=mock_request)
    exc.__cause__ = ConnectionRefusedError("Connection refused")
    mapped = MictlanXError.from_exception(exc)
    assert isinstance(mapped, ConnectFailedError)
    assert "refused" in mapped.message.lower()

def test_from_exception_plain_exception_maps_to_unknown():
    """A plain Exception with no status code maps to UnknownError."""
    exc = Exception("something totally unexpected")
    mapped = MictlanXError.from_exception(exc)
    assert isinstance(mapped, MictlanXError)

def test_from_exception_get_name():
    """get_name() returns snake_case class name."""
    assert NotFoundError().get_name() == "NOT_FOUND_ERROR"
    assert IntegrityError().get_name() == "INTEGRITY_ERROR"