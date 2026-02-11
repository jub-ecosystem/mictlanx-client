import pytest
import httpx
import socket
import respx
from unittest.mock import MagicMock
from mictlanx.errors import (
    MictlanXError, NotFoundError, RequestTimeoutError, 
    DNSResolutionError, ConnectFailedError, ValidationError
)

# Assuming your code is in exceptions.py
# from exceptions import (
#     MictlanXError, NotFoundError, RequestTimeoutError, 
#     DNSResolutionError, ConnectFailedError, ValidationError
# )

### --- Fixtures ---

@pytest.fixture
def mock_request():
    return httpx.Request("GET", "https://api.mictlanx.com/data")

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