
from typing import Optional
import socket
# 
import httpx
# 
from mictlanx.utils import Utils
class MictlanXError(Exception):
    """Base class for all custom exceptions."""
    
    default_message = "An error occurred."
    default_status_code     = 500                   # Default to Internal Server Error
    default_error_code      = 0

    def __init__(self, message:Optional[int]=None, status_code:Optional[int]=None,error_code:Optional[int]= None):
        self.message     = message or self.default_message
        self.status_code = status_code or self.default_status_code
        self.error_code  = error_code or self.default_error_code
        super().__init__(self.message)

    def __str__(self):
        return f"{self.__class__.__name__} (status={self.status_code},code={self.error_code}): {self.message}"
    def get_name(self):
        return Utils.camel_to_snake(self.__class__.__name__)
    
    @staticmethod
    def _root_cause(exc: BaseException) -> BaseException:
        cur = exc
        # Walk __cause__/__context__ to the deepest cause.
        while True:
            nxt = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
            if nxt is None: return cur
            cur = nxt

    @staticmethod
    def _format_endpoint_from_request(req: Optional[httpx.Request]) -> str:
        if not req:
            return ""
        try:
            url = req.url
            # Explicit host:port if available
            host = url.host or ""
            port = url.port or (443 if url.scheme == "https" else 80)
            return f"{url.scheme}://{host}:{port}"
        except Exception:
            return ""
        
    @staticmethod
    def from_exception(e: Exception) -> 'MictlanXError': 
        """Maps an exception to a defined error class based on its status code."""

        status_code = 500  # default
        message = str(e)   # default message
        error_code = 0    # default error code

        # If it's from httpx and a non-2xx HTTP response
        if isinstance(e, httpx.RequestError):
            endpoint = MictlanXError._format_endpoint_from_request(getattr(e, "request", None))
            root = MictlanXError._root_cause(e)
            root_name = root.__class__.__name__
            root_msg = str(root) or repr(root)

            # Classify special cases
            if isinstance(e, httpx.ConnectTimeout):
                return RequestTimeoutError(f"Timeout connecting to {endpoint} (connect): {root_name}: {root_msg}")
            if isinstance(e, (httpx.ReadTimeout, httpx.WriteTimeout)):
                return RequestTimeoutError(f"Timeout during request to {endpoint}: {root_name}: {root_msg}")
            if isinstance(e, httpx.ConnectError):
                # Common low-level causes: socket.gaierror (DNS), ConnectionRefusedError, etc.
                if isinstance(root, socket.gaierror):
                    return DNSResolutionError(f"DNS resolution failed for {endpoint}: {root}")
                if isinstance(root, ConnectionRefusedError):
                    return ConnectFailedError(f"Connection refused to {endpoint}: {root}")
                return ConnectFailedError(f"Failed to connect to {endpoint}: {root_name}: {root_msg}")
            if isinstance(e, httpx.ProxyError):
                return NetworkError(f"Proxy error while contacting {endpoint}: {root_name}: {root_msg}")
            if isinstance(e, httpx.RemoteProtocolError):
                return UpstreamProtocolError(f"Remote protocol error from {endpoint}: {root_name}: {root_msg}")

        if isinstance(e, httpx.HTTPStatusError):
            resp        = e.response
            status_code = resp.status_code
            # FastAPI puts detail in JSON body
            try:
                detail     = resp.json().get("detail","Unknown Error")
                message    = detail.get("msg", "Unknown Error") if isinstance(detail, dict) else str(detail)
                error_code = detail.get("code",0)
                # message = 
                # detail if isinstance(detail, str) else str(detail)
            except Exception:
                message = resp.text

        # Else if the exception has a status_code attribute (like FastAPI HTTPException locally)
        elif hasattr(e, "status_code"):
            status_code = getattr(e, "status_code", 500)
            if hasattr(e, "detail"):
                message = e.detail if isinstance(e.detail, str) else str(e.detail)
                if hasattr(e, "code"):
                    error_code = getattr(e, "code",0)

        # Define mapping of status codes to custom exceptions
        ERROR_MAP = {
            0: UnknownError, 
            400: ValidationError,
            401: AuthenticationError,
            403: PermissionError,
            404: NotFoundError, 
            405: FileAlreadyExists,
            500: UnknownError,
            501: IntegrityError,
            502: PutChunksError,
            503: GetChunkError,
            1000: NetworkError,
            1001: ConnectFailedError,
            1002: DNSResolutionError,
            1004: RequestTimeoutError,     # mirrors 504
            1005: UpstreamProtocolError,
            666: MaxAvailabilityReachedError,
        }

        error_class = ERROR_MAP.get(error_code, UnknownError)
        return error_class(message)

class MaxAvailabilityReachedError(MictlanXError):
    """Exception raised when maximum replication factor is reached."""
    def __init__(self, message = "Maximum availability reached for this resource"):
        super().__init__(message, 409, 666)

class ValidationError(MictlanXError):
    """Exception raised when a resource is not found."""
    def __init__(self, message = "Validation failed"):
        super().__init__(message, 400, 400)

class GetChunkError(MictlanXError):
    """Exception raised when a resource is not found."""
    def __init__(self, message = "Get chunk failed"):
        super().__init__(message, 503, 503)
class PutChunksError(MictlanXError):
    """Exception raised when a resource is not found."""
    def __init__(self, message = "Put chunks failed"):
        super().__init__(message, 502, 502)

class IntegrityError(MictlanXError):
    """Exception raised when a resource is not found."""
    def __init__(self, message = "Integrity check failed"):
        super().__init__(message, 501, 501)

class UnknownError(MictlanXError):
    """Exception raised when a resource is not found."""
    def __init__(self, message = "An unknown error occurred"):
        super().__init__(message, 500,500)


class NotFoundError(MictlanXError):
    """Exception raised when a resource is not found."""
    def __init__(self, message = "Resource not found"):
        super().__init__(message, 404, 404)
    # default_message = "Resource not found."
    # error_code = 404


class AuthenticationError(MictlanXError):
    """Exception raised for authentication failures."""
    def __init__(self, message = "Authentication failed"):
        super().__init__(message, 401, 401)

class PermissionError(MictlanXError):
    """Exception raised when a user lacks permissions."""
    def __init__(self, message = "Permission denied"):
        super().__init__(message, 403, 403)

class FileAlreadyExists(MictlanXError):
    """Exception raised when a user lacks permissions."""
    def __init__(self, message = "File already exists"):
        super().__init__(message, 405, 405)

class NetworkError(MictlanXError):
    def __init__(self, message = "Network error",status_code=1000,error_code=1000):
        super().__init__(message, status_code, error_code)

class ConnectFailedError(NetworkError):
    def __init__(self, message = "Connection failed"):
        super().__init__(message, 1001, 1001)

class DNSResolutionError(NetworkError):
    def __init__(self, message = "DNS resolution failed"):
        super().__init__(message, 1002, 1002)

class RequestTimeoutError(NetworkError):
    def __init__(self, message = "Request timed out"):
        super().__init__(message, 1004, 1004)

class UpstreamProtocolError(MictlanXError):
    def __init__(self, message = "Upstream protocol error"):
        super().__init__(message, 1005, 1005)