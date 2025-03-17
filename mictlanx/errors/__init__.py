from mictlanx.utils import Utils
class MictlanXError(Exception):
    """Base class for all custom exceptions."""
    
    default_message = "An error occurred."
    status_code = 500  # Default to Internal Server Error

    def __init__(self, message=None, status_code=None):
        self.message = message or self.default_message
        self.status_code = status_code or self.status_code
        super().__init__(self.message)

    def __str__(self):
        return f"{self.__class__.__name__} (status={self.status_code}): {self.message}"
    def get_name(self):
        return Utils.camel_to_snake(self.__class__.__name__)
    @staticmethod
    def from_exception(e:Exception)->'MictlanXError': 
        """Maps an exception to a defined error class based on its status code."""
        
        # Extract the status code if it exists, otherwise default to 500
        status_code = getattr(e, "status_code", 500)

        # Define mapping of status codes to custom exceptions
        ERROR_MAP = {
            400: ValidationError,
            401: AuthenticationError,
            403: PermissionError,
            404: NotFoundError,
            501: IntegrityError,
            502: PutChunksError
        }

        # Get the matching error class, default to `UnknownError`
        error_class = ERROR_MAP.get(status_code, UnknownError)

        # Convert exception to custom error
        return error_class(str(e))        
class ValidationError(MictlanXError):
    """Exception raised when a resource is not found."""
    default_message = "Validation failed"
    status_code = 400

class GetChunkError(MictlanXError):
    """Exception raised when a resource is not found."""
    default_message = "Get chunk failed"
    status_code = 503
class PutChunksError(MictlanXError):
    """Exception raised when a resource is not found."""
    default_message = "Put chunks failed"
    status_code = 502

class IntegrityError(MictlanXError):
    """Exception raised when a resource is not found."""
    default_message = "Integrity check failed"
    status_code = 501

class UnknownError(MictlanXError):
    """Exception raised when a resource is not found."""
    default_message = "Uknown error"
    status_code = 500


class NotFoundError(MictlanXError):
    """Exception raised when a resource is not found."""
    default_message = "Resource not found."
    status_code = 404

class Validation2rror(MictlanXError):
    """Exception raised for invalid inputs."""
    default_message = "Invalid input provided."
    status_code = 400

class AuthenticationError(MictlanXError):
    """Exception raised for authentication failures."""
    default_message = "Authentication failed."
    status_code = 401

class PermissionError(MictlanXError):
    """Exception raised when a user lacks permissions."""
    default_message = "Permission denied."
    status_code = 403
