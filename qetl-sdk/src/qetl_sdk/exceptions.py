"""
QETL SDK Exceptions

Custom exceptions for the QETL SDK.
"""


class QETLError(Exception):
    """Base exception for all QETL SDK errors."""
    
    def __init__(self, message: str, error_code: str = None, details: dict = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}
    
    def __str__(self) -> str:
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class AuthenticationError(QETLError):
    """Raised when authentication fails."""
    
    def __init__(self, message: str = "Authentication failed"):
        super().__init__(message, error_code="AUTH_ERROR")


class AuthorizationError(QETLError):
    """Raised when authorization fails."""
    
    def __init__(self, message: str = "Authorization failed"):
        super().__init__(message, error_code="AUTHZ_ERROR")


class ValidationError(QETLError):
    """Raised when input validation fails."""
    
    def __init__(self, message: str, validation_errors: list = None):
        super().__init__(message, error_code="VALIDATION_ERROR")
        self.validation_errors = validation_errors or []


class JobExecutionError(QETLError):
    """Raised when job execution fails."""
    
    def __init__(self, message: str, job_id: str = None):
        super().__init__(message, error_code="JOB_EXECUTION_ERROR")
        self.job_id = job_id


class TimeoutError(QETLError):
    """Raised when operations timeout."""
    
    def __init__(self, message: str = "Operation timed out", timeout_seconds: int = None):
        super().__init__(message, error_code="TIMEOUT_ERROR")
        self.timeout_seconds = timeout_seconds


class ConfigurationError(QETLError):
    """Raised when configuration is invalid."""
    
    def __init__(self, message: str):
        super().__init__(message, error_code="CONFIG_ERROR")


class ComponentNotFoundError(QETLError):
    """Raised when a requested component is not found."""
    
    def __init__(self, component_name: str):
        message = f"Component not found: {component_name}"
        super().__init__(message, error_code="COMPONENT_NOT_FOUND")
        self.component_name = component_name


class JobNotFoundError(QETLError):
    """Raised when a requested job is not found."""
    
    def __init__(self, job_id: str):
        message = f"Job not found: {job_id}"
        super().__init__(message, error_code="JOB_NOT_FOUND")
        self.job_id = job_id


class NetworkError(QETLError):
    """Raised when network operations fail."""
    
    def __init__(self, message: str, status_code: int = None):
        super().__init__(message, error_code="NETWORK_ERROR")
        self.status_code = status_code


class RateLimitError(QETLError):
    """Raised when rate limits are exceeded."""
    
    def __init__(self, message: str = "Rate limit exceeded", retry_after: int = None):
        super().__init__(message, error_code="RATE_LIMIT_ERROR")
        self.retry_after = retry_after


class QuotaExceededError(QETLError):
    """Raised when usage quotas are exceeded."""
    
    def __init__(self, message: str = "Quota exceeded", quota_type: str = None):
        super().__init__(message, error_code="QUOTA_EXCEEDED")
        self.quota_type = quota_type
