"""
QETL SDK - Python SDK for QETL Quantum Processing Pipeline
"""

import sys
import logging
from pathlib import Path

# Version check
if sys.version_info < (3, 8):
    raise RuntimeError("qetl-sdk requires Python 3.8 or higher")

# Package metadata
__version__ = "1.0.0"
__author__ = "QETL Development Team"
__email__ = "dev@qetl.io"
__description__ = "Python SDK for QETL Quantum Processing Pipeline"

# Core imports
try:
    from .client import QETLClient
    from .job import Job, JobStatus, JobResults, JobState
    from .builder import JobBuilder
    from .exceptions import (
        QETLError,
        AuthenticationError,
        AuthorizationError,
        ValidationError,
        JobExecutionError,
        TimeoutError,
        ConfigurationError,
        ComponentNotFoundError,
        JobNotFoundError,
        NetworkError,
        RateLimitError,
        QuotaExceededError
    )
except ImportError as e:
    # Handle import errors gracefully during development/testing
    import warnings
    warnings.warn(f"Could not import all QETL SDK components: {e}")
    
    # Define minimal exports for partial imports
    from .exceptions import QETLError

# Setup default logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Version info tuple
VERSION_INFO = tuple(int(x) for x in __version__.split('.') if x.isdigit())

# Convenience functions
def get_version() -> str:
    """Get the current version string."""
    return __version__

def get_version_info() -> tuple:
    """Get the current version as a tuple."""
    return VERSION_INFO

def create_client(mode: str = "local", **kwargs) -> "QETLClient":
    """Convenience function to create a QETL client.
    
    Args:
        mode: Execution mode ('local' or 'cloud')
        **kwargs: Additional client configuration
        
    Returns:
        QETLClient instance
    """
    return QETLClient(mode=mode, **kwargs)

# All exports
__all__ = [
    # Version and metadata
    "__version__",
    "VERSION_INFO",
    "get_version",
    "get_version_info",
    
    # Core classes
    "QETLClient",
    "Job",
    "JobStatus", 
    "JobResults",
    "JobState",
    "JobBuilder",
    
    # Convenience functions
    "create_client",
    
    # Exceptions
    "QETLError",
    "AuthenticationError",
    "AuthorizationError",
    "ValidationError",
    "JobExecutionError",
    "TimeoutError",
    "ConfigurationError",
    "ComponentNotFoundError",
    "JobNotFoundError",
    "NetworkError",
    "RateLimitError",
    "QuotaExceededError"
]
