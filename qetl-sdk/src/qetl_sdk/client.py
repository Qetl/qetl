"""
QETL Client - Main client class for interacting with QETL services
"""

import os
import sys
import logging
from typing import Optional, List, Dict, Any, Union
from pathlib import Path

from .job import Job
from .builder import JobBuilder
from .exceptions import QETLError, AuthenticationError, ValidationError
from .local_runner import LocalRunner
from .cloud_client import CloudClient

logger = logging.getLogger(__name__)


class QETLClient:
    """
    Main client class for interacting with QETL services.
    
    Supports both local and cloud modes:
    - Local mode: Runs jobs using local QETL installation
    - Cloud mode: Submits jobs to QETL Enterprise Cloud
    """
    
    def __init__(
        self,
        mode: str = "local",
        instance_id: Optional[str] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: int = 300,
        qetl_home: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize QETL client.
        
        Args:
            mode: Operation mode - "local" or "cloud"
            instance_id: QETL instance ID (required for cloud mode)
            api_key: API key for authentication (required for cloud mode)
            base_url: Custom API endpoint URL (optional)
            timeout: Request timeout in seconds
            qetl_home: Path to local QETL installation (for local mode)
            **kwargs: Additional configuration options
        """
        self.mode = mode.lower()
        self.instance_id = instance_id
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self.qetl_home = qetl_home or self._detect_qetl_home()
        
        # Initialize appropriate backend
        if self.mode == "local":
            self._backend = LocalRunner(
                qetl_home=self.qetl_home,
                timeout=timeout,
                **kwargs
            )
        elif self.mode == "cloud":
            if not api_key:
                raise AuthenticationError("API key is required for cloud mode")
            if not instance_id:
                raise AuthenticationError("Instance ID is required for cloud mode")
            self._backend = CloudClient(
                instance_id=instance_id,
                api_key=api_key,
                base_url=base_url,
                timeout=timeout,
                **kwargs
            )
        else:
            raise ValueError(f"Unsupported mode: {mode}. Use 'local' or 'cloud'")
            
        logger.info(f"QETL client initialized in {self.mode} mode")
    
    def _detect_qetl_home(self) -> Optional[str]:
        """
        Detect local QETL installation path.
        
        Returns:
            Path to QETL installation or None if not found
        """
        # Check environment variable
        if "QETL_HOME" in os.environ:
            return os.environ["QETL_HOME"]
        
        # Check current directory and parent directories
        current_dir = Path.cwd()
        for path in [current_dir] + list(current_dir.parents):
            # Look for QETL pipeline indicators
            if (path / "core" / "quantum_mathematics_engine.py").exists():
                return str(path)
            if (path / "yaml_pipeline_runner").exists():
                return str(path)
        
        return None
    
    def submit_job(
        self, 
        yaml_file: Union[str, Path],
        **kwargs
    ) -> Job:
        """
        Submit a job from a YAML configuration file.
        
        Args:
            yaml_file: Path to YAML configuration file
            **kwargs: Additional job parameters (priority, timeout, etc.)
            
        Returns:
            Job object for monitoring and result retrieval
            
        Raises:
            ValidationError: If YAML configuration is invalid
            QETLError: If job submission fails
        """
        yaml_path = Path(yaml_file)
        if not yaml_path.exists():
            raise QETLError(f"YAML file not found: {yaml_file}")
        
        logger.info(f"Submitting job from {yaml_file}")
        
        try:
            return self._backend.submit_job(yaml_path, **kwargs)
        except Exception as e:
            logger.error(f"Failed to submit job: {e}")
            raise QETLError(f"Job submission failed: {e}") from e
    
    def create_job(self) -> JobBuilder:
        """
        Create a job programmatically using the builder pattern.
        
        Returns:
            JobBuilder instance for constructing job configuration
        """
        return JobBuilder(client=self)
    
    def list_jobs(
        self, 
        status: Optional[str] = None,
        limit: int = 100,
        **kwargs
    ) -> List[Job]:
        """
        List jobs with optional filtering.
        
        Args:
            status: Filter by job status (queued, running, completed, failed)
            limit: Maximum number of jobs to return
            **kwargs: Additional filter parameters
            
        Returns:
            List of Job objects
        """
        try:
            return self._backend.list_jobs(status=status, limit=limit, **kwargs)
        except Exception as e:
            logger.error(f"Failed to list jobs: {e}")
            raise QETLError(f"Failed to list jobs: {e}") from e
    
    def get_job(self, job_id: str) -> Job:
        """
        Get a specific job by ID.
        
        Args:
            job_id: Unique job identifier
            
        Returns:
            Job object
            
        Raises:
            QETLError: If job not found or retrieval fails
        """
        try:
            return self._backend.get_job(job_id)
        except Exception as e:
            logger.error(f"Failed to get job {job_id}: {e}")
            raise QETLError(f"Failed to get job {job_id}: {e}") from e
    
    def list_components(self) -> List[Dict[str, Any]]:
        """
        List available processing components.
        
        Returns:
            List of component information dictionaries
        """
        try:
            return self._backend.list_components()
        except Exception as e:
            logger.error(f"Failed to list components: {e}")
            raise QETLError(f"Failed to list components: {e}") from e
    
    def validate_yaml(self, yaml_file: Union[str, Path]) -> Dict[str, Any]:
        """
        Validate a YAML configuration file without submitting a job.
        
        Args:
            yaml_file: Path to YAML configuration file
            
        Returns:
            Validation result dictionary
            
        Raises:
            ValidationError: If YAML is invalid
        """
        yaml_path = Path(yaml_file)
        if not yaml_path.exists():
            raise FileNotFoundError(f"YAML file not found: {yaml_file}")
        
        try:
            return self._backend.validate_yaml(yaml_path)
        except Exception as e:
            logger.error(f"YAML validation failed: {e}")
            raise ValidationError(f"YAML validation failed: {e}") from e
    
    def get_instance_info(self) -> Dict[str, Any]:
        """
        Get information about the QETL instance.
        
        Returns:
            Dictionary containing instance information
        """
        try:
            return self._backend.get_instance_info()
        except Exception as e:
            logger.error(f"Failed to get instance info: {e}")
            raise QETLError(f"Failed to get instance info: {e}") from e
    
    def create_job_builder(self) -> "JobBuilder":
        """Create a new job builder."""
        from .builder import JobBuilder
        return JobBuilder(client=self)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if hasattr(self._backend, 'cleanup'):
            self._backend.cleanup()
    
    def __repr__(self) -> str:
        """String representation of the client."""
        if self.mode == "local":
            return f"QETLClient(mode='local', qetl_home='{self.qetl_home}')"
        else:
            return f"QETLClient(mode='cloud', instance_id='{self.instance_id}')"
