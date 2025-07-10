"""
QETL Job - Job management and status tracking
"""

import time
import asyncio
from enum import Enum
from typing import Optional, Dict, Any, List, Callable, Union
from datetime import datetime, timezone
from pathlib import Path
import json

from .exceptions import QETLError, JobNotFoundError, TimeoutError


class JobState(Enum):
    """Job execution states."""
    SUBMITTED = "submitted"
    VALIDATING = "validating"
    QUEUED = "queued"
    INITIALIZING = "initializing"
    RUNNING = "running"
    COMPLETING = "completing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobStatus:
    """Job status information."""
    
    def __init__(
        self,
        job_id: str,
        state: JobState,
        progress: float = 0.0,
        message: Optional[str] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        **kwargs
    ):
        self.job_id = job_id
        self.state = state
        self.progress = progress
        self.message = message
        self.created_at = created_at or datetime.now(timezone.utc)
        self.updated_at = updated_at or datetime.now(timezone.utc)
        self.metadata = kwargs
    
    @property
    def is_terminal(self) -> bool:
        """Check if job is in a terminal state."""
        return self.state in {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED}
    
    @property
    def is_running(self) -> bool:
        """Check if job is currently running."""
        return self.state in {JobState.RUNNING, JobState.INITIALIZING, JobState.COMPLETING}
    
    @property
    def is_successful(self) -> bool:
        """Check if job completed successfully."""
        return self.state == JobState.COMPLETED
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert status to dictionary."""
        return {
            "job_id": self.job_id,
            "state": self.state.value,
            "progress": self.progress,
            "message": self.message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            **self.metadata
        }
    
    def __repr__(self) -> str:
        return f"JobStatus(job_id='{self.job_id}', state='{self.state.value}', progress={self.progress})"


class JobResults:
    """Job execution results."""
    
    def __init__(
        self,
        job_id: str,
        outputs: Dict[str, Any],
        execution_time: Optional[float] = None,
        logs: Optional[List[str]] = None,
        metrics: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        self.job_id = job_id
        self.outputs = outputs
        self.execution_time = execution_time
        self.logs = logs or []
        self.metrics = metrics or {}
        self.metadata = kwargs
    
    def get_output(self, name: str, default: Any = None) -> Any:
        """Get specific output by name."""
        return self.outputs.get(name, default)
    
    def save_to_file(self, filepath: Union[str, Path]) -> None:
        """Save results to JSON file."""
        filepath = Path(filepath)
        result_data = {
            "job_id": self.job_id,
            "outputs": self.outputs,
            "execution_time": self.execution_time,
            "logs": self.logs,
            "metrics": self.metrics,
            **self.metadata
        }
        
        with open(filepath, 'w') as f:
            json.dump(result_data, f, indent=2, default=str)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert results to dictionary."""
        return {
            "job_id": self.job_id,
            "outputs": self.outputs,
            "execution_time": self.execution_time,
            "logs": self.logs,
            "metrics": self.metrics,
            **self.metadata
        }
    
    def __repr__(self) -> str:
        output_count = len(self.outputs)
        return f"JobResults(job_id='{self.job_id}', outputs={output_count})"


class Job:
    """
    Represents a QETL job with status tracking and result retrieval capabilities.
    """
    
    def __init__(
        self,
        job_id: str,
        backend,
        initial_status: Optional[JobStatus] = None
    ):
        self.job_id = job_id
        self._backend = backend
        self._status = initial_status
        self._results: Optional[JobResults] = None
        self._completion_callbacks: List[Callable] = []
        self._status_callbacks: List[Callable] = []
    
    @property
    def id(self) -> str:
        """Get job ID."""
        return self.job_id
    
    @property
    def status(self) -> JobStatus:
        """Get current job status (cached)."""
        if self._status is None:
            self._status = self.get_status()
        return self._status
    
    def get_status(self) -> JobStatus:
        """Refresh and return current job status."""
        try:
            self._status = self._backend.get_job_status(self.job_id)
            
            # Trigger status callbacks
            for callback in self._status_callbacks:
                try:
                    callback(self._status)
                except Exception as e:
                    # Don't let callback errors break status updates
                    pass
            
            # Check for completion
            if self._status.is_terminal and self._completion_callbacks:
                self._trigger_completion_callbacks()
            
            return self._status
        except Exception as e:
            raise QETLError(f"Failed to get job status: {e}") from e
    
    def get_results(self) -> JobResults:
        """
        Get job results. Blocks until job is complete.
        
        Returns:
            JobResults object containing outputs and metadata
            
        Raises:
            JobExecutionError: If job failed
            QETLError: If results retrieval fails
        """
        # Wait for completion
        while not self.status.is_terminal:
            time.sleep(1)
        
        if not self.status.is_successful:
            raise QETLError(f"Job {self.job_id} failed: {self.status.message}")
        
        if self._results is None:
            try:
                self._results = self._backend.get_job_results(self.job_id)
            except Exception as e:
                raise QETLError(f"Failed to get job results: {e}") from e
        
        return self._results
    
    async def get_results_when_done(self, timeout: Optional[int] = None) -> JobResults:
        """
        Async version of get_results with optional timeout.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            JobResults object
            
        Raises:
            TimeoutError: If timeout is exceeded
            JobExecutionError: If job failed
        """
        start_time = time.time()
        
        while not self.status.is_terminal:
            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError(f"Job {self.job_id} timed out after {timeout} seconds")
            
            await asyncio.sleep(1)
            self.get_status()  # Refresh status
        
        if not self.status.is_successful:
            raise QETLError(f"Job {self.job_id} failed: {self.status.message}")
        
        if self._results is None:
            try:
                self._results = self._backend.get_job_results(self.job_id)
            except Exception as e:
                raise QETLError(f"Failed to get job results: {e}") from e
        
        return self._results
    
    def cancel(self) -> bool:
        """
        Cancel the job if it's still running.
        
        Returns:
            True if job was successfully cancelled
        """
        try:
            result = self._backend.cancel_job(self.job_id)
            self._status = None  # Force status refresh
            return result
        except Exception as e:
            raise QETLError(f"Failed to cancel job: {e}") from e
    
    def get_logs(self, follow: bool = False) -> List[str]:
        """
        Get job execution logs.
        
        Args:
            follow: If True, stream logs in real-time
            
        Returns:
            List of log messages
        """
        try:
            return self._backend.get_job_logs(self.job_id, follow=follow)
        except Exception as e:
            raise QETLError(f"Failed to get job logs: {e}") from e
    
    def on_completion(self, callback: Callable[[JobResults], None]) -> None:
        """
        Register a callback to be called when the job completes.
        
        Args:
            callback: Function to call with JobResults when job completes
        """
        self._completion_callbacks.append(callback)
        
        # If job is already complete, trigger callback immediately
        if self.status.is_terminal:
            self._trigger_completion_callbacks()
    
    def on_status_change(self, callback: Callable[[JobStatus], None]) -> None:
        """
        Register a callback to be called when job status changes.
        
        Args:
            callback: Function to call with JobStatus on status changes
        """
        self._status_callbacks.append(callback)
    
    def wait_until_complete(self, timeout: Optional[int] = None) -> JobStatus:
        """
        Block until job completes or timeout is reached.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            Final job status
            
        Raises:
            TimeoutError: If timeout is exceeded
        """
        start_time = time.time()
        
        while not self.status.is_terminal:
            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError(f"Job {self.job_id} timed out after {timeout} seconds")
            
            time.sleep(1)
            self.get_status()
        
        return self.status
    
    def is_complete(self) -> bool:
        """Check if job is complete (terminal state)."""
        return self.status.is_terminal
    
    def is_successful(self) -> bool:
        """Check if job completed successfully."""
        return self.status.is_successful
    
    def _trigger_completion_callbacks(self) -> None:
        """Trigger all completion callbacks."""
        if not self._completion_callbacks:
            return
        
        try:
            results = self.get_results() if self.status.is_successful else None
            for callback in self._completion_callbacks:
                try:
                    callback(results)
                except Exception as e:
                    # Don't let callback errors break the job
                    pass
        except Exception:
            # If we can't get results, call callbacks with None
            for callback in self._completion_callbacks:
                try:
                    callback(None)
                except Exception:
                    pass
    
    def display(self):
        """Display job information in Jupyter notebook (if available)."""
        try:
            from .jupyter import display_job
            display_job(self)
        except ImportError:
            # Fall back to text display
            print(f"Job ID: {self.job_id}")
            print(f"Status: {self.status}")
            if self.status.is_terminal and self.status.is_successful:
                results = self.get_results()
                print(f"Results: {len(results.outputs)} outputs")
    
    def __repr__(self) -> str:
        return f"Job(id='{self.job_id}', status='{self.status.state.value}')"
