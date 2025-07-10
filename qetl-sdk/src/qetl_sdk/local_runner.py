"""
Local Runner - Backend for running QETL jobs locally
"""

import os
import sys
import subprocess
import uuid
import json
import time
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import yaml
import threading
from datetime import datetime, timezone

from .job import Job, JobStatus, JobResults, JobState
from .exceptions import QETLError, ValidationError, JobExecutionError, ConfigurationError

logger = logging.getLogger(__name__)


class LocalRunner:
    """
    Backend for running QETL jobs using local QETL installation.
    
    This class interfaces with the existing YAML pipeline runner to execute
    jobs locally before cloud infrastructure is available.
    """
    
    def __init__(
        self,
        qetl_home: Optional[str] = None,
        timeout: int = 300,
        **kwargs
    ):
        self.qetl_home = Path(qetl_home) if qetl_home else self._find_qetl_home()
        self.timeout = timeout
        self.jobs: Dict[str, Dict[str, Any]] = {}  # In-memory job storage
        self._job_lock = threading.Lock()
        
        # Validate QETL installation
        self._validate_installation()
        
        logger.info(f"Local runner initialized with QETL_HOME: {self.qetl_home}")
    
    def _find_qetl_home(self) -> Path:
        """Find QETL installation directory."""
        # Check environment variable
        if "QETL_HOME" in os.environ:
            return Path(os.environ["QETL_HOME"])
        
        # Check current directory and parents
        current = Path.cwd()
        for path in [current] + list(current.parents):
            if (path / "core" / "quantum_mathematics_engine.py").exists():
                return path
            if (path / "yaml_pipeline_runner" / "main.py").exists():
                return path
        
        raise ConfigurationError("QETL installation not found. Set QETL_HOME environment variable.")
    
    def _validate_installation(self) -> None:
        """Validate that QETL installation is complete."""
        required_paths = [
            self.qetl_home / "core" / "quantum_mathematics_engine.py",
            self.qetl_home / "yaml_pipeline_runner" / "main.py"
        ]
        
        for path in required_paths:
            if not path.exists():
                raise ConfigurationError(f"Required QETL component not found: {path}")
        
        # Check if Python dependencies are available
        try:
            sys.path.insert(0, str(self.qetl_home))
            import yaml_pipeline_runner.main
        except ImportError as e:
            raise ConfigurationError(f"QETL dependencies not available: {e}")
    
    def submit_job(self, yaml_path: Path, **kwargs) -> Job:
        """
        Submit a job from YAML configuration.
        
        Args:
            yaml_path: Path to YAML configuration file
            **kwargs: Additional job parameters
            
        Returns:
            Job object for monitoring
        """
        job_id = str(uuid.uuid4())
        
        # Validate YAML first
        try:
            self.validate_yaml(yaml_path)
        except ValidationError as e:
            logger.error(f"YAML validation failed for job {job_id}: {e}")
            raise
        
        # Create job record
        job_data = {
            "job_id": job_id,
            "yaml_path": str(yaml_path),
            "status": JobState.SUBMITTED,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "progress": 0.0,
            "message": "Job submitted",
            "kwargs": kwargs
        }
        
        with self._job_lock:
            self.jobs[job_id] = job_data
        
        # Start job execution in background thread
        thread = threading.Thread(target=self._execute_job, args=(job_id,))
        thread.daemon = True
        thread.start()
        
        initial_status = JobStatus(
            job_id=job_id,
            state=JobState.SUBMITTED,
            progress=0.0,
            message="Job submitted",
            created_at=job_data["created_at"]
        )
        
        logger.info(f"Job {job_id} submitted from {yaml_path}")
        return Job(job_id, self, initial_status)
    
    def _execute_job(self, job_id: str) -> None:
        """Execute job in background thread."""
        try:
            with self._job_lock:
                job_data = self.jobs[job_id].copy()
            
            yaml_path = Path(job_data["yaml_path"])
            
            # Update status to validating
            self._update_job_status(job_id, JobState.VALIDATING, 10.0, "Validating configuration")
            
            # Load and validate YAML
            with open(yaml_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Update status to queued
            self._update_job_status(job_id, JobState.QUEUED, 20.0, "Job queued for execution")
            
            # Update status to initializing
            self._update_job_status(job_id, JobState.INITIALIZING, 30.0, "Initializing execution environment")
            
            # Update status to running
            self._update_job_status(job_id, JobState.RUNNING, 40.0, "Executing pipeline")
            
            # Execute the pipeline using the existing YAML runner
            start_time = time.time()
            result = self._run_yaml_pipeline(yaml_path, job_id)
            execution_time = time.time() - start_time
            
            # Update status to completing
            self._update_job_status(job_id, JobState.COMPLETING, 90.0, "Processing results")
            
            # Store results
            job_results = JobResults(
                job_id=job_id,
                outputs=result.get("outputs", {}),
                execution_time=execution_time,
                logs=result.get("logs", []),
                metrics=result.get("metrics", {})
            )
            
            with self._job_lock:
                self.jobs[job_id]["results"] = job_results
            
            # Update status to completed
            self._update_job_status(job_id, JobState.COMPLETED, 100.0, "Job completed successfully")
            
            logger.info(f"Job {job_id} completed successfully in {execution_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}")
            self._update_job_status(job_id, JobState.FAILED, progress=None, message=str(e))
            
            # Store error information
            with self._job_lock:
                self.jobs[job_id]["error"] = {
                    "type": type(e).__name__,
                    "message": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
    
    def _run_yaml_pipeline(self, yaml_path: Path, job_id: str) -> Dict[str, Any]:
        """
        Execute the YAML pipeline using the existing runner.
        
        Args:
            yaml_path: Path to YAML configuration
            job_id: Job identifier for logging
            
        Returns:
            Dictionary containing execution results
        """
        try:
            # Set up environment
            env = os.environ.copy()
            env["PYTHONPATH"] = str(self.qetl_home)
            
            # Run the YAML pipeline runner
            runner_path = self.qetl_home / "yaml_pipeline_runner" / "main.py"
            cmd = [
                sys.executable,
                str(runner_path),
                "--config", str(yaml_path),
                "--job-id", job_id
            ]
            
            # Execute with timeout
            process = subprocess.run(
                cmd,
                cwd=str(self.qetl_home),
                env=env,
                capture_output=True,
                text=True,
                timeout=self.timeout
            )
            
            if process.returncode != 0:
                raise JobExecutionError(f"Pipeline execution failed: {process.stderr}")
            
            # Parse output for results
            result = {
                "outputs": self._parse_pipeline_outputs(process.stdout),
                "logs": process.stdout.split('\n') if process.stdout else [],
                "metrics": {
                    "return_code": process.returncode,
                    "execution_command": ' '.join(cmd)
                }
            }
            
            return result
            
        except subprocess.TimeoutExpired:
            raise JobExecutionError(f"Pipeline execution timed out after {self.timeout} seconds")
        except Exception as e:
            raise JobExecutionError(f"Failed to execute pipeline: {e}")
    
    def _parse_pipeline_outputs(self, stdout: str) -> Dict[str, Any]:
        """
        Parse pipeline outputs from stdout.
        
        This is a simplified parser - in practice, the YAML runner would
        need to be modified to output structured results.
        """
        outputs = {}
        
        if not stdout:
            return outputs
        
        # Look for output patterns in the logs
        lines = stdout.split('\n')
        for line in lines:
            if 'Processing complete' in line:
                outputs['status'] = 'completed'
            elif 'Results saved to' in line:
                # Extract file path
                parts = line.split('Results saved to')
                if len(parts) > 1:
                    outputs['result_path'] = parts[1].strip()
        
        # Default output if no specific patterns found
        if not outputs:
            outputs = {
                "status": "completed",
                "message": "Pipeline execution completed",
                "stdout": stdout
            }
        
        return outputs
    
    def _update_job_status(
        self, 
        job_id: str, 
        state: JobState, 
        progress: Optional[float], 
        message: str
    ) -> None:
        """Update job status."""
        with self._job_lock:
            if job_id in self.jobs:
                self.jobs[job_id].update({
                    "status": state,
                    "progress": progress,
                    "message": message,
                    "updated_at": datetime.now(timezone.utc)
                })
    
    def get_job_status(self, job_id: str) -> JobStatus:
        """Get current job status."""
        with self._job_lock:
            if job_id not in self.jobs:
                raise QETLError(f"Job not found: {job_id}")
            
            job_data = self.jobs[job_id]
            
            return JobStatus(
                job_id=job_id,
                state=job_data["status"],
                progress=job_data.get("progress", 0.0),
                message=job_data.get("message", ""),
                created_at=job_data["created_at"],
                updated_at=job_data["updated_at"]
            )
    
    def get_job_results(self, job_id: str) -> JobResults:
        """Get job results."""
        with self._job_lock:
            if job_id not in self.jobs:
                raise QETLError(f"Job not found: {job_id}")
            
            job_data = self.jobs[job_id]
            
            if "results" not in job_data:
                raise QETLError(f"Results not available for job {job_id}")
            
            return job_data["results"]
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job."""
        with self._job_lock:
            if job_id not in self.jobs:
                return False
            
            job_data = self.jobs[job_id]
            
            if job_data["status"].is_terminal:
                return False
            
            # Update status to cancelled
            self.jobs[job_id].update({
                "status": JobState.CANCELLED,
                "message": "Job cancelled by user",
                "updated_at": datetime.now(timezone.utc)
            })
            
            return True
    
    def list_jobs(
        self, 
        status: Optional[str] = None, 
        limit: int = 100,
        **kwargs
    ) -> List[Job]:
        """List jobs with optional filtering."""
        with self._job_lock:
            jobs = []
            
            for job_id, job_data in list(self.jobs.items()):
                if status and job_data["status"].value != status:
                    continue
                
                job_status = JobStatus(
                    job_id=job_id,
                    state=job_data["status"],
                    progress=job_data.get("progress", 0.0),
                    message=job_data.get("message", ""),
                    created_at=job_data["created_at"],
                    updated_at=job_data["updated_at"]
                )
                
                jobs.append(Job(job_id, self, job_status))
                
                if len(jobs) >= limit:
                    break
            
            return jobs
    
    def get_job(self, job_id: str) -> Job:
        """Get specific job by ID."""
        status = self.get_job_status(job_id)
        return Job(job_id, self, status)
    
    def get_job_logs(self, job_id: str, follow: bool = False) -> List[str]:
        """Get job logs."""
        with self._job_lock:
            if job_id not in self.jobs:
                raise QETLError(f"Job not found: {job_id}")
            
            job_data = self.jobs[job_id]
            
            if "results" in job_data:
                return job_data["results"].logs
            
            # Return status messages as logs if results not available
            return [job_data.get("message", "No logs available")]
    
    def list_components(self) -> List[Dict[str, Any]]:
        """List available QETL components."""
        # This would ideally introspect the QETL installation
        # For now, return a basic list based on known components
        components = [
            {
                "name": "wave_encoder",
                "description": "Quantum wave encoding component",
                "version": "1.0",
                "type": "encoder"
            },
            {
                "name": "wave_decoder", 
                "description": "Quantum wave decoding component",
                "version": "1.0",
                "type": "decoder"
            },
            {
                "name": "quantum_homology_analyzer",
                "description": "Quantum homology analysis component",
                "version": "1.0",
                "type": "analyzer"
            },
            {
                "name": "williams_pebbler",
                "description": "Williams pebbling optimization component",
                "version": "1.0", 
                "type": "optimizer"
            },
            {
                "name": "holographic_grover",
                "description": "Holographic Grover search component",
                "version": "1.0",
                "type": "search"
            }
        ]
        
        return components
    
    def validate_yaml(self, yaml_path: Path) -> Dict[str, Any]:
        """Validate YAML configuration."""
        try:
            with open(yaml_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Basic validation
            required_fields = ["pipeline_name", "input_sources", "transformations"]
            errors = []
            
            for field in required_fields:
                if field not in config:
                    errors.append(f"Missing required field: {field}")
            
            if errors:
                raise ValidationError("YAML validation failed", validation_errors=errors)
            
            return {
                "valid": True,
                "message": "YAML configuration is valid",
                "config": config
            }
            
        except yaml.YAMLError as e:
            raise ValidationError(f"Invalid YAML format: {e}")
        except FileNotFoundError:
            raise ValidationError(f"YAML file not found: {yaml_path}")
    
    def get_instance_info(self) -> Dict[str, Any]:
        """Get local instance information."""
        return {
            "mode": "local",
            "qetl_home": str(self.qetl_home),
            "python_version": sys.version,
            "active_jobs": len([j for j in self.jobs.values() if not j["status"].is_terminal]),
            "total_jobs": len(self.jobs)
        }
