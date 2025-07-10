"""
Tests for QETL SDK Client
"""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import Mock, patch

from qetl_sdk.client import QETLClient
from qetl_sdk.exceptions import QETLError, ConfigurationError
from qetl_sdk.job import JobState


class TestQETLClient:
    """Test cases for QETLClient."""
    
    def test_init_local_mode(self):
        """Test client initialization in local mode."""
        with patch('qetl_sdk.client.LocalRunner') as mock_runner:
            client = QETLClient(mode="local")
            assert client.mode == "local"
            mock_runner.assert_called_once()
    
    def test_init_cloud_mode(self):
        """Test client initialization in cloud mode."""
        with patch('qetl_sdk.client.CloudClient') as mock_client:
            client = QETLClient(mode="cloud", instance_id="test-instance", api_key="test-key")
            assert client.mode == "cloud"
            mock_client.assert_called_once_with(
                instance_id="test-instance",
                api_key="test-key",
                base_url=None,
                timeout=300
            )
    
    def test_init_cloud_mode_no_api_key(self):
        """Test cloud mode initialization without API key."""
        with pytest.raises(QETLError, match="API key is required"):
            QETLClient(mode="cloud")
    
    def test_submit_job_local(self):
        """Test job submission in local mode."""
        with patch('qetl_sdk.client.LocalRunner') as mock_runner:
            mock_backend = Mock()
            mock_runner.return_value = mock_backend
            
            client = QETLClient(mode="local")
            
            # Create temporary YAML file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                yaml.dump({"pipeline_name": "test"}, f)
                yaml_path = Path(f.name)
            
            try:
                client.submit_job(yaml_path)
                mock_backend.submit_job.assert_called_once()
            finally:
                yaml_path.unlink()
    
    def test_submit_job_file_not_found(self):
        """Test job submission with non-existent file."""
        with patch('qetl_sdk.client.LocalRunner'):
            client = QETLClient(mode="local")

            with pytest.raises(QETLError, match="YAML file not found"):
                client.submit_job(Path("nonexistent.yaml"))
    
    def test_list_jobs(self):
        """Test job listing."""
        with patch('qetl_sdk.client.LocalRunner') as mock_runner:
            mock_backend = Mock()
            mock_backend.list_jobs.return_value = []
            mock_runner.return_value = mock_backend
            
            client = QETLClient(mode="local")
            jobs = client.list_jobs()
            
            assert isinstance(jobs, list)
            mock_backend.list_jobs.assert_called_once()
    
    def test_get_job(self):
        """Test getting specific job."""
        with patch('qetl_sdk.client.LocalRunner') as mock_runner:
            mock_backend = Mock()
            mock_runner.return_value = mock_backend
            
            client = QETLClient(mode="local")
            client.get_job("test-job-id")
            
            mock_backend.get_job.assert_called_once_with("test-job-id")
    
    def test_list_components(self):
        """Test component listing."""
        with patch('qetl_sdk.client.LocalRunner') as mock_runner:
            mock_backend = Mock()
            mock_backend.list_components.return_value = []
            mock_runner.return_value = mock_backend
            
            client = QETLClient(mode="local")
            components = client.list_components()
            
            assert isinstance(components, list)
            mock_backend.list_components.assert_called_once()
    
    def test_validate_yaml(self):
        """Test YAML validation."""
        with patch('qetl_sdk.client.LocalRunner') as mock_runner:
            mock_backend = Mock()
            mock_backend.validate_yaml.return_value = {"valid": True}
            mock_runner.return_value = mock_backend
            
            client = QETLClient(mode="local")
            
            # Create temporary YAML file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                yaml.dump({"pipeline_name": "test"}, f)
                yaml_path = Path(f.name)
            
            try:
                result = client.validate_yaml(yaml_path)
                assert result["valid"] is True
                mock_backend.validate_yaml.assert_called_once()
            finally:
                yaml_path.unlink()
    
    def test_get_instance_info(self):
        """Test instance info retrieval."""
        with patch('qetl_sdk.client.LocalRunner') as mock_runner:
            mock_backend = Mock()
            mock_backend.get_instance_info.return_value = {"mode": "local"}
            mock_runner.return_value = mock_backend
            
            client = QETLClient(mode="local")
            info = client.get_instance_info()
            
            assert info["mode"] == "local"
            mock_backend.get_instance_info.assert_called_once()
    
    def test_context_manager(self):
        """Test client as context manager."""
        with patch('qetl_sdk.client.LocalRunner') as mock_runner:
            mock_backend = Mock()
            mock_runner.return_value = mock_backend
            
            with QETLClient(mode="local") as client:
                assert client.mode == "local"
            
            # Context manager should work without errors
    
    def test_create_job_builder(self):
        """Test job builder creation."""
        with patch('qetl_sdk.client.LocalRunner'):
            client = QETLClient(mode="local")
            builder = client.create_job_builder()
            
            assert builder is not None
            assert builder._client is client
