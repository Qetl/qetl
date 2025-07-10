"""
Tests for JobBuilder
"""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import Mock

from qetl_sdk.builder import JobBuilder
from qetl_sdk.exceptions import ValidationError


class TestJobBuilder:
    """Test cases for JobBuilder."""
    
    def setup_method(self):
        """Setup for each test."""
        self.mock_client = Mock()
        self.builder = JobBuilder(self.mock_client)
    
    def test_init(self):
        """Test builder initialization."""
        assert self.builder._client is not None
        assert self.builder._config["pipeline_name"] == "Programmatic Pipeline"
        assert isinstance(self.builder._config["input_sources"], list)
        assert isinstance(self.builder._config["transformations"], list)
    
    def test_set_name(self):
        """Test setting pipeline name."""
        result = self.builder.set_name("Test Pipeline")
        
        assert result is self.builder  # Check fluent interface
        assert self.builder._config["pipeline_name"] == "Test Pipeline"
    
    def test_set_version(self):
        """Test setting pipeline version."""
        self.builder.set_version("2.0")
        assert self.builder._config["version"] == "2.0"
    
    def test_set_description(self):
        """Test setting pipeline description."""
        self.builder.set_description("Test description")
        assert self.builder._config["description"] == "Test description"
    
    def test_add_input_source(self):
        """Test adding input source."""
        self.builder.add_input_source("test_input", "/path/to/data.csv", "csv")
        
        sources = self.builder._config["input_sources"]
        assert len(sources) == 1
        assert sources[0]["name"] == "test_input"
        assert sources[0]["path"] == "/path/to/data.csv"
        assert sources[0]["type"] == "csv"
    
    def test_add_input_source_with_config(self):
        """Test adding input source with configuration."""
        config = {"delimiter": ",", "header": True}
        self.builder.add_input_source("test_input", "/path/to/data.csv", config=config)
        
        sources = self.builder._config["input_sources"]
        assert sources[0]["config"] == config
    
    def test_add_transformation(self):
        """Test adding transformation."""
        self.builder.add_transformation("test_component", {"param": "value"})
        
        transforms = self.builder._config["transformations"]
        assert len(transforms) == 1
        assert transforms[0]["component"] == "test_component"
        assert transforms[0]["config"] == {"param": "value"}
    
    def test_add_transformation_with_dependencies(self):
        """Test adding transformation with dependencies."""
        self.builder.add_transformation("comp1", name="transform1")
        self.builder.add_transformation("comp2", name="transform2", dependencies=["transform1"])
        
        transforms = self.builder._config["transformations"]
        assert len(transforms) == 2
        assert transforms[1]["dependencies"] == ["transform1"]
    
    def test_add_output(self):
        """Test adding output configuration."""
        self.builder.add_output("results", "/path/to/output.json", "json")
        
        outputs = self.builder._config["outputs"]
        assert len(outputs) == 1
        assert outputs[0]["name"] == "results"
        assert outputs[0]["path"] == "/path/to/output.json"
        assert outputs[0]["format"] == "json"
    
    def test_set_execution_params(self):
        """Test setting execution parameters."""
        self.builder.set_execution_params(priority=80, timeout=7200)
        
        assert self.builder._execution_params["priority"] == 80
        assert self.builder._execution_params["timeout"] == 7200
    
    def test_add_quantum_homology_analyzer(self):
        """Test convenience method for quantum homology analyzer."""
        self.builder.add_quantum_homology_analyzer(dimensions=6, precision="high")
        
        transforms = self.builder._config["transformations"]
        assert len(transforms) == 1
        assert transforms[0]["component"] == "quantum_homology_analyzer"
        assert transforms[0]["config"]["dimensions"] == 6
        assert transforms[0]["config"]["precision"] == "high"
    
    def test_add_williams_pebbler(self):
        """Test convenience method for Williams pebbler."""
        self.builder.add_williams_pebbler(optimization_level=3)
        
        transforms = self.builder._config["transformations"]
        assert len(transforms) == 1
        assert transforms[0]["component"] == "williams_pebbler"
        assert transforms[0]["config"]["optimization_level"] == 3
    
    def test_add_holographic_grover(self):
        """Test convenience method for holographic Grover."""
        self.builder.add_holographic_grover(search_iterations=2000)
        
        transforms = self.builder._config["transformations"]
        assert len(transforms) == 1
        assert transforms[0]["component"] == "holographic_grover"
        assert transforms[0]["config"]["search_iterations"] == 2000
    
    def test_add_wave_encoder(self):
        """Test convenience method for wave encoder."""
        self.builder.add_wave_encoder(encoding_type="molecular_orbital")
        
        transforms = self.builder._config["transformations"]
        assert len(transforms) == 1
        assert transforms[0]["component"] == "wave_encoder"
        assert transforms[0]["config"]["encoding_type"] == "molecular_orbital"
    
    def test_add_wave_decoder(self):
        """Test convenience method for wave decoder."""
        self.builder.add_wave_decoder(decoding_type="quantum_fourier")
        
        transforms = self.builder._config["transformations"]
        assert len(transforms) == 1
        assert transforms[0]["component"] == "wave_decoder"
        assert transforms[0]["config"]["decoding_type"] == "quantum_fourier"
    
    def test_validate_valid_config(self):
        """Test validation of valid configuration."""
        self.builder.add_input_source("input1", "/path/data.csv")
        self.builder.add_transformation("test_component")
        
        result = self.builder.validate()
        assert result is True
    
    def test_validate_missing_input(self):
        """Test validation with missing input sources."""
        self.builder.add_transformation("test_component")
        
        with pytest.raises(ValidationError) as exc_info:
            self.builder.validate()
        
        assert "input source" in str(exc_info.value)
    
    def test_validate_missing_transformations(self):
        """Test validation with missing transformations."""
        self.builder.add_input_source("input1", "/path/data.csv")
        
        with pytest.raises(ValidationError) as exc_info:
            self.builder.validate()
        
        assert "transformation" in str(exc_info.value)
    
    def test_validate_unknown_dependency(self):
        """Test validation with unknown dependency."""
        self.builder.add_input_source("input1", "/path/data.csv")
        self.builder.add_transformation("comp1", dependencies=["unknown_transform"])
        
        with pytest.raises(ValidationError) as exc_info:
            self.builder.validate()
        
        assert "Unknown dependency" in str(exc_info.value)
    
    def test_to_yaml(self):
        """Test YAML export."""
        self.builder.add_input_source("input1", "/path/data.csv")
        self.builder.add_transformation("test_component")
        
        yaml_str = self.builder.to_yaml()
        assert isinstance(yaml_str, str)
        
        # Parse YAML to verify structure
        config = yaml.safe_load(yaml_str)
        assert config["pipeline_name"] == "Programmatic Pipeline"
        assert len(config["input_sources"]) == 1
        assert len(config["transformations"]) == 1
    
    def test_save_yaml(self):
        """Test saving YAML to file."""
        self.builder.add_input_source("input1", "/path/data.csv")
        self.builder.add_transformation("test_component")
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            filepath = Path(f.name)
        
        try:
            result_path = self.builder.save_yaml(filepath)
            assert result_path == filepath
            assert filepath.exists()
            
            # Verify content
            with open(filepath, 'r') as f:
                config = yaml.safe_load(f)
            
            assert config["pipeline_name"] == "Programmatic Pipeline"
        finally:
            if filepath.exists():
                filepath.unlink()
    
    def test_submit(self):
        """Test job submission."""
        self.builder.add_input_source("input1", "/path/data.csv")
        self.builder.add_transformation("test_component")
        
        mock_job = Mock()
        self.mock_client.submit_job.return_value = mock_job
        
        job = self.builder.submit()
        
        assert job is mock_job
        self.mock_client.submit_job.assert_called_once()
    
    def test_clone(self):
        """Test builder cloning."""
        self.builder.set_name("Original Pipeline")
        self.builder.add_input_source("input1", "/path/data.csv")
        
        cloned = self.builder.clone()
        
        assert cloned is not self.builder
        assert cloned._config["pipeline_name"] == "Original Pipeline"
        assert len(cloned._config["input_sources"]) == 1
    
    def test_get_config(self):
        """Test getting current configuration."""
        self.builder.add_input_source("input1", "/path/data.csv")
        self.builder.set_execution_params(priority=90)
        
        config = self.builder.get_config()
        
        assert config["pipeline_name"] == "Programmatic Pipeline"
        assert len(config["input_sources"]) == 1
        assert config["execution"]["priority"] == 90
    
    def test_fluent_interface(self):
        """Test fluent interface chaining."""
        result = (self.builder
                 .set_name("Chained Pipeline")
                 .set_version("1.5")
                 .add_input_source("input1", "/path/data.csv")
                 .add_transformation("comp1")
                 .add_output("out1", "/path/output.json"))
        
        assert result is self.builder
        assert self.builder._config["pipeline_name"] == "Chained Pipeline"
        assert self.builder._config["version"] == "1.5"
        assert len(self.builder._config["input_sources"]) == 1
        assert len(self.builder._config["transformations"]) == 1
        assert len(self.builder._config["outputs"]) == 1
