"""
Job Builder - Programmatic job construction using builder pattern
"""

import yaml
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import tempfile

from .job import Job
from .exceptions import ValidationError


class JobBuilder:
    """
    Builder class for programmatically constructing QETL job configurations.
    
    Provides a fluent interface for building complex pipeline configurations
    without manually writing YAML.
    """
    
    def __init__(self, client):
        self._client = client
        self._config = {
            "pipeline_name": "Programmatic Pipeline",
            "version": "1.0",
            "input_sources": [],
            "transformations": [],
            "outputs": []
        }
        self._execution_params = {}
    
    def set_name(self, name: str) -> "JobBuilder":
        """
        Set pipeline name.
        
        Args:
            name: Pipeline name
            
        Returns:
            Self for chaining
        """
        self._config["pipeline_name"] = name
        return self
    
    def set_version(self, version: str) -> "JobBuilder":
        """
        Set pipeline version.
        
        Args:
            version: Version string
            
        Returns:
            Self for chaining
        """
        self._config["version"] = version
        return self
    
    def set_description(self, description: str) -> "JobBuilder":
        """
        Set pipeline description.
        
        Args:
            description: Description text
            
        Returns:
            Self for chaining
        """
        self._config["description"] = description
        return self
    
    def add_input_source(
        self,
        name: str,
        path: str,
        source_type: str = "auto",
        config: Optional[Dict[str, Any]] = None
    ) -> "JobBuilder":
        """
        Add an input data source.
        
        Args:
            name: Source identifier
            path: Path to data source
            source_type: Type of data source (csv, json, pdb, sdf, etc.)
            config: Optional source-specific configuration
            
        Returns:
            Self for chaining
        """
        source = {
            "name": name,
            "path": path
        }
        
        if source_type != "auto":
            source["type"] = source_type
        
        if config:
            source["config"] = config
        
        self._config["input_sources"].append(source)
        return self
    
    def add_transformation(
        self,
        component: str,
        config: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
        dependencies: Optional[List[str]] = None
    ) -> "JobBuilder":
        """
        Add a processing transformation component.
        
        Args:
            component: Component identifier
            config: Component-specific configuration
            name: Optional custom name for this transformation
            dependencies: List of transformation names this depends on
            
        Returns:
            Self for chaining
        """
        transformation = {
            "component": component
        }
        
        if name:
            transformation["name"] = name
        
        if config:
            transformation["config"] = config
        
        if dependencies:
            transformation["dependencies"] = dependencies
        
        self._config["transformations"].append(transformation)
        return self
    
    def add_output(
        self,
        name: str,
        path: str,
        format: str = "json",
        config: Optional[Dict[str, Any]] = None
    ) -> "JobBuilder":
        """
        Add an output configuration.
        
        Args:
            name: Output identifier
            path: Output path
            format: Output format (json, csv, etc.)
            config: Optional output-specific configuration
            
        Returns:
            Self for chaining
        """
        output = {
            "name": name,
            "path": path,
            "format": format
        }
        
        if config:
            output["config"] = config
        
        self._config["outputs"].append(output)
        return self
    
    def set_execution_params(
        self,
        priority: int = 50,
        timeout: int = 3600,
        notifications: Optional[List[str]] = None
    ) -> "JobBuilder":
        """
        Set job execution parameters.
        
        Args:
            priority: Job priority (1-100, higher = more priority)
            timeout: Maximum execution time in seconds
            notifications: List of notification endpoints
            
        Returns:
            Self for chaining
        """
        self._execution_params = {
            "priority": priority,
            "timeout": timeout
        }
        
        if notifications:
            self._execution_params["notifications"] = notifications
        
        return self
    
    def add_quantum_homology_analyzer(
        self,
        dimensions: int = 4,
        precision: str = "high",
        name: Optional[str] = None,
        **kwargs
    ) -> "JobBuilder":
        """
        Convenience method for adding quantum homology analyzer.
        
        Args:
            dimensions: Number of dimensions for analysis
            precision: Precision level (low, medium, high)
            name: Optional custom name
            **kwargs: Additional configuration
            
        Returns:
            Self for chaining
        """
        config = {
            "dimensions": dimensions,
            "precision": precision,
            **kwargs
        }
        
        return self.add_transformation(
            "quantum_homology_analyzer",
            config=config,
            name=name
        )
    
    def add_williams_pebbler(
        self,
        optimization_level: int = 2,
        name: Optional[str] = None,
        **kwargs
    ) -> "JobBuilder":
        """
        Convenience method for adding Williams pebbling optimizer.
        
        Args:
            optimization_level: Optimization level (1-3)
            name: Optional custom name
            **kwargs: Additional configuration
            
        Returns:
            Self for chaining
        """
        config = {
            "optimization_level": optimization_level,
            **kwargs
        }
        
        return self.add_transformation(
            "williams_pebbler",
            config=config,
            name=name
        )
    
    def add_holographic_grover(
        self,
        search_iterations: int = 1000,
        name: Optional[str] = None,
        dependencies: Optional[List[str]] = None,
        **kwargs
    ) -> "JobBuilder":
        """
        Convenience method for adding holographic Grover search.
        
        Args:
            search_iterations: Number of search iterations
            name: Optional custom name
            dependencies: List of transformation dependencies
            **kwargs: Additional configuration
            
        Returns:
            Self for chaining
        """
        config = {
            "search_iterations": search_iterations,
            **kwargs
        }
        
        return self.add_transformation(
            "holographic_grover",
            config=config,
            name=name,
            dependencies=dependencies
        )
    
    def add_wave_encoder(
        self,
        encoding_type: str = "quantum_fourier",
        name: Optional[str] = None,
        **kwargs
    ) -> "JobBuilder":
        """
        Convenience method for adding wave encoder.
        
        Args:
            encoding_type: Type of encoding (quantum_fourier, molecular_orbital, etc.)
            name: Optional custom name
            **kwargs: Additional configuration
            
        Returns:
            Self for chaining
        """
        config = {
            "encoding_type": encoding_type,
            **kwargs
        }
        
        return self.add_transformation(
            "wave_encoder",
            config=config,
            name=name
        )
    
    def add_wave_decoder(
        self,
        decoding_type: str = "quantum_fourier",
        name: Optional[str] = None,
        **kwargs
    ) -> "JobBuilder":
        """
        Convenience method for adding wave decoder.
        
        Args:
            decoding_type: Type of decoding
            name: Optional custom name
            **kwargs: Additional configuration
            
        Returns:
            Self for chaining
        """
        config = {
            "decoding_type": decoding_type,
            **kwargs
        }
        
        return self.add_transformation(
            "wave_decoder",
            config=config,
            name=name
        )
    
    def validate(self) -> bool:
        """
        Validate the current job configuration.
        
        Returns:
            True if configuration is valid
            
        Raises:
            ValidationError: If configuration is invalid
        """
        errors = []
        
        # Check required fields
        if not self._config.get("pipeline_name"):
            errors.append("Pipeline name is required")
            
        if not self._config.get("input_sources"):
            errors.append("At least one input source is required")
            
        if not self._config.get("transformations"):
            errors.append("At least one transformation is required")
            
        # Validate dependencies
        transformation_names = set()
        for transform in self._config.get("transformations", []):
            if "name" in transform:
                transformation_names.add(transform["name"])
        
        for transform in self._config.get("transformations", []):
            # Check dependencies at the transformation level
            dependencies = transform.get("dependencies", [])
            if isinstance(dependencies, list):
                for dep in dependencies:
                    if dep not in transformation_names:
                        errors.append(f"Unknown dependency: {dep}")
            # Also check dependencies in config if they exist there
            config_deps = transform.get("config", {}).get("dependencies", [])
            if isinstance(config_deps, list):
                for dep in config_deps:
                    if dep not in transformation_names:
                        errors.append(f"Unknown dependency: {dep}")
        
        if errors:
            error_msg = "; ".join(errors)
            raise ValidationError(error_msg)
            
        return True
    
    def to_yaml(self) -> str:
        """
        Export job configuration as YAML string.
        
        Returns:
            YAML configuration string
        """
        config = self._config.copy()
        
        if self._execution_params:
            config["execution"] = self._execution_params
        
        return yaml.dump(config, default_flow_style=False, sort_keys=False)
    
    def save_yaml(self, filepath: Union[str, Path]) -> Path:
        """
        Save job configuration to YAML file.
        
        Args:
            filepath: Path to save YAML file
            
        Returns:
            Path object for the saved file
        """
        filepath = Path(filepath)
        
        with open(filepath, 'w') as f:
            f.write(self.to_yaml())
        
        return filepath
    
    def submit(self) -> Job:
        """
        Submit the constructed job for execution.
        
        Returns:
            Job object for monitoring
            
        Raises:
            ValidationError: If job configuration is invalid
        """
        # Validate before submission
        self.validate()
        
        # Create temporary YAML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(self.to_yaml())
            temp_yaml_path = Path(f.name)
        
        try:
            # Submit job using the client
            job = self._client.submit_job(temp_yaml_path, **self._execution_params)
            return job
        finally:
            # Clean up temporary file
            try:
                temp_yaml_path.unlink()
            except:
                pass  # Don't fail if cleanup fails
    
    def clone(self) -> "JobBuilder":
        """
        Create a copy of this job builder.
        
        Returns:
            New JobBuilder instance with same configuration
        """
        new_builder = JobBuilder(self._client)
        new_builder._config = self._config.copy()
        new_builder._execution_params = self._execution_params.copy()
        return new_builder
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get current job configuration.
        
        Returns:
            Dictionary containing current configuration
        """
        config = self._config.copy()
        
        if self._execution_params:
            config["execution"] = self._execution_params
        
        return config
    
    def __repr__(self) -> str:
        transform_count = len(self._config.get("transformations", []))
        input_count = len(self._config.get("input_sources", []))
        return f"JobBuilder(inputs={input_count}, transformations={transform_count})"
