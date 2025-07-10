# QETL SDK

[![PyPI version](https://badge.fury.io/py/qetl-sdk.svg)](https://badge.fury.io/py/qetl-sdk)
[![Python Support](https://img.shields.io/pypi/pyversions/qetl-sdk.svg)](https://pypi.org/project/qetl-sdk/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

The QETL SDK is a Python library for interacting with the Quantum Enhanced Transform Layer (QETL) pipeline processing system. It provides a programmatic interface for submitting, monitoring, and managing quantum bioinformatics and data processing jobs.

## Features

- **Unified Interface**: Single API for both local and cloud QETL deployments
- **YAML-First Configuration**: Intuitive pipeline configuration using YAML
- **Async Support**: Full async/await support for non-blocking operations
- **Rich Visualization**: Built-in support for result visualization and analysis
- **Enterprise Ready**: Authentication, authorization, and audit logging
- **Jupyter Integration**: Seamless integration with Jupyter notebooks

## Installation

```bash
# Install base SDK
pip install qetl-sdk

# Install with optional dependencies
pip install qetl-sdk[jupyter,visualization,cloud]

# Install development version
pip install qetl-sdk[dev]
```

## Quick Start

### Local Mode (Current QETL Installation)

```python
from qetl_sdk import QETLClient

# Initialize client for local processing
client = QETLClient(mode="local")

# Submit job from YAML file
job = client.submit_job("pipeline_config.yaml")

# Monitor job progress
status = job.get_status()
print(f"Job Status: {status.state}")

# Get results when complete
results = job.get_results_when_done()
print(f"Results: {results}")
```

### Cloud Mode (QETL Enterprise Cloud)

```python
from qetl_sdk import QETLClient

# Initialize client for cloud processing
client = QETLClient(
    mode="cloud",
    instance_id="your-instance-id",
    api_key="your-api-key"
)

# Programmatically build pipeline
job = client.create_job() \
    .add_input_source("drug_data", "s3://bucket/data.csv") \
    .add_transformation("quantum_homology_analyzer", {
        "dimensions": 4,
        "precision": "high"
    }) \
    .add_output("results", "s3://bucket/results/") \
    .submit()

# Wait for completion with callback
job.on_completion(lambda result: print(f"Job completed: {result}"))
```

### Jupyter Notebook Integration

```python
# Enable rich display in Jupyter
from qetl_sdk import QETLClient
from qetl_sdk.jupyter import enable_notebook_mode

enable_notebook_mode()

client = QETLClient()
job = client.submit_job("analysis.yaml")

# Display interactive progress bar and results
job.display()  # Shows progress bar, logs, and visualizations
```

## Configuration Examples

### Basic Pipeline Configuration

```yaml
# pipeline_config.yaml
pipeline_name: "Drug Discovery Analysis"
version: "1.0"

input_sources:
  - name: "drug_targets"
    type: "csv"
    path: "./data/targets.csv"

transformations:
  - component: "quantum_homology_analyzer"
    config:
      dimensions: 4
      precision: "high"
      
  - component: "williams_pebbler"
    config:
      optimization_level: 2
      
outputs:
  - name: "analysis_results"
    format: "json"
    path: "./results/"
```

### Advanced Pipeline with Dependencies

```yaml
pipeline_name: "Multi-Stage Quantum Analysis"
version: "2.0"

input_sources:
  - name: "protein_data"
    type: "pdb"
    path: "s3://data/proteins/"
    
  - name: "drug_library"
    type: "sdf"
    path: "s3://data/compounds/"

transformations:
  - component: "wave_encoder"
    name: "encode_proteins"
    config:
      encoding_type: "quantum_fourier"
      
  - component: "wave_encoder" 
    name: "encode_compounds"
    config:
      encoding_type: "molecular_orbital"
      
  - component: "quantum_homology_analyzer"
    dependencies: ["encode_proteins", "encode_compounds"]
    config:
      binding_affinity_threshold: 0.8
      
  - component: "holographic_grover"
    dependencies: ["quantum_homology_analyzer"]
    config:
      search_iterations: 1000

outputs:
  - name: "binding_predictions"
    format: "csv"
    path: "s3://results/predictions.csv"
    
  - name: "visualization_data"
    format: "json" 
    path: "s3://results/viz_data.json"
```

## API Reference

### QETLClient

Main client class for interacting with QETL services.

```python
class QETLClient:
    def __init__(
        self,
        mode: str = "local",  # "local" or "cloud"
        instance_id: str = None,  # Required for cloud mode
        api_key: str = None,  # Required for cloud mode
        base_url: str = None,  # Optional custom endpoint
        timeout: int = 300,  # Request timeout in seconds
    )
    
    def submit_job(self, yaml_file: str) -> Job:
        """Submit job from YAML file"""
        
    def create_job(self) -> JobBuilder:
        """Create job programmatically"""
        
    def list_jobs(self, status: str = None) -> List[Job]:
        """List jobs with optional status filter"""
        
    def get_job(self, job_id: str) -> Job:
        """Get specific job by ID"""
        
    def list_components(self) -> List[Component]:
        """List available processing components"""
```

### Job Management

```python
class Job:
    @property
    def id(self) -> str:
        """Unique job identifier"""
        
    @property 
    def status(self) -> JobStatus:
        """Current job status"""
        
    def get_status(self) -> JobStatus:
        """Refresh and return current status"""
        
    def get_results(self) -> JobResults:
        """Get job results (blocks until complete)"""
        
    def get_results_when_done(self) -> JobResults:
        """Async version of get_results"""
        
    def cancel(self) -> bool:
        """Cancel running job"""
        
    def on_completion(self, callback: Callable):
        """Register completion callback"""
        
    def display(self):
        """Display job in Jupyter notebook"""
```

### Programmatic Job Building

```python
class JobBuilder:
    def add_input_source(
        self, 
        name: str, 
        path: str, 
        source_type: str = "auto",
        config: dict = None
    ) -> JobBuilder:
        """Add input data source"""
        
    def add_transformation(
        self,
        component: str,
        config: dict = None,
        name: str = None,
        dependencies: List[str] = None
    ) -> JobBuilder:
        """Add processing transformation"""
        
    def add_output(
        self,
        name: str,
        path: str,
        format: str = "json",
        config: dict = None  
    ) -> JobBuilder:
        """Add output configuration"""
        
    def set_execution_params(
        self,
        priority: int = 50,
        timeout: int = 3600,
        notifications: List[str] = None
    ) -> JobBuilder:
        """Set job execution parameters"""
        
    def submit(self) -> Job:
        """Submit the constructed job"""
        
    def validate(self) -> ValidationResult:
        """Validate job configuration without submitting"""
        
    def to_yaml(self) -> str:
        """Export job configuration as YAML"""
```

## Examples

### Data Science Workflow

```python
import pandas as pd
from qetl_sdk import QETLClient

# Load and prepare data
data = pd.read_csv("drug_targets.csv")
processed_data = preprocess_data(data)

# Upload to shared storage
storage_path = upload_to_storage(processed_data)

# Create and submit QETL job
client = QETLClient(instance_id="research-east-1")
job = client.create_job() \
    .add_input_source("drug_data", storage_path) \
    .add_transformation("quantum_homology_analyzer") \
    .submit()

# Wait for results and analyze
results = job.get_results_when_done()
analysis = analyze_results(results)
```

### CI/CD Integration

```python
# ci_pipeline.py
from qetl_sdk import QETLClient
import os

def run_qetl_analysis():
    client = QETLClient(
        mode="cloud",
        api_key=os.getenv("QETL_API_KEY"),
        instance_id=os.getenv("QETL_INSTANCE_ID")
    )
    
    # Submit parameterized job
    job = client.submit_job("pipeline.yaml")
    
    # Wait with timeout
    try:
        results = job.get_results_when_done(timeout=3600)
        
        # Save results for downstream CI steps
        results.save_to_file("results.json")
        
        return True
    except TimeoutError:
        print("Job timed out")
        job.cancel()
        return False

if __name__ == "__main__":
    success = run_qetl_analysis()
    exit(0 if success else 1)
```

### Batch Processing

```python
from qetl_sdk import QETLClient
import asyncio

async def process_batch(file_list):
    client = QETLClient()
    
    # Submit multiple jobs concurrently
    jobs = []
    for file_path in file_list:
        job = client.create_job() \
            .add_input_source("data", file_path) \
            .add_transformation("quantum_analyzer") \
            .submit()
        jobs.append(job)
    
    # Wait for all jobs to complete
    results = await asyncio.gather(*[
        job.get_results_when_done() for job in jobs
    ])
    
    return results

# Usage
file_list = ["data1.csv", "data2.csv", "data3.csv"]
results = asyncio.run(process_batch(file_list))
```

## Development

### Setting up Development Environment

```bash
# Clone repository
git clone https://github.com/iconlabs/qetl-sdk.git
cd qetl-sdk

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e .[dev]

# Install pre-commit hooks
pre-commit install

# Run tests
pytest
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=qetl_sdk --cov-report=html

# Run specific test file
pytest tests/test_client.py

# Run with specific marker
pytest -m "not integration"
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Run the test suite (`pytest`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- **Documentation**: [https://docs.qetl.ai/sdk](https://docs.qetl.ai/sdk)
- **Issues**: [GitHub Issues](https://github.com/iconlabs/qetl-sdk/issues)
- **Discussions**: [GitHub Discussions](https://github.com/iconlabs/qetl-sdk/discussions)
- **Email**: support@iconlabs.ai

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for details about changes in each version.
