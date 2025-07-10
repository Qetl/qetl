"""
Basic Usage Example for QETL SDK

This example demonstrates how to use the QETL SDK to:
1. Create a client
2. Build a job programmatically
3. Submit and monitor the job
4. Retrieve results
"""

import os
from pathlib import Path
import tempfile
import yaml

# Import QETL SDK components
from qetl_sdk import QETLClient, JobBuilder


def create_sample_yaml() -> Path:
    """Create a sample YAML configuration for testing."""
    config = {
        "pipeline_name": "Molecular Analysis Pipeline",
        "version": "1.0",
        "description": "Sample quantum molecular analysis pipeline",
        "input_sources": [
            {
                "name": "molecular_data",
                "path": "data/molecules.sdf",
                "type": "sdf"
            }
        ],
        "transformations": [
            {
                "component": "wave_encoder",
                "name": "encode_molecular_orbitals",
                "config": {
                    "encoding_type": "molecular_orbital",
                    "precision": "high"
                }
            },
            {
                "component": "quantum_homology_analyzer",
                "name": "analyze_topology",
                "config": {
                    "dimensions": 4,
                    "precision": "high"
                },
                "dependencies": ["encode_molecular_orbitals"]
            },
            {
                "component": "williams_pebbler",
                "name": "optimize_structure",
                "config": {
                    "optimization_level": 2
                },
                "dependencies": ["analyze_topology"]
            },
            {
                "component": "holographic_grover",
                "name": "search_patterns",
                "config": {
                    "search_iterations": 1000
                },
                "dependencies": ["optimize_structure"]
            },
            {
                "component": "wave_decoder",
                "name": "decode_results",
                "config": {
                    "decoding_type": "quantum_fourier"
                },
                "dependencies": ["search_patterns"]
            }
        ],
        "outputs": [
            {
                "name": "analysis_results",
                "path": "results/molecular_analysis.json",
                "format": "json"
            },
            {
                "name": "visualization_data",
                "path": "results/visualization.csv",
                "format": "csv"
            }
        ]
    }
    
    # Create temporary YAML file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config, f, default_flow_style=False)
        return Path(f.name)


def example_yaml_submission():
    """Example: Submit job from YAML file."""
    print("=== Example 1: YAML Job Submission ===")
    
    try:
        # Create QETL client (local mode by default)
        client = QETLClient(mode="local")
        
        print(f"Client initialized in {client.mode} mode")
        
        # Get instance information
        info = client.get_instance_info()
        print(f"Instance info: {info}")
        
        # Create sample YAML configuration
        yaml_path = create_sample_yaml()
        print(f"Created sample YAML: {yaml_path}")
        
        try:
            # Validate YAML before submission
            print("Validating YAML configuration...")
            validation = client.validate_yaml(yaml_path)
            print(f"Validation result: {validation['message']}")
            
            # Submit job
            print("Submitting job...")
            job = client.submit_job(yaml_path)
            print(f"Job submitted! ID: {job.id}")
            print(f"Initial status: {job.status.state.value}")
            
            # Monitor job progress
            print("Monitoring job progress...")
            while not job.is_complete():
                status = job.get_status()
                print(f"Status: {status.state.value} ({status.progress}%) - {status.message}")
                
                if job.is_complete():
                    break
                
                # In a real scenario, you'd add a delay here
                # time.sleep(5)
                break  # For demo purposes, exit after first check
            
            # Get results if job completed successfully
            if job.is_successful():
                print("Job completed successfully!")
                results = job.get_results()
                print(f"Results: {len(results.outputs)} outputs available")
                print(f"Execution time: {results.execution_time} seconds")
            else:
                print(f"Job failed: {job.status.message}")
        
        finally:
            # Clean up temporary file
            if yaml_path.exists():
                yaml_path.unlink()
    
    except Exception as e:
        print(f"Error: {e}")


def example_programmatic_job():
    """Example: Build and submit job programmatically."""
    print("\n=== Example 2: Programmatic Job Building ===")
    
    try:
        # Create QETL client
        client = QETLClient(mode="local")
        
        # Create job builder
        builder = client.create_job_builder()
        
        # Build job using fluent interface
        job = (builder
               .set_name("Protein Structure Analysis")
               .set_version("1.2")
               .set_description("Analyze protein structure using quantum methods")
               .add_input_source("protein_data", "data/protein.pdb", "pdb")
               .add_wave_encoder(encoding_type="molecular_orbital", precision="high")
               .add_quantum_homology_analyzer(dimensions=6, precision="high", name="topology_analysis")
               .add_williams_pebbler(optimization_level=3, name="structure_optimization")
               .add_holographic_grover(search_iterations=2000, dependencies=["structure_optimization"])
               .add_wave_decoder(decoding_type="quantum_fourier")
               .add_output("structure_analysis", "results/protein_analysis.json", "json")
               .add_output("optimization_metrics", "results/metrics.csv", "csv")
               .set_execution_params(priority=80, timeout=7200)
               .submit())
        
        print(f"Job built and submitted! ID: {job.id}")
        print(f"Status: {job.status.state.value}")
        
        # Show generated configuration
        print("\nGenerated YAML configuration:")
        yaml_config = builder.to_yaml()
        print(yaml_config)
    
    except Exception as e:
        print(f"Error: {e}")


def example_job_management():
    """Example: Job listing and management."""
    print("\n=== Example 3: Job Management ===")
    
    try:
        client = QETLClient(mode="local")
        
        # List all jobs
        print("Listing all jobs...")
        jobs = client.list_jobs(limit=10)
        
        if jobs:
            print(f"Found {len(jobs)} jobs:")
            for job in jobs:
                status = job.status
                print(f"  - {job.id}: {status.state.value} ({status.progress}%)")
        else:
            print("No jobs found")
        
        # List components
        print("\nAvailable components:")
        components = client.list_components()
        for component in components:
            print(f"  - {component['name']} ({component['type']}): {component['description']}")
    
    except Exception as e:
        print(f"Error: {e}")


def example_error_handling():
    """Example: Error handling."""
    print("\n=== Example 4: Error Handling ===")
    
    try:
        client = QETLClient(mode="local")
        
        # Try to submit non-existent YAML file
        try:
            client.submit_job(Path("nonexistent.yaml"))
        except Exception as e:
            print(f"Expected error caught: {e}")
        
        # Try to get non-existent job
        try:
            client.get_job("nonexistent-job-id")
        except Exception as e:
            print(f"Expected error caught: {e}")
        
        # Try invalid job builder configuration
        try:
            builder = client.create_job_builder()
            # Missing required fields
            builder.validate()
        except Exception as e:
            print(f"Expected validation error: {e}")
    
    except Exception as e:
        print(f"Unexpected error: {e}")


def main():
    """Run all examples."""
    print("QETL SDK Usage Examples")
    print("=" * 50)
    
    # Check if we're in local mode and QETL is available
    try:
        # Example 1: YAML submission
        example_yaml_submission()
        
        # Example 2: Programmatic job building
        example_programmatic_job()
        
        # Example 3: Job management
        example_job_management()
        
        # Example 4: Error handling
        example_error_handling()
        
    except Exception as e:
        print(f"\nNote: Some examples may not work without a complete QETL installation.")
        print(f"Error: {e}")
        print("\nTo use the SDK with a real QETL installation:")
        print("1. Set QETL_HOME environment variable")
        print("2. Ensure QETL dependencies are installed")
        print("3. For cloud mode, provide a valid API key")


if __name__ == "__main__":
    main()
