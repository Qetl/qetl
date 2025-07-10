"""
QETL SDK Demo - Test SDK without full QETL installation

This demo shows SDK functionality that doesn't require a complete QETL installation.
"""

import qetl_sdk
from qetl_sdk import JobBuilder
from qetl_sdk.exceptions import QETLError, ConfigurationError


def demo_sdk_info():
    """Demo: SDK version and basic info."""
    print("=== QETL SDK Information ===")
    print(f"Version: {qetl_sdk.get_version()}")
    print(f"Version Info: {qetl_sdk.get_version_info()}")
    print(f"Author: {qetl_sdk.__author__}")
    print(f"Description: {qetl_sdk.__description__}")
    

def demo_job_builder():
    """Demo: JobBuilder functionality without client."""
    print("\n=== JobBuilder Demo ===")
    
    try:
        # Create a job builder directly (no client needed)
        # Note: JobBuilder requires a client, but we can pass None for demo
        builder = JobBuilder(client=None)
        
        # Build a complex job configuration
        config = (builder
                 .set_name("Protein Analysis Demo")
                 .set_version("2.0")
                 .set_description("Demonstrate QETL SDK job building capabilities")
                 .add_input_source("protein_structure", "data/1abc.pdb", "pdb")
                 .add_input_source("molecular_data", "data/compounds.sdf", "sdf")
                 .add_wave_encoder(
                     name="encode_protein",
                     encoding_type="molecular_orbital", 
                     precision="high"
                 )
                 .add_quantum_homology_analyzer(
                     name="analyze_topology",
                     dimensions=6,
                     precision="high",
                     dependencies=["encode_protein"]
                 )
                 .add_williams_pebbler(
                     name="optimize_structure", 
                     optimization_level=3,
                     dependencies=["analyze_topology"]
                 )
                 .add_holographic_grover(
                     name="search_patterns",
                     search_iterations=5000,
                     dependencies=["optimize_structure"]
                 )
                 .add_wave_decoder(
                     name="decode_results",
                     decoding_type="quantum_fourier",
                     dependencies=["search_patterns"]
                 )
                 .add_output("protein_analysis", "results/analysis.json", "json")
                 .add_output("optimization_data", "results/optimization.csv", "csv")
                 .add_output("visualization", "results/structure.png", "png")
                 .set_execution_params(priority=90, timeout=3600)
                 .get_config())
        
        print("✅ Job configuration built successfully!")
        print(f"Pipeline: {config['pipeline_name']}")
        print(f"Components: {len(config['transformations'])} transformations")
        print(f"Inputs: {len(config['input_sources'])} sources")
        print(f"Outputs: {len(config['outputs'])} outputs")
        
        # Export to YAML
        yaml_config = builder.to_yaml()
        print(f"\n📄 Generated YAML ({len(yaml_config)} characters):")
        print("=" * 50)
        print(yaml_config)
        print("=" * 50)
        
    except Exception as e:
        print(f"❌ Error in JobBuilder demo: {e}")


def demo_exception_hierarchy():
    """Demo: Exception hierarchy and error handling."""
    print("\n=== Exception Hierarchy Demo ===")
    
    # Test exception hierarchy
    exceptions_to_test = [
        qetl_sdk.QETLError("Base QETL error"),
        qetl_sdk.AuthenticationError("Auth failed"),
        qetl_sdk.ValidationError("Invalid config"),
        qetl_sdk.JobExecutionError("Job failed"),
        qetl_sdk.ConfigurationError("Config missing"),
        qetl_sdk.NetworkError("Connection failed"),
    ]
    
    for exc in exceptions_to_test:
        print(f"✅ {exc.__class__.__name__}: {exc}")
        print(f"   - Is QETLError: {isinstance(exc, qetl_sdk.QETLError)}")


def demo_cloud_mode_stub():
    """Demo: Cloud mode detection (should fail gracefully)."""
    print("\n=== Cloud Mode Demo (Expected to Show Stub) ===")
    
    try:
        # Try to create cloud client (should fail with informative message)
        client = qetl_sdk.create_client(mode="cloud", api_key="test-key")
        print("❌ Unexpected: Cloud client created (should have failed)")
    except qetl_sdk.QETLError as e:
        print(f"✅ Expected cloud error: {e}")
    except Exception as e:
        print(f"⚠️  Unexpected error type: {e}")


def demo_local_mode_detection():
    """Demo: Local mode detection (should fail gracefully without installation)."""
    print("\n=== Local Mode Detection Demo ===")
    
    try:
        # Try to create local client (should fail with configuration error)
        client = qetl_sdk.create_client(mode="local")
        print("❌ Unexpected: Local client created without QETL installation")
    except qetl_sdk.ConfigurationError as e:
        print(f"✅ Expected configuration error: {e}")
        print("   This is correct behavior - QETL installation not found")
    except Exception as e:
        print(f"⚠️  Unexpected error type: {e}")


def demo_builder_validation():
    """Demo: Builder validation."""
    print("\n=== Builder Validation Demo ===")
    
    try:
        # Test validation with empty builder
        empty_builder = JobBuilder(client=None)
        empty_builder.validate()
        print("❌ Unexpected: Empty builder validated successfully")
    except qetl_sdk.ValidationError as e:
        print(f"✅ Expected validation error: {e}")
    
    try:
        # Test validation with minimal valid config
        valid_builder = JobBuilder(client=None)
        valid_builder.set_name("Test Job").add_input_source("test", "test.txt", "txt")
        valid_builder.validate()
        print("✅ Valid builder passed validation")
    except Exception as e:
        print(f"⚠️  Unexpected validation error: {e}")


def main():
    """Run all SDK demos."""
    print("QETL SDK Functionality Demo")
    print("=" * 60)
    print("This demo shows SDK functionality without requiring")
    print("a complete QETL installation.")
    print("=" * 60)
    
    try:
        demo_sdk_info()
        demo_job_builder()
        demo_exception_hierarchy()
        demo_cloud_mode_stub()
        demo_local_mode_detection()
        demo_builder_validation()
        
        print("\n" + "=" * 60)
        print("✅ SDK Demo Completed Successfully!")
        print("The QETL SDK is properly installed and functional.")
        print("To use with actual QETL jobs:")
        print("1. Install complete QETL pipeline")
        print("2. Set QETL_HOME environment variable")
        print("3. For cloud mode, obtain API credentials")
        
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
