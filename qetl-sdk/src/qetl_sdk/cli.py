"""
QETL SDK Command Line Interface
"""

import argparse
import sys
import json
import logging
from pathlib import Path
from typing import Optional

from .client import QETLClient
from .exceptions import QETLError
from . import __version__


def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def cmd_submit(args) -> int:
    """Submit a job from YAML configuration."""
    try:
        # Initialize client
        client = QETLClient(
            mode=args.mode,
            api_key=args.api_key,
            qetl_home=args.qetl_home
        )
        
        # Submit job
        job = client.submit_job(Path(args.yaml_file))
        
        print(f"Job submitted successfully!")
        print(f"Job ID: {job.id}")
        print(f"Status: {job.status.state.value}")
        
        if args.wait:
            print("Waiting for job completion...")
            job.wait_until_complete(timeout=args.timeout)
            
            if job.is_successful():
                print("Job completed successfully!")
                
                if args.show_results:
                    results = job.get_results()
                    print(f"Results: {json.dumps(results.outputs, indent=2)}")
            else:
                print(f"Job failed: {job.status.message}")
                return 1
        
        return 0
        
    except QETLError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


def cmd_status(args) -> int:
    """Get job status."""
    try:
        client = QETLClient(
            mode=args.mode,
            api_key=args.api_key,
            qetl_home=args.qetl_home
        )
        
        job = client.get_job(args.job_id)
        status = job.status
        
        print(f"Job ID: {job.id}")
        print(f"Status: {status.state.value}")
        print(f"Progress: {status.progress}%")
        print(f"Message: {status.message}")
        print(f"Created: {status.created_at}")
        print(f"Updated: {status.updated_at}")
        
        if args.json:
            print(json.dumps(status.to_dict(), indent=2))
        
        return 0
        
    except QETLError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_results(args) -> int:
    """Get job results."""
    try:
        client = QETLClient(
            mode=args.mode,
            api_key=args.api_key,
            qetl_home=args.qetl_home
        )
        
        job = client.get_job(args.job_id)
        
        if not job.is_complete():
            print(f"Job {args.job_id} is not yet complete", file=sys.stderr)
            return 1
        
        if not job.is_successful():
            print(f"Job {args.job_id} failed: {job.status.message}", file=sys.stderr)
            return 1
        
        results = job.get_results()
        
        if args.output_file:
            results.save_to_file(args.output_file)
            print(f"Results saved to {args.output_file}")
        else:
            print(json.dumps(results.to_dict(), indent=2))
        
        return 0
        
    except QETLError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_list(args) -> int:
    """List jobs."""
    try:
        client = QETLClient(
            mode=args.mode,
            api_key=args.api_key,
            qetl_home=args.qetl_home
        )
        
        jobs = client.list_jobs(status=args.status, limit=args.limit)
        
        if args.json:
            job_data = []
            for job in jobs:
                job_data.append(job.status.to_dict())
            print(json.dumps(job_data, indent=2))
        else:
            print(f"{'Job ID':<36} {'Status':<12} {'Progress':<10} {'Created'}")
            print("-" * 80)
            
            for job in jobs:
                status = job.status
                created = status.created_at.strftime("%Y-%m-%d %H:%M") if status.created_at else "N/A"
                print(f"{job.id:<36} {status.state.value:<12} {status.progress:<10.1f} {created}")
        
        return 0
        
    except QETLError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_cancel(args) -> int:
    """Cancel a job."""
    try:
        client = QETLClient(
            mode=args.mode,
            api_key=args.api_key,
            qetl_home=args.qetl_home
        )
        
        job = client.get_job(args.job_id)
        
        if job.cancel():
            print(f"Job {args.job_id} cancelled successfully")
            return 0
        else:
            print(f"Could not cancel job {args.job_id} (may already be complete)")
            return 1
        
    except QETLError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_logs(args) -> int:
    """Get job logs."""
    try:
        client = QETLClient(
            mode=args.mode,
            api_key=args.api_key,
            qetl_home=args.qetl_home
        )
        
        job = client.get_job(args.job_id)
        logs = job.get_logs(follow=args.follow)
        
        for log_line in logs:
            print(log_line)
        
        return 0
        
    except QETLError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_validate(args) -> int:
    """Validate YAML configuration."""
    try:
        client = QETLClient(
            mode=args.mode,
            api_key=args.api_key,
            qetl_home=args.qetl_home
        )
        
        result = client.validate_yaml(Path(args.yaml_file))
        
        if result["valid"]:
            print("✓ YAML configuration is valid")
            return 0
        else:
            print("✗ YAML configuration is invalid")
            if "errors" in result:
                for error in result["errors"]:
                    print(f"  - {error}")
            return 1
        
    except QETLError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_components(args) -> int:
    """List available components."""
    try:
        client = QETLClient(
            mode=args.mode,
            api_key=args.api_key,
            qetl_home=args.qetl_home
        )
        
        components = client.list_components()
        
        if args.json:
            print(json.dumps(components, indent=2))
        else:
            print(f"{'Name':<25} {'Type':<15} {'Version':<10} {'Description'}")
            print("-" * 80)
            
            for component in components:
                name = component.get("name", "Unknown")
                comp_type = component.get("type", "Unknown")
                version = component.get("version", "Unknown")
                description = component.get("description", "No description")
                
                print(f"{name:<25} {comp_type:<15} {version:<10} {description}")
        
        return 0
        
    except QETLError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_info(args) -> int:
    """Get instance information."""
    try:
        client = QETLClient(
            mode=args.mode,
            api_key=args.api_key,
            qetl_home=args.qetl_home
        )
        
        info = client.get_instance_info()
        
        if args.json:
            print(json.dumps(info, indent=2))
        else:
            print("QETL Instance Information:")
            for key, value in info.items():
                print(f"  {key}: {value}")
        
        return 0
        
    except QETLError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def create_parser() -> argparse.ArgumentParser:
    """Create command line argument parser."""
    parser = argparse.ArgumentParser(
        description="QETL SDK Command Line Interface",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--version", 
        action="version", 
        version=f"qetl-sdk {__version__}"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        "--mode",
        choices=["local", "cloud"],
        default="local",
        help="Execution mode (default: local)"
    )
    
    parser.add_argument(
        "--api-key",
        help="API key for cloud mode"
    )
    
    parser.add_argument(
        "--qetl-home",
        help="Path to QETL installation (for local mode)"
    )
    
    # Subcommands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Submit command
    submit_parser = subparsers.add_parser("submit", help="Submit a job")
    submit_parser.add_argument("yaml_file", help="Path to YAML configuration file")
    submit_parser.add_argument("--wait", action="store_true", help="Wait for job completion")
    submit_parser.add_argument("--timeout", type=int, default=3600, help="Timeout in seconds")
    submit_parser.add_argument("--show-results", action="store_true", help="Show results when complete")
    submit_parser.set_defaults(func=cmd_submit)
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Get job status")
    status_parser.add_argument("job_id", help="Job ID")
    status_parser.add_argument("--json", action="store_true", help="Output as JSON")
    status_parser.set_defaults(func=cmd_status)
    
    # Results command
    results_parser = subparsers.add_parser("results", help="Get job results")
    results_parser.add_argument("job_id", help="Job ID")
    results_parser.add_argument("--output-file", "-o", help="Save results to file")
    results_parser.set_defaults(func=cmd_results)
    
    # List command
    list_parser = subparsers.add_parser("list", help="List jobs")
    list_parser.add_argument("--status", help="Filter by status")
    list_parser.add_argument("--limit", type=int, default=20, help="Maximum number of jobs to show")
    list_parser.add_argument("--json", action="store_true", help="Output as JSON")
    list_parser.set_defaults(func=cmd_list)
    
    # Cancel command
    cancel_parser = subparsers.add_parser("cancel", help="Cancel a job")
    cancel_parser.add_argument("job_id", help="Job ID")
    cancel_parser.set_defaults(func=cmd_cancel)
    
    # Logs command
    logs_parser = subparsers.add_parser("logs", help="Get job logs")
    logs_parser.add_argument("job_id", help="Job ID")
    logs_parser.add_argument("--follow", "-f", action="store_true", help="Follow logs in real-time")
    logs_parser.set_defaults(func=cmd_logs)
    
    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate YAML configuration")
    validate_parser.add_argument("yaml_file", help="Path to YAML configuration file")
    validate_parser.set_defaults(func=cmd_validate)
    
    # Components command
    components_parser = subparsers.add_parser("components", help="List available components")
    components_parser.add_argument("--json", action="store_true", help="Output as JSON")
    components_parser.set_defaults(func=cmd_components)
    
    # Info command
    info_parser = subparsers.add_parser("info", help="Get instance information")
    info_parser.add_argument("--json", action="store_true", help="Output as JSON")
    info_parser.set_defaults(func=cmd_info)
    
    return parser


def main() -> int:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Check if command was provided
    if not args.command:
        parser.print_help()
        return 1
    
    # Execute command
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
