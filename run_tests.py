#!/usr/bin/env python3
"""
Test Runner for Path-Aware Workflow Tracker Library

Convenient script to run different test scenarios and generate reports.
"""

import sys
import subprocess
import argparse
from pathlib import Path

def run_command(cmd, description):
    """Run a command and return the result"""
    print(f"\n🔄 {description}")
    print(f"Command: {' '.join(cmd)}")
    print("-" * 50)
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent)
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"Error running command: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Test runner for tracker library")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    parser.add_argument("--unit", action="store_true", help="Run unit tests only")
    parser.add_argument("--integration", action="store_true", help="Run integration tests only")
    parser.add_argument("--coverage", action="store_true", help="Run tests with coverage report")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--fast", action="store_true", help="Fast test run (no coverage)")
    parser.add_argument("--specific", help="Run specific test class or method")
    
    args = parser.parse_args()
    
    # Default to running all tests if no specific option given
    if not any([args.unit, args.integration, args.coverage, args.specific]):
        args.all = True
    
    success = True
    base_cmd = ["python", "-m", "pytest", "test_tracker.py"]
    
    if args.verbose:
        base_cmd.append("-v")
    
    if args.specific:
        # Run specific test
        cmd = base_cmd + [f"::{args.specific}"]
        success &= run_command(cmd, f"Running specific test: {args.specific}")
    
    elif args.unit:
        # Run unit tests (exclude integration)
        cmd = base_cmd + ["-k", "not integration"]
        success &= run_command(cmd, "Running unit tests")
    
    elif args.integration:
        # Run integration tests only
        cmd = base_cmd + ["-k", "integration"]
        success &= run_command(cmd, "Running integration tests")
    
    elif args.coverage:
        # Run with coverage
        try:
            subprocess.run(["pip", "install", "pytest-cov"], check=True, capture_output=True)
        except subprocess.CalledProcessError:
            print("⚠️  Warning: Could not install pytest-cov")
        
        cmd = base_cmd + ["--cov=tracker", "--cov-report=term-missing", "--cov-report=html"]
        success &= run_command(cmd, "Running tests with coverage report")
        
        if success:
            print("\n📊 Coverage report generated in htmlcov/index.html")
    
    elif args.fast:
        # Fast test run
        cmd = base_cmd + ["-x"]  # Stop on first failure
        success &= run_command(cmd, "Running fast test suite (stop on first failure)")
    
    else:  # args.all or default
        # Run all tests
        cmd = base_cmd
        success &= run_command(cmd, "Running complete test suite")
    
    # Summary
    print("\n" + "=" * 50)
    if success:
        print("✅ All tests completed successfully!")
        sys.exit(0)
    else:
        print("❌ Some tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
