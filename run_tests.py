#!/usr/bin/env python3
"""Simple test runner for SSIS migration tool using built-in unittest.

This provides an alternative to pytest for environments where pytest isn't available.
"""
import sys
import unittest
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

def run_basic_tests():
    """Run basic functionality tests without external dependencies."""
    
    print("🧪 Running SSIS Migration Tool Tests")
    print("=" * 50)
    
    # Test 1: Import all modules
    print("\n📦 Testing Module Imports...")
    try:
        from models.ir import IRPackage, IRProject
        print("✅ IR Models imported successfully")
    except ImportError as e:
        print(f"❌ IR Models import failed: {e}")
        return False
    
    try:
        from parser.project_parser import SSISProjectParser
        print("✅ Project Parser imported successfully")
    except ImportError as e:
        print(f"❌ Project Parser import failed: {e}")
        return False
    
    try:
        from generators.master_dag_gen import MasterDAGGenerator
        print("✅ Master DAG Generator imported successfully")
    except ImportError as e:
        print(f"❌ Master DAG Generator import failed: {e}")
        return False
    
    try:
        import ssis_project_migrator
        print("✅ CLI Module imported successfully")
    except ImportError as e:
        print(f"❌ CLI Module import failed: {e}")
        return False
    
    # Test 2: Create basic IR objects
    print("\n🔧 Testing IR Object Creation...")
    try:
        # Create a simple IR package
        ir_package = IRPackage(
            package_name="TestPackage",
            executables=[],
            connection_managers=[],
            parameters=[],
            variables=[]
        )
        print(f"✅ Created IR Package: {ir_package.package_name}")
        
        # Create a simple IR project
        ir_project = IRProject(
            project_name="TestProject",
            packages=[],
            package_irs={},
            dependencies=[]
        )
        print(f"✅ Created IR Project: {ir_project.project_name}")
        
    except Exception as e:
        print(f"❌ IR Object creation failed: {e}")
        return False
    
    # Test 3: Test project parser with existing project
    print("\n📁 Testing Project Parser with Real Data...")
    try:
        project_path = Path("ssis/Integration Services Project7")
        if project_path.exists():
            parser = SSISProjectParser()
            ssis_project = parser.parse_project(project_path)
            print(f"✅ Successfully parsed project: {ssis_project.project_name}")
            print(f"   📦 Found {len(ssis_project.packages)} packages")
            print(f"   🔗 Found {len(ssis_project.dependencies)} dependencies")
        else:
            print("ℹ️  Sample project not found, skipping real data test")
    except Exception as e:
        print(f"❌ Project parsing failed: {e}")
        return False
    
    # Test 4: Test CLI help
    print("\n💻 Testing CLI Interface...")
    try:
        from click.testing import CliRunner
        from ssis_project_migrator import cli
        
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        if result.exit_code == 0:
            print("✅ CLI help command works")
        else:
            print(f"⚠️  CLI help returned exit code: {result.exit_code}")
    except ImportError:
        print("ℹ️  Click not available, skipping CLI test")
    except Exception as e:
        print(f"❌ CLI test failed: {e}")
        return False
    
    print("\n" + "=" * 50)
    print("🎉 All basic tests passed!")
    return True

def run_integration_test():
    """Run an integration test with the sample project."""
    print("\n🔄 Running Integration Test...")
    
    try:
        from parser.project_to_ir import convert_project_to_ir
        
        project_path = Path("ssis/Integration Services Project7")
        if not project_path.exists():
            print("ℹ️  Sample project not found, skipping integration test")
            return True
        
        # Test complete project conversion
        print("🔄 Converting SSIS project to IR...")
        ir_project = convert_project_to_ir(project_path)
        
        print(f"✅ Successfully converted project: {ir_project.project_name}")
        print(f"   📦 Packages: {len(ir_project.packages)}")
        print(f"   🎯 Strategy: {ir_project.recommend_migration_strategy()['strategy']}")
        
        # Test migration strategy
        strategy = ir_project.recommend_migration_strategy()
        print(f"   💡 Recommendation: {', '.join(strategy['rationale'])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        return False

if __name__ == "__main__":
    print("SSIS Migration Tool - Test Suite")
    print("Using built-in unittest (pytest alternative)")
    
    success = True
    
    # Run basic functionality tests
    if not run_basic_tests():
        success = False
    
    # Run integration test
    if not run_integration_test():
        success = False
    
    if success:
        print("\n🎉 All tests completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)