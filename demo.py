#!/usr/bin/env python3
"""
Generate sample DTSX files and run example migrations.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from tests.samples.sample_dtsx import write_sample_files, SIMPLE_PACKAGE
from parser import DTSXToIRConverter
from generators import AirflowDAGGenerator, DBTProjectGenerator
import tempfile
import json


def main():
    """Generate samples and demonstrate the migration tool."""
    print("🚀 SSIS to Airflow/dbt Migration Tool Demo")
    print("=" * 50)
    
    # Create output directories
    output_dir = Path("./demo_output")
    samples_dir = output_dir / "samples"
    
    output_dir.mkdir(exist_ok=True)
    samples_dir.mkdir(exist_ok=True)
    
    # Generate sample DTSX files
    print("\n📄 Generating sample DTSX files...")
    sample_files = write_sample_files(str(samples_dir))
    
    for file_path in sample_files:
        print(f"  ✅ Created: {file_path}")
    
    # Test migration with simple package
    print("\n🔄 Testing migration with simple package...")
    simple_dtsx = samples_dir / "simple_etl_package.dtsx"
    
    try:
        # Parse DTSX to IR
        print("  📋 Parsing DTSX file...")
        converter = DTSXToIRConverter()
        ir_package, report = converter.convert_file(str(simple_dtsx))
        
        print(f"  ✅ Parsed package: {ir_package.package_name}")
        print(f"  📊 Found {len(ir_package.executables)} tasks, {len(ir_package.connection_managers)} connections")
        
        # Save IR for inspection
        ir_file = output_dir / "simple_package_ir.json"
        with open(ir_file, 'w') as f:
            json.dump(ir_package.model_dump(), f, indent=2)
        print(f"  💾 Saved IR: {ir_file}")
        
        # Generate Airflow DAG
        print("  ⚡ Generating Airflow DAG...")
        airflow_gen = AirflowDAGGenerator()
        airflow_result = airflow_gen.generate_dag(ir_package, str(output_dir / "airflow"))
        
        if airflow_result["success"]:
            print(f"  ✅ Generated Airflow DAG: {airflow_result['dag_file']}")
        else:
            print(f"  ❌ Airflow generation failed: {airflow_result['errors']}")
        
        # Generate dbt project
        print("  📊 Generating dbt project...")
        dbt_gen = DBTProjectGenerator()
        dbt_result = dbt_gen.generate_project(ir_package, str(output_dir / "dbt"))
        
        if dbt_result["success"]:
            print(f"  ✅ Generated dbt project with {len(dbt_result['models'])} models")
        else:
            print(f"  ❌ dbt generation failed: {dbt_result['errors']}")
        
        # Generate migration report
        print("  📋 Generating migration report...")
        report_data = {
            "package_name": ir_package.package_name,
            "migration_mode": "mixed",
            "airflow_result": airflow_result,
            "dbt_result": dbt_result,
            "parse_report": report.model_dump()
        }
        
        report_file = output_dir / "migration_report.json"
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2)
        print(f"  💾 Saved report: {report_file}")
        
    except Exception as e:
        print(f"  ❌ Migration failed: {e}")
        return 1
    
    # Summary
    print("\n📋 Demo Summary")
    print("=" * 20)
    print(f"Sample files: {len(sample_files)}")
    print(f"IR package tasks: {len(ir_package.executables)}")
    print(f"Airflow success: {'✅' if airflow_result['success'] else '❌'}")
    print(f"dbt success: {'✅' if dbt_result['success'] else '❌'}")
    
    print(f"\n📁 Output files in: {output_dir.absolute()}")
    print("\n🎯 Next Steps:")
    print("1. Examine generated files in the demo_output directory")
    print("2. Review the IR JSON to understand the intermediate format")
    print("3. Check the Airflow DAG for task mappings")
    print("4. Look at dbt models for transformation logic")
    print("5. Run the CLI tool with: python -m cli.main --help")
    
    print("\n✨ Demo completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())