"""CLI interface for SSIS to Airflow/dbt migration tool.

Main command-line interface according to section 7 of the migration plan.
Usage: ssis-migrate --in ./packages/MyPackage.dtsx --mode auto --out ./build
"""
import click
import logging
import sys
import json
from pathlib import Path
from typing import Optional
import os

from parser import DTSXToIRConverter
from generators import AirflowDAGGenerator, DBTProjectGenerator
from models import ir_to_json


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('ssis_migration.log')
    ]
)

logger = logging.getLogger(__name__)


@click.group()
def cli():
    """SSIS to Airflow/dbt Migration Tool."""
    pass


@cli.command()
@click.option(
    '--in', 'input_file',
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help='Input DTSX file path'
)
@click.option(
    '--mode',
    type=click.Choice(['auto', 'airflow', 'dbt'], case_sensitive=False),
    default='auto',
    help='Migration mode: auto (detect), airflow (DAG only), or dbt (project only)'
)
@click.option(
    '--out', 'output_dir',
    type=click.Path(path_type=Path),
    default='./output',
    help='Output directory for generated files'
)
@click.option(
    '--password',
    type=str,
    help='Password for encrypted DTSX packages'
)
@click.option(
    '--anthropic-key',
    type=str,
    envvar='ANTHROPIC_API_KEY',
    help='Claude API key for AI code generation'
)
@click.option(
    '--verbose', '-v',
    is_flag=True,
    help='Enable verbose logging'
)
@click.option(
    '--save-ir',
    is_flag=True,
    help='Save intermediate representation (IR) JSON file'
)
@click.option(
    '--validate-only',
    is_flag=True,
    help='Only validate DTSX file and show analysis'
)
def main(input_file: Path, mode: str, output_dir: Path, password: Optional[str],
         anthropic_key: Optional[str], verbose: bool, save_ir: bool, 
         validate_only: bool):
    """SSIS to Airflow/dbt Migration Tool.
    
    Converts SSIS .dtsx packages to Airflow DAGs and/or dbt projects.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info(f"Starting SSIS migration: {input_file}")
    logger.info(f"Mode: {mode}, Output: {output_dir}")
    
    try:
        # Parse DTSX to IR
        converter = DTSXToIRConverter(password=password)
        ir_package, parse_report = converter.convert_file(str(input_file))
        
        if not parse_report.success:
            logger.error("Failed to parse DTSX file:")
            for error in parse_report.errors:
                logger.error(f"  - {error}")
            sys.exit(1)
        
        # Display parsing results
        logger.info(f"Successfully parsed package: {ir_package.package_name}")
        logger.info(f"Found {parse_report.total_executables} tasks, {parse_report.supported_executables} supported")
        
        if parse_report.warnings:
            logger.warning("Parsing warnings:")
            for warning in parse_report.warnings:
                logger.warning(f"  - {warning}")
        
        if parse_report.manual_review_items:
            logger.info("Manual review required for:")
            for item in parse_report.manual_review_items:
                logger.info(f"  - {item}")
        
        if validate_only:
            click.echo("Validation complete - see log for details")
            sys.exit(0)
        
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save IR if requested
        if save_ir:
            ir_file = output_dir / f"{ir_package.package_name}_ir.json"
            with open(ir_file, 'w', encoding='utf-8') as f:
                f.write(ir_to_json(ir_package))
            logger.info(f"Saved IR to: {ir_file}")
        
        # Determine migration mode
        if mode == 'auto':
            if ir_package.is_transformation_only():
                mode = 'dbt'
            else:
                # Check for mixed mode
                has_transforms = len(ir_package.get_data_flows()) > 0
                if has_transforms:
                    mode = 'mixed'
                else:
                    mode = 'airflow'
        
        logger.info(f"Migration mode determined: {mode}")
        
        # Generate outputs based on mode
        generated_files = []
        
        if mode in ['airflow', 'mixed']:
            # Generate Airflow DAG
            airflow_gen = AirflowDAGGenerator(anthropic_key)
            airflow_result = airflow_gen.generate_dag(ir_package, str(output_dir / 'airflow'))
            
            if airflow_result['success']:
                logger.info(f"Generated Airflow DAG: {airflow_result['dag_file']}")
                generated_files.append(airflow_result['dag_file'])
                generated_files.extend(airflow_result['helper_files'])
            else:
                logger.error("Failed to generate Airflow DAG:")
                for error in airflow_result['errors']:
                    logger.error(f"  - {error}")
        
        if mode in ['dbt', 'mixed']:
            # Generate dbt project
            dbt_gen = DBTProjectGenerator(anthropic_key)
            dbt_result = dbt_gen.generate_project(ir_package, str(output_dir / 'dbt'))
            
            if dbt_result['success']:
                logger.info(f"Generated dbt project with {len(dbt_result['models'])} models")
                generated_files.extend(dbt_result['project_files'])
            else:
                logger.error("Failed to generate dbt project:")
                for error in dbt_result['errors']:
                    logger.error(f"  - {error}")
        
        # Generate migration report
        report_file = output_dir / f"{ir_package.package_name}_migration_report.json"
        
        migration_report = {
            "package_name": ir_package.package_name,
            "source_file": str(input_file),
            "migration_mode": mode,
            "parse_report": parse_report.model_dump(),
            "generated_files": generated_files,
            "summary": {
                "total_tasks": parse_report.total_executables,
                "supported_tasks": parse_report.supported_executables,
                "warnings": len(parse_report.warnings),
                "manual_review_items": len(parse_report.manual_review_items)
            }
        }
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(migration_report, f, indent=2)
        
        logger.info(f"Migration report saved: {report_file}")
        
        # Final summary
        click.echo("\n" + "="*50)
        click.echo("MIGRATION SUMMARY")
        click.echo("="*50)
        click.echo(f"Package: {ir_package.package_name}")
        click.echo(f"Mode: {mode}")
        click.echo(f"Generated files: {len(generated_files)}")
        
        if generated_files:
            click.echo("\nGenerated files:")
            for file_path in generated_files:
                click.echo(f"  - {file_path}")
        
        if parse_report.warnings or parse_report.manual_review_items:
            click.echo(f"\nWarnings: {len(parse_report.warnings)}")
            click.echo(f"Manual review items: {len(parse_report.manual_review_items)}")
            click.echo("See migration report for details.")
        
        # Next steps
        click.echo("\nNext Steps:")
        if mode in ['airflow', 'mixed']:
            click.echo("1. Configure Airflow connections (see generated connection script)")
            click.echo("2. Test the generated DAG in your Airflow environment")
        
        if mode in ['dbt', 'mixed']:
            click.echo("1. Set up dbt profiles.yml with your Snowflake credentials")
            click.echo("2. Run 'dbt compile' to validate the generated models")
        
        click.echo("3. Review manual review items and warnings")
        click.echo("4. Test thoroughly before production deployment")
        
        logger.info("Migration completed successfully")
        
    except KeyboardInterrupt:
        logger.info("Migration cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.argument('dtsx_file', type=click.Path(exists=True, path_type=Path))
@click.option('--password', type=str, help='Password for encrypted packages')
def analyze(dtsx_file: Path, password: Optional[str]):
    """Analyze a DTSX file and show package information."""
    logger.info(f"Analyzing DTSX file: {dtsx_file}")
    
    try:
        converter = DTSXToIRConverter(password=password)
        ir_package, report = converter.convert_file(str(dtsx_file))
        
        click.echo(f"Package: {ir_package.package_name}")
        click.echo(f"Protection Level: {ir_package.protection_level}")
        click.echo(f"Parameters: {len(ir_package.parameters)}")
        click.echo(f"Variables: {len(ir_package.variables)}")
        click.echo(f"Connections: {len(ir_package.connection_managers)}")
        click.echo(f"Tasks: {len(ir_package.executables)}")
        click.echo(f"Precedence Constraints: {len(ir_package.edges)}")
        
        if ir_package.executables:
            click.echo("\nTask Types:")
            task_types = {}
            for exe in ir_package.executables:
                task_type = str(exe.type)
                task_types[task_type] = task_types.get(task_type, 0) + 1
            
            for task_type, count in task_types.items():
                click.echo(f"  {task_type}: {count}")
        
        click.echo(f"\nTransformation-only: {ir_package.is_transformation_only()}")
        
        if report.warnings:
            click.echo("\nWarnings:")
            for warning in report.warnings:
                click.echo(f"  - {warning}")
    
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        sys.exit(1)


@cli.command()
@click.option('--output', '-o', type=click.Path(path_type=Path), default='./example')
def create_example(output: Path):
    """Create example DTSX files and test the migration."""
    click.echo(f"Creating example files in: {output}")
    output.mkdir(parents=True, exist_ok=True)
    
    # Create a simple example DTSX (minimal XML structure for testing)
    example_dtsx = '''<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts">
  <DTS:Property DTS:Name="PackageFormatVersion">8</DTS:Property>
  <DTS:Property DTS:Name="VersionBuild">1</DTS:Property>
  <DTS:Property DTS:Name="VersionGUID">{12345678-1234-1234-1234-123456789012}</DTS:Property>
  <DTS:Property DTS:Name="PackageType">5</DTS:Property>
  <DTS:Property DTS:Name="ProtectionLevel">1</DTS:Property>
  <DTS:Property DTS:Name="MaxConcurrentExecutables">-1</DTS:Property>
  <DTS:Property DTS:Name="ObjectName">ExamplePackage</DTS:Property>
  <DTS:Property DTS:Name="DTSID">{12345678-1234-1234-1234-123456789012}</DTS:Property>
  <DTS:Property DTS:Name="Description"></DTS:Property>
  <DTS:Property DTS:Name="CreationName">Microsoft.Package</DTS:Property>
</DTS:Executable>'''
    
    example_file = output / "example_package.dtsx"
    with open(example_file, 'w', encoding='utf-8') as f:
        f.write(example_dtsx)
    
    click.echo(f"Created example DTSX: {example_file}")
    click.echo("Run: ssis-migrate --in example_package.dtsx --out ./output")


if __name__ == '__main__':
    cli()  # Use the group instead of main directly