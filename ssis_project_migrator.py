#!/usr/bin/env python3
"""SSIS Project Migrator CLI - Batch conversion tool for entire SSIS projects.

This is the main CLI entry point for converting complete SSIS projects (.ispac files 
or project directories) to Airflow DAGs and dbt projects with proper orchestration.

Usage:
    ssis-project-migrate --project path/to/project.ispac --out ./build
    ssis-project-migrate --project path/to/project-folder --out ./build --mode mixed
"""
import logging
import sys
import click
from pathlib import Path
from typing import Optional, Dict, Any
import json
from datetime import datetime

# Import our project modules
import sys
sys.path.append('.')

from parser.project_to_ir import convert_project_to_ir
from generators.master_dag_gen import MasterDAGGenerator
from generators.project_dbt_gen import ProjectDBTGenerator
from generators.airflow_gen import AirflowDAGGenerator
from models.ir import IRProject, ProjectMigrationReport, MigrationReport


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('ssis_project_migration.log')
    ]
)

logger = logging.getLogger(__name__)


@click.group()
def cli():
    """SSIS Project Migration Tool - Convert entire SSIS projects to Airflow/dbt."""
    pass


@cli.command()
@click.option(
    '--project', 'project_path',
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help='Path to SSIS project (.ispac file or project directory)'
)
@click.option(
    '--out', 'output_dir',
    required=True,
    type=click.Path(path_type=Path),
    help='Output directory for generated files'
)
@click.option(
    '--mode',
    type=click.Choice(['auto', 'airflow', 'dbt', 'mixed'], case_sensitive=False),
    default='auto',
    help='Migration mode: auto (detect), airflow (DAGs only), dbt (project only), mixed (both)'
)
@click.option(
    '--anthropic-key',
    help='Claude API key for AI-assisted generation (or set ANTHROPIC_API_KEY env var)'
)
@click.option(
    '--password',
    help='Password for encrypted SSIS packages'
)
@click.option(
    '--save-ir',
    is_flag=True,
    help='Save intermediate representation files for debugging'
)
@click.option(
    '--verbose', '-v',
    is_flag=True,
    help='Enable verbose logging'
)
def migrate(
    project_path: Path,
    output_dir: Path,
    mode: str,
    anthropic_key: Optional[str],
    password: Optional[str],
    save_ir: bool,
    verbose: bool
):
    """Migrate SSIS project to Airflow DAGs and/or dbt projects.
    
    This command processes an entire SSIS project, analyzes cross-package dependencies,
    and generates appropriate Airflow DAGs and dbt projects based on the migration strategy.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info(f"Starting SSIS project migration: {project_path}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Migration mode: {mode}")
    
    try:
        # Step 1: Convert project to IR
        logger.info("Step 1: Parsing SSIS project to intermediate representation...")
        ir_project = convert_project_to_ir(
            project_path=project_path,
            password=password,
            anthropic_key=anthropic_key,
            save_ir=save_ir
        )
        
        logger.info(f"Parsed project: {ir_project.project_name} with {len(ir_project.packages)} packages")
        
        # Step 2: Determine migration strategy
        logger.info("Step 2: Analyzing project structure and determining migration strategy...")
        strategy = ir_project.recommend_migration_strategy()
        
        if mode == 'auto':
            chosen_mode = strategy['strategy']
        else:
            chosen_mode = mode
            
        logger.info(f"Migration strategy: {chosen_mode}")
        logger.info(f"Strategy rationale: {', '.join(strategy['rationale'])}")
        
        # Step 3: Create output directories
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Step 4: Generate based on chosen strategy
        generation_results = {}
        
        if chosen_mode in ['airflow', 'airflow_only', 'mixed']:
            logger.info("Step 4a: Generating Airflow DAGs...")
            airflow_results = _generate_airflow_artifacts(
                ir_project, 
                output_dir / 'airflow', 
                anthropic_key
            )
            generation_results['airflow'] = airflow_results
        
        if chosen_mode in ['dbt', 'dbt_only', 'dbt_with_orchestration', 'mixed']:
            logger.info("Step 4b: Generating dbt project...")
            dbt_results = _generate_dbt_artifacts(
                ir_project,
                output_dir / 'dbt',
                anthropic_key
            )
            generation_results['dbt'] = dbt_results
        
        # Step 5: Generate comprehensive migration report
        logger.info("Step 5: Generating migration report...")
        migration_report = _generate_migration_report(
            ir_project,
            strategy,
            generation_results,
            project_path,
            output_dir
        )
        
        # Save migration report
        report_file = output_dir / f"{ir_project.project_name}_migration_report.json"
        report_file.write_text(migration_report.model_dump_json(indent=2), encoding='utf-8')
        
        # Generate summary
        _generate_migration_summary(migration_report, output_dir)
        
        logger.info("✅ SSIS project migration completed successfully!")
        logger.info(f"📁 Output files: {output_dir}")
        logger.info(f"📊 Migration report: {report_file}")
        
        # Print summary to console
        _print_migration_summary(migration_report)
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        sys.exit(1)


@cli.command()
@click.argument('project_path', type=click.Path(exists=True, path_type=Path))
@click.option(
    '--password',
    help='Password for encrypted SSIS packages'
)
@click.option(
    '--verbose', '-v',
    is_flag=True,
    help='Enable verbose logging'
)
def analyze(project_path: Path, password: Optional[str], verbose: bool):
    """Analyze SSIS project structure and dependencies without generating code.
    
    This command provides detailed analysis of the project structure, package dependencies,
    and migration recommendations without actually generating any code.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info(f"Analyzing SSIS project: {project_path}")
    
    try:
        # Parse project to IR
        ir_project = convert_project_to_ir(
            project_path=project_path,
            password=password,
            save_ir=False
        )
        
        # Generate analysis report
        analysis = _generate_project_analysis(ir_project)
        
        # Print analysis to console
        _print_project_analysis(analysis)
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        sys.exit(1)


def _generate_airflow_artifacts(ir_project: IRProject, output_dir: Path, anthropic_key: Optional[str]) -> Dict[str, Any]:
    """Generate Airflow DAGs and supporting files."""
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = {
        'generated_files': [],
        'package_dags': {},
        'master_dag': None,
        'warnings': [],
        'errors': []
    }
    
    try:
        # Generate master DAG and coordinated package DAGs
        master_generator = MasterDAGGenerator(anthropic_key=anthropic_key)
        generated_files = master_generator.generate_master_dag(ir_project, output_dir)
        
        results['generated_files'] = list(generated_files.values())
        results['master_dag'] = generated_files.get('master')
        
        # Track package DAGs
        for pkg_name in ir_project.package_irs.keys():
            if pkg_name in generated_files:
                results['package_dags'][pkg_name] = generated_files[pkg_name]
        
        logger.info(f"Generated {len(generated_files)} Airflow files")
        
    except Exception as e:
        logger.error(f"Failed to generate Airflow artifacts: {e}")
        results['errors'].append(str(e))
    
    return results


def _generate_dbt_artifacts(ir_project: IRProject, output_dir: Path, anthropic_key: Optional[str]) -> Dict[str, Any]:
    """Generate dbt project files."""
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = {
        'generated_files': [],
        'models': {},
        'dependencies': [],
        'warnings': [],
        'errors': []
    }
    
    try:
        # Generate unified dbt project
        dbt_generator = ProjectDBTGenerator(anthropic_api_key=anthropic_key)
        dbt_results = dbt_generator.generate_project_dbt(ir_project, output_dir)
        
        results.update(dbt_results)
        
        logger.info(f"Generated dbt project with {sum(len(models.get('staging', [])) + len(models.get('intermediate', [])) + len(models.get('marts', [])) for models in results['models'].values())} models")
        
    except Exception as e:
        logger.error(f"Failed to generate dbt artifacts: {e}")
        results['errors'].append(str(e))
    
    return results


def _generate_migration_report(
    ir_project: IRProject,
    strategy: Dict[str, Any],
    generation_results: Dict[str, Any],
    source_path: Path,
    output_dir: Path
) -> ProjectMigrationReport:
    """Generate comprehensive migration report."""
    
    # Collect all generated artifacts
    generated_artifacts = {}
    for component, results in generation_results.items():
        if 'generated_files' in results:
            generated_artifacts[component] = results['generated_files']
    
    # Collect package-level reports
    package_reports = {}
    for pkg_name, pkg_ir in ir_project.package_irs.items():
        package_reports[pkg_name] = MigrationReport(
            package_name=pkg_name,
            source_file=f"{pkg_name}.dtsx",
            migration_mode=strategy['strategy'],
            total_executables=len(pkg_ir.executables),
            supported_executables=len([exe for exe in pkg_ir.executables if exe.type.value != "Unknown"]),
            warnings=[],
            errors=[]
        )
    
    # Collect warnings and errors
    all_warnings = []
    all_errors = []
    for results in generation_results.values():
        all_warnings.extend(results.get('warnings', []))
        all_errors.extend(results.get('errors', []))
    
    return ProjectMigrationReport(
        project_name=ir_project.project_name,
        project_version=ir_project.project_version,
        source_path=str(source_path),
        total_packages=len(ir_project.packages),
        transformation_packages=ir_project.get_transformation_packages(),
        orchestration_packages=ir_project.get_orchestration_packages(),
        dependencies_count=len(ir_project.dependencies),
        migration_strategy=strategy,
        package_reports=package_reports,
        generated_artifacts=generated_artifacts,
        warnings=all_warnings,
        errors=all_errors,
        success=len(all_errors) == 0
    )


def _generate_migration_summary(report: ProjectMigrationReport, output_dir: Path):
    """Generate human-readable migration summary."""
    
    summary = f"""# {report.project_name} Migration Summary

## Project Overview
- **Source**: {report.source_path}
- **Total Packages**: {report.total_packages}
- **Migration Strategy**: {report.migration_strategy['strategy']}
- **Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Package Analysis
- **Transformation Packages**: {len(report.transformation_packages)} ({', '.join(report.transformation_packages)})
- **Orchestration Packages**: {len(report.orchestration_packages)} ({', '.join(report.orchestration_packages)})
- **Cross-package Dependencies**: {report.dependencies_count}

## Generated Artifacts
{_format_generated_artifacts(report.generated_artifacts)}

## Migration Strategy Rationale
{chr(10).join(f"- {reason}" for reason in report.migration_strategy.get('rationale', []))}

## Next Steps
1. **Review Generated Code**: Examine all generated DAGs and dbt models
2. **Test Connections**: Set up and test database connections
3. **Data Validation**: Verify data transformations produce expected results
4. **Performance Optimization**: Review and optimize queries and materializations
5. **Deployment**: Deploy to your Airflow and dbt environments

## Warnings
{chr(10).join(f"⚠️ {warning}" for warning in report.warnings) if report.warnings else "None"}

## Errors
{chr(10).join(f"❌ {error}" for error in report.errors) if report.errors else "None"}

---
*Generated by SSIS Project Migration Tool*
"""
    
    (output_dir / "MIGRATION_SUMMARY.md").write_text(summary, encoding='utf-8')


def _format_generated_artifacts(artifacts: Dict[str, Any]) -> str:
    """Format generated artifacts for display."""
    
    lines = []
    for component, files in artifacts.items():
        lines.append(f"### {component.title()}")
        for file_path in files:
            lines.append(f"- `{file_path}`")
        lines.append("")
    
    return "\\n".join(lines) if lines else "No artifacts generated"


def _generate_project_analysis(ir_project: IRProject) -> Dict[str, Any]:
    """Generate detailed project analysis."""
    
    analysis = {
        'project_info': {
            'name': ir_project.project_name,
            'version': ir_project.project_version,
            'total_packages': len(ir_project.packages),
            'protection_level': ir_project.protection_level
        },
        'package_breakdown': {},
        'dependencies': {
            'cross_package_count': len(ir_project.dependencies),
            'dependency_chains': ir_project.execution_chains,
            'entry_points': ir_project.entry_points,
            'isolated_packages': ir_project.isolated_packages
        },
        'migration_strategy': ir_project.recommend_migration_strategy(),
        'complexity_metrics': _calculate_complexity_metrics(ir_project)
    }
    
    # Analyze each package
    for pkg_name, pkg_ir in ir_project.package_irs.items():
        analysis['package_breakdown'][pkg_name] = {
            'total_executables': len(pkg_ir.executables),
            'data_flows': len(pkg_ir.get_data_flows()),
            'sql_tasks': len(pkg_ir.get_sql_tasks()),
            'transformation_only': pkg_ir.is_transformation_only(),
            'connections': len(pkg_ir.connection_managers),
            'variables': len(pkg_ir.variables),
            'parameters': len(pkg_ir.parameters)
        }
    
    return analysis


def _calculate_complexity_metrics(ir_project: IRProject) -> Dict[str, int]:
    """Calculate project complexity metrics."""
    
    total_executables = sum(len(pkg_ir.executables) for pkg_ir in ir_project.package_irs.values())
    total_data_flows = sum(len(pkg_ir.get_data_flows()) for pkg_ir in ir_project.package_irs.values())
    total_connections = len(ir_project.project_connections) + sum(len(pkg_ir.connection_managers) for pkg_ir in ir_project.package_irs.values())
    
    return {
        'total_executables': total_executables,
        'total_data_flows': total_data_flows,
        'total_connections': total_connections,
        'dependency_complexity': len(ir_project.dependencies),
        'estimated_migration_days': _estimate_migration_effort(ir_project)
    }


def _estimate_migration_effort(ir_project: IRProject) -> int:
    """Estimate migration effort in person-days."""
    
    base_days = 1  # Base setup
    package_days = len(ir_project.packages) * 2  # 2 days per package
    dependency_days = len(ir_project.dependencies) * 1  # 1 day per dependency
    
    # Add complexity factors
    complexity_factor = 1
    if len(ir_project.packages) > 10:
        complexity_factor *= 1.5
    if len(ir_project.dependencies) > 5:
        complexity_factor *= 1.3
    
    total_executables = sum(len(pkg_ir.executables) for pkg_ir in ir_project.package_irs.values())
    if total_executables > 50:
        complexity_factor *= 1.4
    
    return max(1, int((base_days + package_days + dependency_days) * complexity_factor))


def _print_project_analysis(analysis: Dict[str, Any]):
    """Print project analysis to console."""
    
    info = analysis['project_info']
    deps = analysis['dependencies']
    strategy = analysis['migration_strategy']
    metrics = analysis['complexity_metrics']
    
    click.echo(f"\\n🏗️  SSIS Project Analysis: {info['name']}")
    click.echo("=" * 60)
    
    click.echo(f"📦 Total Packages: {info['total_packages']}")
    click.echo(f"🔐 Protection Level: {info['protection_level']}")
    click.echo(f"🔗 Cross-package Dependencies: {deps['cross_package_count']}")
    click.echo(f"🚀 Entry Points: {len(deps['entry_points'])}")
    
    if deps['entry_points']:
        click.echo(f"   Entry packages: {', '.join(deps['entry_points'])}")
    
    click.echo(f"\\n📊 Complexity Metrics:")
    click.echo(f"   Total Executables: {metrics['total_executables']}")
    click.echo(f"   Total Data Flows: {metrics['total_data_flows']}")
    click.echo(f"   Total Connections: {metrics['total_connections']}")
    click.echo(f"   Estimated Migration Effort: {metrics['estimated_migration_days']} person-days")
    
    click.echo(f"\\n🎯 Recommended Migration Strategy: {strategy['strategy']}")
    click.echo(f"   Components: {', '.join(strategy['components'])}")
    for rationale in strategy['rationale']:
        click.echo(f"   - {rationale}")
    
    click.echo(f"\\n📋 Package Breakdown:")
    for pkg_name, pkg_info in analysis['package_breakdown'].items():
        transform_only = "✅" if pkg_info['transformation_only'] else "❌"
        click.echo(f"   {pkg_name}: {pkg_info['total_executables']} tasks, Transform-only: {transform_only}")


def _print_migration_summary(report: ProjectMigrationReport):
    """Print migration summary to console."""
    
    click.echo(f"\\n✅ Migration Completed: {report.project_name}")
    click.echo("=" * 60)
    
    click.echo(f"📦 Processed: {report.total_packages} packages")
    click.echo(f"🎯 Strategy: {report.migration_strategy['strategy']}")
    
    if report.generated_artifacts:
        click.echo(f"\\n📁 Generated Files:")
        for component, files in report.generated_artifacts.items():
            click.echo(f"   {component}: {len(files)} files")
    
    if report.warnings:
        click.echo(f"\\n⚠️  Warnings: {len(report.warnings)}")
        for warning in report.warnings[:3]:  # Show first 3
            click.echo(f"   - {warning}")
        if len(report.warnings) > 3:
            click.echo(f"   ... and {len(report.warnings) - 3} more")
    
    if report.errors:
        click.echo(f"\\n❌ Errors: {len(report.errors)}")
        for error in report.errors:
            click.echo(f"   - {error}")
    
    click.echo(f"\\n📄 See MIGRATION_SUMMARY.md for detailed information")


if __name__ == '__main__':
    cli()