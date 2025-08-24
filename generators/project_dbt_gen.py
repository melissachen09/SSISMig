"""Project-level dbt generator for multi-package SSIS projects.

Generates unified dbt projects from multiple transformation-only SSIS packages,
with proper model dependencies and project structure.
"""
import logging
import os
import json
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, Template
from anthropic import Anthropic

from models.ir import IRProject, IRPackage, Executable, ExecutableType, DataFlowComponent, ComponentType
from .sql_converter import SQLDialectConverter
from .dbt_gen import DBTProjectGenerator

logger = logging.getLogger(__name__)


class ProjectDBTGenerator:
    """Generator for unified dbt projects from multi-package SSIS projects."""
    
    def __init__(self, anthropic_api_key: Optional[str] = None):
        self.sql_converter = SQLDialectConverter()
        self.single_package_gen = DBTProjectGenerator(anthropic_api_key)
        self.anthropic_client = None
        
        if anthropic_api_key or os.getenv("ANTHROPIC_API_KEY"):
            self.anthropic_client = Anthropic(
                api_key=anthropic_api_key or os.getenv("ANTHROPIC_API_KEY")
            )
        
        self.logger = logging.getLogger(__name__)
    
    def generate_project_dbt(self, ir_project: IRProject, output_dir: Path) -> Dict[str, Any]:
        """Generate unified dbt project from multiple SSIS packages.
        
        Args:
            ir_project: Complete project IR with multiple packages
            output_dir: Directory to write dbt project
            
        Returns:
            Generation result with files and model information
        """
        self.logger.info(f"Generating unified dbt project for: {ir_project.project_name}")
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        result = {
            "success": False,
            "project_files": [],
            "models": {},  # Models by package
            "dependencies": [],
            "warnings": [],
            "errors": []
        }
        
        try:
            # Get transformation packages
            transform_packages = ir_project.get_transformation_packages()
            
            if not transform_packages:
                result["warnings"].append("No transformation-only packages found")
                return result
            
            self.logger.info(f"Processing {len(transform_packages)} transformation packages")
            
            # Generate unified project structure
            self._create_unified_dbt_project(ir_project, output_dir, transform_packages)
            
            # Generate models for each package
            all_models = {}
            for pkg_name in transform_packages:
                pkg_ir = ir_project.package_irs[pkg_name]
                models = self._generate_package_models(pkg_ir, output_dir, ir_project)
                all_models[pkg_name] = models
            
            # Generate cross-package dependencies
            dependencies = self._analyze_cross_package_dependencies(ir_project, transform_packages)
            
            # Create unified schema.yml
            self._generate_unified_schema(ir_project, output_dir, all_models, dependencies)
            
            # Generate project documentation
            self._generate_project_documentation(ir_project, output_dir, all_models)
            
            result.update({
                "success": True,
                "models": all_models,
                "dependencies": dependencies,
                "project_files": list(output_dir.rglob("*"))
            })
            
            self.logger.info(f"Generated unified dbt project with {sum(len(models) for models in all_models.values())} models")
            
        except Exception as e:
            self.logger.error(f"Failed to generate unified dbt project: {e}")
            result["errors"].append(str(e))
        
        return result
    
    def _create_unified_dbt_project(self, ir_project: IRProject, output_dir: Path, transform_packages: List[str]):
        """Create the unified dbt project structure."""
        
        # Create directory structure
        models_dir = output_dir / "models"
        models_dir.mkdir(exist_ok=True)
        
        # Create package-specific subdirectories
        for pkg_name in transform_packages:
            pkg_dir = models_dir / pkg_name.lower()
            pkg_dir.mkdir(exist_ok=True)
            
            # Create staging/intermediate/marts structure within each package
            (pkg_dir / "staging").mkdir(exist_ok=True)
            (pkg_dir / "intermediate").mkdir(exist_ok=True)
            (pkg_dir / "marts").mkdir(exist_ok=True)
        
        # Create shared models directory for cross-package dependencies
        (models_dir / "shared").mkdir(exist_ok=True)
        
        # Generate dbt_project.yml
        project_config = self._generate_unified_project_yml(ir_project, transform_packages)
        (output_dir / "dbt_project.yml").write_text(yaml.dump(project_config, default_flow_style=False), encoding='utf-8')
        
        # Generate profiles.yml template
        profiles_config = self._generate_profiles_yml(ir_project)
        (output_dir / "profiles.yml.template").write_text(yaml.dump(profiles_config, default_flow_style=False), encoding='utf-8')
        
        # Generate README
        readme_content = self._generate_project_readme(ir_project, transform_packages)
        (output_dir / "README.md").write_text(readme_content, encoding='utf-8')
        
        # Generate .gitignore
        gitignore_content = self._generate_gitignore()
        (output_dir / ".gitignore").write_text(gitignore_content, encoding='utf-8')
    
    def _generate_package_models(self, pkg_ir: IRPackage, output_dir: Path, ir_project: IRProject) -> Dict[str, Any]:
        """Generate dbt models for a single package."""
        
        models = {
            "staging": [],
            "intermediate": [],
            "marts": []
        }
        
        pkg_name = pkg_ir.package_name.lower()
        pkg_dir = output_dir / "models" / pkg_name
        
        # Process data flows
        for executable in pkg_ir.get_data_flows():
            model_files = self._convert_dataflow_to_dbt_models(executable, pkg_dir, ir_project)
            
            # Categorize models by complexity
            for model_file, model_info in model_files.items():
                if model_info['type'] == 'source':
                    models['staging'].append(model_info)
                elif model_info['type'] == 'transformation':
                    models['intermediate'].append(model_info)
                elif model_info['type'] == 'final':
                    models['marts'].append(model_info)
        
        # Process Execute SQL tasks
        for executable in pkg_ir.get_sql_tasks():
            model_info = self._convert_sql_task_to_dbt_model(executable, pkg_dir, ir_project)
            if model_info:
                # SQL tasks typically create final tables
                models['marts'].append(model_info)
        
        return models
    
    def _convert_dataflow_to_dbt_models(self, dataflow: Executable, pkg_dir: Path, ir_project: IRProject) -> Dict[str, Dict]:
        """Convert SSIS data flow to dbt models."""
        
        models = {}
        components = dataflow.components
        
        # Analyze data flow structure
        sources = [c for c in components if self._is_source_component(c.component_type)]
        destinations = [c for c in components if self._is_destination_component(c.component_type)]
        transformations = [c for c in components if self._is_transformation_component(c.component_type)]
        
        # Generate staging models for sources
        for source in sources:
            model_info = self._generate_staging_model(source, pkg_dir)
            models[model_info['file_path']] = model_info
        
        # Generate intermediate models for transformations
        for transform in transformations:
            model_info = self._generate_intermediate_model(transform, pkg_dir, sources)
            models[model_info['file_path']] = model_info
        
        # Generate mart models for destinations
        for dest in destinations:
            model_info = self._generate_mart_model(dest, pkg_dir, transformations)
            models[model_info['file_path']] = model_info
        
        return models
    
    def _generate_staging_model(self, source_component: DataFlowComponent, pkg_dir: Path) -> Dict[str, Any]:
        """Generate staging model for source component."""
        
        model_name = f"stg_{source_component.name.lower()}"
        model_file = pkg_dir / "staging" / f"{model_name}.sql"
        
        # Generate SQL for source
        if source_component.sql:
            sql_content = self.sql_converter.convert_tsql_to_snowflake(source_component.sql)
        else:
            sql_content = f"SELECT * FROM {{{{ source('{source_component.table or 'raw_data'}') }}}}"
        
        model_content = f"""{{{{
  config(
    materialized='view',
    tags=['staging', '{pkg_dir.name}']
  )
}}}}

-- Staging model for {source_component.name}
-- Source: {source_component.component_type}

{sql_content}
"""
        
        model_file.write_text(model_content, encoding='utf-8')
        
        return {
            'name': model_name,
            'file_path': str(model_file),
            'type': 'source',
            'component_name': source_component.name,
            'description': f"Staging model for {source_component.name}"
        }
    
    def _generate_intermediate_model(self, transform_component: DataFlowComponent, pkg_dir: Path, sources: List[DataFlowComponent]) -> Dict[str, Any]:
        """Generate intermediate model for transformation component."""
        
        model_name = f"int_{transform_component.name.lower()}"
        model_file = pkg_dir / "intermediate" / f"{model_name}.sql"
        
        # Build SQL based on transformation type
        sql_content = self._build_transformation_sql(transform_component, sources)
        
        model_content = f"""{{{{
  config(
    materialized='table',
    tags=['intermediate', '{pkg_dir.name}']
  )
}}}}

-- Intermediate transformation: {transform_component.name}
-- Type: {transform_component.component_type}

{sql_content}
"""
        
        model_file.write_text(model_content, encoding='utf-8')
        
        return {
            'name': model_name,
            'file_path': str(model_file),
            'type': 'transformation',
            'component_name': transform_component.name,
            'description': f"Intermediate transformation for {transform_component.name}"
        }
    
    def _generate_mart_model(self, dest_component: DataFlowComponent, pkg_dir: Path, transformations: List[DataFlowComponent]) -> Dict[str, Any]:
        """Generate mart model for destination component."""
        
        model_name = f"mart_{dest_component.table or dest_component.name}".lower()
        model_file = pkg_dir / "marts" / f"{model_name}.sql"
        
        # Generate final model SQL
        sql_content = self._build_destination_sql(dest_component, transformations)
        
        model_content = f"""{{{{
  config(
    materialized='table',
    tags=['marts', '{pkg_dir.name}'],
    indexes=[
      {{'columns': ['id'], 'type': 'btree'}}
    ]
  )
}}}}

-- Final mart model: {dest_component.table or dest_component.name}
-- Destination: {dest_component.component_type}

{sql_content}
"""
        
        model_file.write_text(model_content, encoding='utf-8')
        
        return {
            'name': model_name,
            'file_path': str(model_file),
            'type': 'final',
            'component_name': dest_component.name,
            'table_name': dest_component.table,
            'description': f"Final mart for {dest_component.table or dest_component.name}"
        }
    
    def _build_transformation_sql(self, transform: DataFlowComponent, sources: List[DataFlowComponent]) -> str:
        """Build SQL for transformation component."""
        
        if transform.component_type == ComponentType.DERIVED_COLUMN:
            return self._build_derived_column_sql(transform)
        elif transform.component_type == ComponentType.LOOKUP:
            return self._build_lookup_sql(transform)
        elif transform.component_type == ComponentType.CONDITIONAL_SPLIT:
            return self._build_conditional_split_sql(transform)
        elif transform.component_type == ComponentType.AGGREGATE:
            return self._build_aggregate_sql(transform)
        else:
            return f"-- TODO: Implement transformation for {transform.component_type}\\nSELECT * FROM {{{{ ref('upstream_model') }}}}"
    
    def _build_derived_column_sql(self, transform: DataFlowComponent) -> str:
        """Build SQL for derived column transformation."""
        
        if transform.expression:
            converted_expr = self.sql_converter.convert_tsql_to_snowflake(transform.expression)
            return f"""
SELECT *,
    {converted_expr} AS derived_column
FROM {{{{ ref('upstream_model') }}}}
"""
        else:
            return "SELECT * FROM {{ ref('upstream_model') }}"
    
    def _build_lookup_sql(self, transform: DataFlowComponent) -> str:
        """Build SQL for lookup transformation."""
        
        join_keys = transform.join_on or ['id']
        ref_table = transform.ref or 'lookup_table'
        
        return f"""
SELECT 
    main.*,
    lookup.* EXCLUDE ({', '.join(join_keys)})
FROM {{{{ ref('upstream_model') }}}} AS main
LEFT JOIN {{{{ ref('{ref_table}') }}}} AS lookup
    ON {' AND '.join(f'main.{key} = lookup.{key}' for key in join_keys)}
"""
    
    def _analyze_cross_package_dependencies(self, ir_project: IRProject, transform_packages: List[str]) -> List[Dict[str, Any]]:
        """Analyze dependencies between transformation packages."""
        
        dependencies = []
        
        # Check for ExecutePackageTask dependencies between transform packages
        for dep in ir_project.dependencies:
            if dep.parent_package in transform_packages and dep.child_package in transform_packages:
                dependencies.append({
                    'parent_package': dep.parent_package,
                    'child_package': dep.child_package,
                    'type': 'execution_order',
                    'description': f"Package {dep.child_package} depends on {dep.parent_package}"
                })
        
        # Analyze potential data dependencies (tables/views referenced across packages)
        data_dependencies = self._analyze_data_dependencies(ir_project, transform_packages)
        dependencies.extend(data_dependencies)
        
        return dependencies
    
    def _analyze_data_dependencies(self, ir_project: IRProject, transform_packages: List[str]) -> List[Dict[str, Any]]:
        """Analyze data dependencies between packages."""
        
        dependencies = []
        
        # Build table/view registry across all packages
        table_registry = {}
        for pkg_name in transform_packages:
            pkg_ir = ir_project.package_irs[pkg_name]
            for exe in pkg_ir.executables:
                if exe.type == ExecutableType.DATA_FLOW:
                    for comp in exe.components:
                        if comp.table:
                            table_registry[comp.table] = pkg_name
        
        # Check for cross-package table references
        for pkg_name in transform_packages:
            pkg_ir = ir_project.package_irs[pkg_name]
            for exe in pkg_ir.executables:
                if exe.sql:
                    referenced_tables = self._extract_table_references(exe.sql)
                    for table in referenced_tables:
                        if table in table_registry and table_registry[table] != pkg_name:
                            dependencies.append({
                                'parent_package': table_registry[table],
                                'child_package': pkg_name,
                                'type': 'data_dependency',
                                'table': table,
                                'description': f"Package {pkg_name} references table {table} from {table_registry[table]}"
                            })
        
        return dependencies
    
    def _generate_unified_project_yml(self, ir_project: IRProject, transform_packages: List[str]) -> Dict[str, Any]:
        """Generate unified dbt_project.yml configuration."""
        
        return {
            'name': f"{ir_project.project_name.lower()}_dbt",
            'version': ir_project.project_version,
            'description': f"dbt project generated from SSIS project: {ir_project.project_name}",
            'profile': f"{ir_project.project_name.lower()}_profile",
            
            'model-paths': ["models"],
            'analysis-paths': ["analyses"],
            'test-paths': ["tests"],
            'seed-paths': ["seeds"],
            'macro-paths': ["macros"],
            'snapshot-paths': ["snapshots"],
            'target-path': "target",
            'clean-targets': ["target", "dbt_packages"],
            
            'require-dbt-version': ">=1.7.0",
            
            'models': {
                f"{ir_project.project_name.lower()}_dbt": {
                    'materialized': 'view',
                    **{pkg_name.lower(): {
                        'staging': {
                            'materialized': 'view',
                            '+tags': ['staging']
                        },
                        'intermediate': {
                            'materialized': 'table',
                            '+tags': ['intermediate']
                        },
                        'marts': {
                            'materialized': 'table',
                            '+tags': ['marts']
                        }
                    } for pkg_name in transform_packages},
                    'shared': {
                        'materialized': 'view',
                        '+tags': ['shared']
                    }
                }
            },
            
            'vars': {
                'project_name': ir_project.project_name,
                'generated_at': datetime.now().isoformat(),
                'ssis_packages': transform_packages
            }
        }
    
    def _generate_profiles_yml(self, ir_project: IRProject) -> Dict[str, Any]:
        """Generate profiles.yml template."""
        
        return {
            f"{ir_project.project_name.lower()}_profile": {
                'target': 'dev',
                'outputs': {
                    'dev': {
                        'type': 'snowflake',
                        'account': '{{ env_var("SNOWFLAKE_ACCOUNT") }}',
                        'user': '{{ env_var("SNOWFLAKE_USER") }}',
                        'password': '{{ env_var("SNOWFLAKE_PASSWORD") }}',
                        'role': '{{ env_var("SNOWFLAKE_ROLE", "TRANSFORMER") }}',
                        'database': '{{ env_var("SNOWFLAKE_DATABASE") }}',
                        'warehouse': '{{ env_var("SNOWFLAKE_WAREHOUSE") }}',
                        'schema': f"{ir_project.project_name.lower()}_dev",
                        'threads': 4,
                        'keepalives_idle': 240
                    },
                    'prod': {
                        'type': 'snowflake',
                        'account': '{{ env_var("SNOWFLAKE_ACCOUNT") }}',
                        'user': '{{ env_var("SNOWFLAKE_USER") }}',
                        'password': '{{ env_var("SNOWFLAKE_PASSWORD") }}',
                        'role': '{{ env_var("SNOWFLAKE_ROLE", "TRANSFORMER") }}',
                        'database': '{{ env_var("SNOWFLAKE_DATABASE") }}',
                        'warehouse': '{{ env_var("SNOWFLAKE_WAREHOUSE") }}',
                        'schema': f"{ir_project.project_name.lower()}_prod",
                        'threads': 8,
                        'keepalives_idle': 240
                    }
                }
            }
        }
    
    def _is_source_component(self, component_type: ComponentType) -> bool:
        """Check if component is a data source."""
        return component_type in {
            ComponentType.OLEDB_SOURCE,
            ComponentType.ADONET_SOURCE,
            ComponentType.FLAT_FILE_SOURCE
        }
    
    def _is_destination_component(self, component_type: ComponentType) -> bool:
        """Check if component is a data destination."""
        return component_type in {
            ComponentType.OLEDB_DESTINATION,
            ComponentType.ADONET_DESTINATION,
            ComponentType.SNOWFLAKE_DEST
        }
    
    def _is_transformation_component(self, component_type: ComponentType) -> bool:
        """Check if component is a transformation."""
        return component_type in {
            ComponentType.DERIVED_COLUMN,
            ComponentType.LOOKUP,
            ComponentType.CONDITIONAL_SPLIT,
            ComponentType.AGGREGATE,
            ComponentType.UNION_ALL,
            ComponentType.SORT
        }
    
    def _build_destination_sql(self, dest: DataFlowComponent, transformations: List[DataFlowComponent]) -> str:
        """Build SQL for destination component."""
        
        # Find the transformation that feeds this destination
        upstream_model = "upstream_model"
        for transform in transformations:
            if dest.id in transform.outputs:
                upstream_model = f"int_{transform.name.lower()}"
                break
        
        return f"SELECT * FROM {{{{ ref('{upstream_model}') }}}}"
    
    def _build_conditional_split_sql(self, transform: DataFlowComponent) -> str:
        """Build SQL for conditional split transformation."""
        
        if transform.expression:
            condition = self.sql_converter.convert_tsql_to_snowflake(transform.expression)
            return f"""
SELECT *,
    CASE 
        WHEN {condition} THEN 'output_1'
        ELSE 'output_default'
    END AS split_condition
FROM {{{{ ref('upstream_model') }}}}
"""
        else:
            return "SELECT * FROM {{ ref('upstream_model') }}"
    
    def _build_aggregate_sql(self, transform: DataFlowComponent) -> str:
        """Build SQL for aggregate transformation."""
        
        # Default aggregate - customize based on properties
        return """
SELECT 
    COUNT(*) as record_count,
    MAX(updated_at) as last_updated
FROM {{ ref('upstream_model') }}
GROUP BY 1
"""
    
    def _convert_sql_task_to_dbt_model(self, sql_task: Executable, pkg_dir: Path, ir_project: IRProject) -> Optional[Dict[str, Any]]:
        """Convert Execute SQL task to dbt model."""
        
        if not sql_task.sql:
            return None
        
        model_name = f"sql_{sql_task.object_name.lower()}"
        model_file = pkg_dir / "marts" / f"{model_name}.sql"
        
        # Convert SQL to Snowflake
        converted_sql = self.sql_converter.convert_tsql_to_snowflake(sql_task.sql)
        
        model_content = f"""{{{{
  config(
    materialized='table',
    tags=['marts', 'sql_task', '{pkg_dir.name}']
  )
}}}}

-- Generated from Execute SQL Task: {sql_task.object_name}
-- Original SQL converted from T-SQL to Snowflake

{converted_sql}
"""
        
        model_file.write_text(model_content, encoding='utf-8')
        
        return {
            'name': model_name,
            'file_path': str(model_file),
            'type': 'final',
            'component_name': sql_task.object_name,
            'description': f"Model from Execute SQL task: {sql_task.object_name}"
        }
    
    def _extract_table_references(self, sql: str) -> List[str]:
        """Extract table references from SQL."""
        
        import re
        
        # Simple regex to find table references - could be enhanced
        table_pattern = r'\bFROM\s+(\w+)|JOIN\s+(\w+)|UPDATE\s+(\w+)|INSERT\s+INTO\s+(\w+)'
        matches = re.findall(table_pattern, sql, re.IGNORECASE)
        
        tables = []
        for match in matches:
            for group in match:
                if group:
                    tables.append(group)
        
        return list(set(tables))  # Remove duplicates
    
    def _generate_unified_schema(self, ir_project: IRProject, output_dir: Path, all_models: Dict[str, Dict], dependencies: List[Dict[str, Any]]):
        """Generate unified schema.yml with tests and documentation."""
        
        schema_config = {
            'version': 2,
            'models': []
        }
        
        # Add models from all packages
        for pkg_name, models in all_models.items():
            for category in ['staging', 'intermediate', 'marts']:
                for model in models.get(category, []):
                    model_config = {
                        'name': model['name'],
                        'description': model.get('description', ''),
                        'tags': [category, pkg_name],
                        'columns': [
                            {
                                'name': 'id',
                                'description': 'Primary key',
                                'tests': ['unique', 'not_null']
                            }
                        ]
                    }
                    schema_config['models'].append(model_config)
        
        # Write schema.yml
        schema_file = output_dir / "models" / "schema.yml"
        schema_file.write_text(yaml.dump(schema_config, default_flow_style=False), encoding='utf-8')
    
    def _generate_project_documentation(self, ir_project: IRProject, output_dir: Path, all_models: Dict[str, Dict]):
        """Generate project documentation."""
        
        docs_dir = output_dir / "docs"
        docs_dir.mkdir(exist_ok=True)
        
        # Generate migration report
        migration_report = f"""# {ir_project.project_name} dbt Migration Report

## Overview
This dbt project was generated from SSIS project: **{ir_project.project_name}**

- **Generated on**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **Total packages**: {len(ir_project.packages)}
- **Transformation packages**: {len(ir_project.get_transformation_packages())}
- **Total models**: {sum(len(models['staging']) + len(models['intermediate']) + len(models['marts']) for models in all_models.values())}

## Package Structure

{self._generate_package_structure_docs(all_models)}

## Dependencies

{self._generate_dependencies_docs(ir_project)}

## Getting Started

1. Set up your Snowflake credentials in `~/.dbt/profiles.yml` (see `profiles.yml.template`)
2. Install dbt dependencies: `dbt deps`
3. Test connection: `dbt debug`
4. Run all models: `dbt run`
5. Test data quality: `dbt test`
6. Generate documentation: `dbt docs generate && dbt docs serve`

## Migration Notes

- All T-SQL has been converted to Snowflake SQL syntax
- Review generated models for accuracy and performance optimization
- Consider adding additional tests and documentation
- Some complex transformations may require manual review

## Next Steps

1. Review and test all generated models
2. Add data quality tests
3. Implement incremental models where appropriate  
4. Set up CI/CD pipeline for dbt deployment
"""
        
        (docs_dir / "migration_report.md").write_text(migration_report, encoding='utf-8')
    
    def _generate_package_structure_docs(self, all_models: Dict[str, Dict]) -> str:
        """Generate documentation for package structure."""
        
        docs = []
        for pkg_name, models in all_models.items():
            docs.append(f"### {pkg_name}")
            docs.append(f"- Staging models: {len(models.get('staging', []))}")
            docs.append(f"- Intermediate models: {len(models.get('intermediate', []))}")
            docs.append(f"- Mart models: {len(models.get('marts', []))}")
            docs.append("")
        
        return "\\n".join(docs)
    
    def _generate_dependencies_docs(self, ir_project: IRProject) -> str:
        """Generate documentation for dependencies."""
        
        if not ir_project.dependencies:
            return "No cross-package dependencies found."
        
        docs = ["The following dependencies were identified:"]
        for dep in ir_project.dependencies:
            docs.append(f"- {dep.child_package} depends on {dep.parent_package} (via {dep.task_name})")
        
        return "\\n".join(docs)
    
    def _generate_project_readme(self, ir_project: IRProject, transform_packages: List[str]) -> str:
        """Generate main project README."""
        
        return f"""# {ir_project.project_name} dbt Project

This dbt project was automatically generated from SSIS project **{ir_project.project_name}**.

## 🏗️ Architecture

This project contains models from {len(transform_packages)} SSIS transformation packages:

{chr(10).join(f"- `{pkg}`" for pkg in transform_packages)}

### Model Organization

```
models/
├── shared/          # Cross-package shared models
{chr(10).join(f"├── {pkg.lower()}/       # Models from {pkg} package" for pkg in transform_packages)}
│   ├── staging/     # Raw data preparation
│   ├── intermediate/# Business logic transformations  
│   └── marts/       # Final analytics-ready tables
```

## 🚀 Quick Start

### Prerequisites
- dbt Core 1.7.0+
- Snowflake account and credentials

### Setup
1. **Configure your profile**:
   ```bash
   cp profiles.yml.template ~/.dbt/profiles.yml
   # Edit with your Snowflake credentials
   ```

2. **Install and test**:
   ```bash
   dbt deps
   dbt debug
   ```

3. **Run the models**:
   ```bash
   dbt run --target dev
   dbt test
   ```

## 📊 Data Flow

The models follow this general pattern:
1. **Staging**: Clean and standardize raw data
2. **Intermediate**: Apply business rules and transformations
3. **Marts**: Create analytics-ready datasets

## 🔧 Configuration

Key configuration is in `dbt_project.yml`:
- **Materialization**: Views for staging, tables for marts
- **Tags**: Organized by package and layer
- **Tests**: Data quality checks on key fields

## 📝 Migration Notes

- **SQL Conversion**: All T-SQL converted to Snowflake syntax
- **Manual Review**: Some complex transformations need verification
- **Performance**: Consider adding indexes and clustering keys
- **Incremental**: Evaluate models for incremental processing

## 🧪 Testing

Run tests to validate data quality:
```bash
dbt test --target dev
```

## 📚 Documentation

Generate and serve documentation:
```bash
dbt docs generate
dbt docs serve
```

---
*Generated by SSIS Migration Tool on {datetime.now().strftime('%Y-%m-%d')}*
"""
    
    def _generate_gitignore(self) -> str:
        """Generate .gitignore for dbt project."""
        
        return """# dbt
target/
dbt_packages/
logs/
.env

# OS
.DS_Store
Thumbs.db

# IDE
.vscode/
.idea/
*.swp
*.swo

# Python
__pycache__/
*.pyc
*.pyo
*.pyd
.Python
env/
venv/
.env
.venv

# Credentials
profiles.yml
.user.yml
"""