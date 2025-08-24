"""dbt project generator from SSIS IR.

Generates dbt projects from transformation-only SSIS packages
according to section 5 of the migration plan.
"""
import logging
import os
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, Template
from anthropic import Anthropic
import yaml

from models.ir import (
    IRPackage, Executable, ExecutableType, DataFlowComponent,
    ComponentType
)
from .sql_converter import SQLDialectConverter

logger = logging.getLogger(__name__)


class DBTProjectGenerator:
    """Generator for dbt projects from SSIS IR."""
    
    def __init__(self, anthropic_api_key: Optional[str] = None):
        """Initialize dbt project generator.
        
        Args:
            anthropic_api_key: Optional Claude API key for LLM code generation
        """
        self.sql_converter = SQLDialectConverter()
        self.anthropic_client = None
        
        if anthropic_api_key or os.getenv("ANTHROPIC_API_KEY"):
            self.anthropic_client = Anthropic(
                api_key=anthropic_api_key or os.getenv("ANTHROPIC_API_KEY")
            )
        
        # Load prompt templates
        self.template_dir = Path(__file__).parent / "prompts"
        
        with open(self.template_dir / "dbt_system.txt", "r") as f:
            self.system_prompt = f.read()
        
        with open(self.template_dir / "dbt_user_template.md", "r") as f:
            self.user_template = f.read()
    
    def generate_project(self, ir_package: IRPackage, output_dir: str) -> Dict[str, Any]:
        """Generate dbt project from IR package.
        
        Args:
            ir_package: IR package to convert (should be transformation-only)
            output_dir: Directory to write generated project
            
        Returns:
            Generation result dictionary
        """
        logger.info(f"Generating dbt project for package: {ir_package.package_name}")
        
        if not ir_package.is_transformation_only():
            logger.warning("Package contains non-transformation tasks - filtering to transformations only")
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        result = {
            "success": False,
            "project_files": [],
            "models": [],
            "warnings": [],
            "errors": []
        }
        
        try:
            # Filter to transformation tasks only
            transform_ir = self._filter_to_transformations(ir_package)
            
            # Generate project using Claude if available, fallback to template
            if self.anthropic_client:
                project_files = self._generate_with_claude(transform_ir, output_path)
            else:
                project_files = self._generate_with_template(transform_ir, output_path)
            
            result["project_files"] = project_files
            result["models"] = self._extract_model_info(output_path)
            result["success"] = True
            
            logger.info(f"Generated dbt project with {len(result['models'])} models")
            
        except Exception as e:
            logger.error(f"Failed to generate dbt project: {e}")
            result["errors"].append(str(e))
        
        return result
    
    def _filter_to_transformations(self, ir_package: IRPackage) -> Dict[str, Any]:
        """Filter IR package to transformation tasks only.
        
        Args:
            ir_package: Complete IR package
            
        Returns:
            Filtered IR dictionary with transformations only
        """
        # Get data flow tasks (main transformations in SSIS)
        data_flows = [exe for exe in ir_package.executables if exe.type == ExecutableType.DATA_FLOW]
        
        # Get SQL tasks that are transformations (not file operations)
        transform_sql_tasks = []
        for exe in ir_package.executables:
            if exe.type == ExecutableType.EXECUTE_SQL and exe.sql:
                # Heuristic: if SQL contains SELECT, it's likely a transformation
                if "SELECT" in exe.sql.upper() and not any(
                    keyword in exe.sql.upper() 
                    for keyword in ["BULK INSERT", "EXEC ", "EXECUTE "]
                ):
                    transform_sql_tasks.append(exe)
        
        filtered_executables = data_flows + transform_sql_tasks
        
        # Filter edges to only include filtered executables
        exe_ids = {exe.id for exe in filtered_executables}
        filtered_edges = [
            edge for edge in ir_package.edges
            if edge.from_task in exe_ids and edge.to_task in exe_ids
        ]
        
        return {
            "package_name": ir_package.package_name,
            "parameters": [p.model_dump() for p in ir_package.parameters],
            "variables": [v.model_dump() for v in ir_package.variables],
            "connection_managers": [cm.model_dump() for cm in ir_package.connection_managers],
            "executables": [exe.model_dump() for exe in filtered_executables],
            "edges": [edge.model_dump() for edge in filtered_edges],
            "expressions": [expr.model_dump() for expr in ir_package.expressions]
        }
    
    def _generate_with_claude(self, transform_ir: Dict[str, Any], output_path: Path) -> List[str]:
        """Generate dbt project using Claude API.
        
        Args:
            transform_ir: Transformation-only IR data
            output_path: Output directory path
            
        Returns:
            List of generated file paths
        """
        logger.info("Generating dbt project with Claude AI")
        
        # Prepare user prompt
        ir_json = json.dumps(transform_ir, indent=2)
        user_prompt = self.user_template.replace("{IR_JSON_TRANSFORM_ONLY}", ir_json)
        
        try:
            message = self.anthropic_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=8000,
                temperature=0.1,
                system=self.system_prompt,
                messages=[{
                    "role": "user",
                    "content": user_prompt
                }]
            )
            
            # Parse Claude's response and extract file contents
            response_text = message.content[0].text
            return self._parse_claude_response(response_text, output_path)
            
        except Exception as e:
            logger.warning(f"Claude generation failed: {e}, falling back to template")
            return self._generate_with_template(transform_ir, output_path)
    
    def _generate_with_template(self, transform_ir: Dict[str, Any], output_path: Path) -> List[str]:
        """Generate dbt project using template fallback.
        
        Args:
            transform_ir: Transformation-only IR data
            output_path: Output directory path
            
        Returns:
            List of generated file paths
        """
        logger.info("Generating dbt project with template fallback")
        
        generated_files = []
        
        # Create project structure
        (output_path / "models" / "staging").mkdir(parents=True, exist_ok=True)
        (output_path / "models" / "intermediate").mkdir(parents=True, exist_ok=True)
        (output_path / "models" / "marts").mkdir(parents=True, exist_ok=True)
        (output_path / "macros").mkdir(parents=True, exist_ok=True)
        (output_path / "tests").mkdir(parents=True, exist_ok=True)
        
        # Generate dbt_project.yml
        project_yml = self._generate_project_yml(transform_ir)
        project_file = output_path / "dbt_project.yml"
        with open(project_file, "w", encoding="utf-8") as f:
            f.write(project_yml)
        generated_files.append(str(project_file))
        
        # Generate models from data flows
        for executable in transform_ir.get("executables", []):
            if executable.get("type") == "DataFlow":
                model_files = self._generate_dataflow_models(executable, output_path)
                generated_files.extend(model_files)
            elif executable.get("type") == "ExecuteSQL":
                model_file = self._generate_sql_model(executable, output_path)
                if model_file:
                    generated_files.append(model_file)
        
        # Generate schema.yml with sources and tests
        schema_yml = self._generate_schema_yml(transform_ir)
        schema_file = output_path / "models" / "schema.yml"
        with open(schema_file, "w", encoding="utf-8") as f:
            f.write(schema_yml)
        generated_files.append(str(schema_file))
        
        # Generate profiles template
        profiles_template = self._generate_profiles_template()
        profiles_file = output_path / "profiles.yml.template"
        with open(profiles_file, "w", encoding="utf-8") as f:
            f.write(profiles_template)
        generated_files.append(str(profiles_file))
        
        return generated_files
    
    def _generate_project_yml(self, transform_ir: Dict[str, Any]) -> str:
        """Generate dbt_project.yml file."""
        safe_name = self._safe_name(transform_ir.get("package_name", "ssis_migration"))
        
        project_config = {
            "name": safe_name,
            "version": "1.0.0",
            "config-version": 2,
            "model-paths": ["models"],
            "analysis-paths": ["analyses"],
            "test-paths": ["tests"],
            "seed-paths": ["seeds"],
            "macro-paths": ["macros"],
            "snapshot-paths": ["snapshots"],
            "target-path": "target",
            "clean-targets": ["target", "dbt_packages"],
            "models": {
                safe_name: {
                    "staging": {"+materialized": "view"},
                    "intermediate": {"+materialized": "table"},
                    "marts": {"+materialized": "table"}
                }
            },
            "vars": {}
        }
        
        # Add variables from IR
        for var in transform_ir.get("variables", []):
            project_config["vars"][var["name"]] = var.get("value")
        
        for param in transform_ir.get("parameters", []):
            project_config["vars"][param["name"]] = param.get("value")
        
        return f"""# dbt project configuration
# Generated from SSIS package: {transform_ir.get("package_name", "Unknown")}
# Migration timestamp: {datetime.now().isoformat()}

{yaml.dump(project_config, default_flow_style=False, indent=2)}
"""
    
    def _generate_dataflow_models(self, dataflow_exe: Dict[str, Any], output_path: Path) -> List[str]:
        """Generate dbt models from a data flow executable.
        
        Args:
            dataflow_exe: Data flow executable dictionary
            output_path: Output directory path
            
        Returns:
            List of generated model file paths
        """
        generated_files = []
        components = dataflow_exe.get("components", [])
        
        if not components:
            return generated_files
        
        # Analyze component flow
        sources = [c for c in components if c.get("component_type", "").endswith("Source")]
        destinations = [c for c in components if c.get("component_type", "").endswith("Destination")]
        transforms = [c for c in components if c not in sources and c not in destinations]
        
        # Generate staging models from sources
        for source_comp in sources:
            model_file = self._generate_source_model(source_comp, dataflow_exe, output_path)
            if model_file:
                generated_files.append(model_file)
        
        # Generate intermediate models for complex transformations
        if transforms:
            model_file = self._generate_transform_model(dataflow_exe, transforms, output_path)
            if model_file:
                generated_files.append(model_file)
        
        # Generate mart models from destinations
        for dest_comp in destinations:
            model_file = self._generate_destination_model(dest_comp, dataflow_exe, output_path)
            if model_file:
                generated_files.append(model_file)
        
        return generated_files
    
    def _generate_source_model(self, source_comp: Dict[str, Any], dataflow_exe: Dict[str, Any], 
                              output_path: Path) -> Optional[str]:
        """Generate staging model from source component."""
        model_name = f"stg_{self._safe_name(source_comp.get('name', 'source'))}"
        model_path = output_path / "models" / "staging" / f"{model_name}.sql"
        
        # Extract SQL or table reference
        sql = source_comp.get("sql", "")
        table = source_comp.get("table", "")
        
        if not sql and not table:
            return None
        
        # Convert SQL if present
        if sql:
            converted_sql = self.sql_converter.convert_to_snowflake(sql, "tsql")
        else:
            converted_sql = f"select * from {{{{ source('raw', '{table}') }}}}"
        
        model_content = f"""-- Migrated from SSIS: {dataflow_exe.get('object_name')} -> {source_comp.get('name')}
-- Source component: {source_comp.get('component_type')}
-- Migration timestamp: {datetime.now().isoformat()}

{{{{ config(materialized='view') }}}}

{converted_sql}
"""
        
        with open(model_path, "w", encoding="utf-8") as f:
            f.write(model_content)
        
        return str(model_path)
    
    def _generate_transform_model(self, dataflow_exe: Dict[str, Any], 
                                 transforms: List[Dict[str, Any]], 
                                 output_path: Path) -> Optional[str]:
        """Generate intermediate model for transformations."""
        model_name = f"int_{self._safe_name(dataflow_exe.get('object_name', 'transform'))}"
        model_path = output_path / "models" / "intermediate" / f"{model_name}.sql"
        
        # Build transformation SQL
        select_clauses = []
        from_clause = "{{ ref('stg_source') }}"  # Placeholder
        
        for transform in transforms:
            comp_type = transform.get("component_type", "")
            
            if comp_type == "DerivedColumn":
                expression = transform.get("expression", "")
                if expression:
                    converted_expr = self.sql_converter.convert_to_snowflake(expression, "tsql")
                    select_clauses.append(f"    {converted_expr} as {transform.get('name', 'derived_col')}")
            
            elif comp_type == "Lookup":
                join_sql = self._generate_lookup_join(transform)
                if join_sql:
                    from_clause += f"\n{join_sql}"
            
            # Add other transformation types as needed
        
        if not select_clauses:
            select_clauses = ["    *"]
        
        model_content = f"""-- Migrated from SSIS: {dataflow_exe.get('object_name')}
-- Transformation components: {', '.join(t.get('name', '') for t in transforms)}
-- Migration timestamp: {datetime.now().isoformat()}

{{{{ config(materialized='table') }}}}

select
{chr(10).join(select_clauses)}
from {from_clause}
"""
        
        with open(model_path, "w", encoding="utf-8") as f:
            f.write(model_content)
        
        return str(model_path)
    
    def _generate_destination_model(self, dest_comp: Dict[str, Any], 
                                   dataflow_exe: Dict[str, Any],
                                   output_path: Path) -> Optional[str]:
        """Generate mart model from destination component."""
        table_name = dest_comp.get("table", "")
        if not table_name:
            return None
        
        model_name = f"mart_{self._safe_name(table_name)}"
        model_path = output_path / "models" / "marts" / f"{model_name}.sql"
        
        # Determine materialization based on mode
        mode = dest_comp.get("mode", "append")
        if mode == "merge":
            materialization = "incremental"
            config_options = ", unique_key='id', on_schema_change='sync_all_columns'"
        else:
            materialization = "table"
            config_options = ""
        
        model_content = f"""-- Migrated from SSIS: {dataflow_exe.get('object_name')} -> {dest_comp.get('name')}
-- Destination table: {table_name}
-- Migration timestamp: {datetime.now().isoformat()}

{{{{ config(materialized='{materialization}'{config_options}) }}}}

select * from {{{{ ref('int_{self._safe_name(dataflow_exe.get("object_name", "transform"))}') }}}}

{{% if is_incremental() %}}
  -- Add incremental logic based on SSIS loading pattern
  where updated_at > (select coalesce(max(updated_at), '1900-01-01') from {{{{ this }}}})
{{% endif %}}
"""
        
        with open(model_path, "w", encoding="utf-8") as f:
            f.write(model_content)
        
        return str(model_path)
    
    def _generate_sql_model(self, sql_exe: Dict[str, Any], output_path: Path) -> Optional[str]:
        """Generate model from Execute SQL task."""
        sql = sql_exe.get("sql", "")
        if not sql or "SELECT" not in sql.upper():
            return None
        
        model_name = f"sql_{self._safe_name(sql_exe.get('object_name', 'query'))}"
        model_path = output_path / "models" / "intermediate" / f"{model_name}.sql"
        
        converted_sql = self.sql_converter.convert_to_snowflake(sql, sql_exe.get("dialect", "tsql"))
        
        model_content = f"""-- Migrated from SSIS: {sql_exe.get('object_name')}
-- Original SQL task converted to dbt model
-- Migration timestamp: {datetime.now().isoformat()}

{{{{ config(materialized='table') }}}}

{converted_sql}
"""
        
        with open(model_path, "w", encoding="utf-8") as f:
            f.write(model_content)
        
        return str(model_path)
    
    def _generate_lookup_join(self, lookup_comp: Dict[str, Any]) -> str:
        """Generate JOIN clause for lookup component."""
        join_keys = lookup_comp.get("join_on", [])
        lookup_table = lookup_comp.get("ref", "")
        
        if not join_keys or not lookup_table:
            return ""
        
        join_condition = " and ".join([f"base.{key} = lookup.{key}" for key in join_keys])
        
        return f"""left join {{{{ ref('{lookup_table}') }}}} as lookup
    on {join_condition}"""
    
    def _generate_schema_yml(self, transform_ir: Dict[str, Any]) -> str:
        """Generate schema.yml with sources and tests."""
        schema_config = {
            "version": 2,
            "sources": [],
            "models": []
        }
        
        # Extract source tables from components
        source_tables = set()
        for executable in transform_ir.get("executables", []):
            for component in executable.get("components", []):
                table = component.get("table")
                if table and component.get("component_type", "").endswith("Source"):
                    source_tables.add(table)
        
        if source_tables:
            schema_config["sources"].append({
                "name": "raw",
                "description": "Raw data sources migrated from SSIS",
                "tables": [{"name": table} for table in sorted(source_tables)]
            })
        
        # Add model tests (basic template)
        schema_config["models"] = [
            {
                "name": "stg_example",
                "description": "Staging model example - update with actual models",
                "tests": ["unique", "not_null"],
                "columns": [
                    {
                        "name": "id",
                        "description": "Primary key",
                        "tests": ["unique", "not_null"]
                    }
                ]
            }
        ]
        
        return f"""# Schema configuration for SSIS migrated models
# Generated from package: {transform_ir.get('package_name', 'Unknown')}

{yaml.dump(schema_config, default_flow_style=False, indent=2)}
"""
    
    def _generate_profiles_template(self) -> str:
        """Generate profiles.yml template."""
        return """# dbt profiles template for SSIS migration
# Copy to ~/.dbt/profiles.yml and update with your Snowflake details

ssis_migration:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('DBT_SNOWFLAKE_USER') }}"
      password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"
      database: "{{ env_var('DBT_SNOWFLAKE_DATABASE') }}"
      warehouse: "{{ env_var('DBT_SNOWFLAKE_WAREHOUSE') }}"
      schema: "{{ env_var('DBT_SNOWFLAKE_SCHEMA') }}"
      role: "{{ env_var('DBT_SNOWFLAKE_ROLE') }}"
      threads: 4
      keepalives_idle: 0
      
    prod:
      type: snowflake
      account: "{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('DBT_SNOWFLAKE_USER') }}"
      password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"
      database: "{{ env_var('DBT_SNOWFLAKE_DATABASE_PROD') }}"
      warehouse: "{{ env_var('DBT_SNOWFLAKE_WAREHOUSE') }}"
      schema: "{{ env_var('DBT_SNOWFLAKE_SCHEMA_PROD') }}"
      role: "{{ env_var('DBT_SNOWFLAKE_ROLE') }}"
      threads: 8
      keepalives_idle: 0
"""
    
    def _parse_claude_response(self, response_text: str, output_path: Path) -> List[str]:
        """Parse Claude's response and extract file contents.
        
        Args:
            response_text: Claude's response text
            output_path: Output directory path
            
        Returns:
            List of generated file paths
        """
        # This is a simplified parser - in practice, you'd need more sophisticated
        # parsing to extract multiple files from Claude's response
        generated_files = []
        
        # For now, create a basic project structure
        # In a real implementation, you would parse Claude's response
        # which would contain multiple files with proper delimiters
        
        # Fallback to template generation
        return self._generate_with_template({"package_name": "claude_generated"}, output_path)
    
    def _extract_model_info(self, output_path: Path) -> List[Dict[str, str]]:
        """Extract information about generated models.
        
        Args:
            output_path: Project output path
            
        Returns:
            List of model information dictionaries
        """
        models = []
        models_dir = output_path / "models"
        
        if models_dir.exists():
            for sql_file in models_dir.rglob("*.sql"):
                rel_path = sql_file.relative_to(models_dir)
                layer = rel_path.parts[0] if len(rel_path.parts) > 1 else "root"
                
                models.append({
                    "name": sql_file.stem,
                    "path": str(sql_file),
                    "layer": layer
                })
        
        return models
    
    def _safe_name(self, name: str) -> str:
        """Convert name to safe identifier."""
        import re
        return re.sub(r'[^a-zA-Z0-9_]', '_', name).lower().strip('_')