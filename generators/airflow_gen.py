"""Airflow DAG generator from SSIS IR.

Generates production-ready Airflow DAGs from Intermediate Representation
according to section 4 of the migration plan.
"""
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
import json
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, Template
from anthropic import Anthropic

from models.ir import (
    IRPackage, Executable, ExecutableType, PrecedenceEdge, 
    PrecedenceCondition, DataFlowComponent, ComponentType
)
from .sql_converter import SQLDialectConverter

logger = logging.getLogger(__name__)


class AirflowDAGGenerator:
    """Generator for Airflow DAGs from SSIS IR."""
    
    def __init__(self, anthropic_api_key: Optional[str] = None):
        """Initialize DAG generator.
        
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
        self.jinja_env = Environment(loader=FileSystemLoader(self.template_dir))
        
        # Load prompt templates using UTF-8 to avoid locale issues on Windows
        with open(self.template_dir / "airflow_system.txt", "r", encoding="utf-8") as f:
            self.system_prompt = f.read()
        
        with open(self.template_dir / "airflow_user_template.md", "r", encoding="utf-8") as f:
            self.user_template = f.read()
    
    def generate_dag(self, ir_package: IRPackage, output_dir: str) -> Dict[str, Any]:
        """Generate Airflow DAG from IR package.
        
        Args:
            ir_package: IR package to convert
            output_dir: Directory to write generated files
            
        Returns:
            Generation result dictionary
        """
        logger.info(f"Generating Airflow DAG for package: {ir_package.package_name}")
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        result = {
            "success": False,
            "dag_file": None,
            "helper_files": [],
            "warnings": [],
            "errors": []
        }
        
        try:
            # Prepare IR for Airflow generation
            airflow_ir = self._prepare_airflow_ir(ir_package)
            
            # Generate DAG using Claude if available, fallback to template
            if self.anthropic_client:
                dag_code = self._generate_with_claude(airflow_ir)
            else:
                dag_code = self._generate_with_template(airflow_ir)
            
            # Write DAG file
            dag_filename = f"{self._safe_name(ir_package.package_name)}_dag.py"
            dag_file_path = output_path / dag_filename
            
            with open(dag_file_path, "w", encoding="utf-8") as f:
                f.write(dag_code)
            
            result["dag_file"] = str(dag_file_path)
            result["success"] = True
            
            # Generate helper files
            helper_files = self._generate_helper_files(ir_package, output_path)
            result["helper_files"] = helper_files
            
            logger.info(f"Generated DAG: {dag_file_path}")
            
        except Exception as e:
            logger.error(f"Failed to generate Airflow DAG: {e}")
            result["errors"].append(str(e))
        
        return result
    
    def _prepare_airflow_ir(self, ir_package: IRPackage) -> Dict[str, Any]:
        """Prepare IR data specifically for Airflow generation.
        
        Args:
            ir_package: IR package to prepare
            
        Returns:
            Prepared IR dictionary
        """
        # Convert to dict and add Airflow-specific annotations
        ir_dict = ir_package.model_dump()
        
        # Convert SQL dialects and annotate executables
        for executable in ir_dict.get("executables", []):
            # Ensure we have a safe task_id for use in template
            obj_name = executable.get("object_name") or executable.get("name") or "task"
            executable["task_id"] = self._safe_name(obj_name)

            if executable.get("sql"):
                converted_sql = self.sql_converter.convert_to_snowflake(
                    executable["sql"], 
                    executable.get("dialect", "tsql")
                )
                executable["sql"] = converted_sql
            
            # Annotate executables with Airflow mappings
            executable["airflow_operator"] = self._map_to_airflow_operator(executable)
        
        # Add dependency graph information and safe edge IDs
        ir_dict["dependency_graph"] = self._build_dependency_graph(ir_package.edges)
        ir_dict["parallel_groups"] = self._identify_parallel_groups(ir_package.edges)
        # Precompute safe from/to identifiers for edges
        for edge in ir_dict.get("edges", []):
            from_raw = edge.get("from_task", "")
            to_raw = edge.get("to_task", "")
            from_last = (from_raw.replace("/", "\\").split("\\") or [from_raw])[-1]
            to_last = (to_raw.replace("/", "\\").split("\\") or [to_raw])[-1]
            edge["from_id"] = self._safe_name(from_last)
            edge["to_id"] = self._safe_name(to_last)
        
        return ir_dict
    
    def _generate_with_claude(self, ir_data: Dict[str, Any]) -> str:
        """Generate DAG using Claude API.
        
        Args:
            ir_data: Prepared IR data
            
        Returns:
            Generated DAG Python code
        """
        logger.info("Generating DAG with Claude AI")
        
        # Prepare user prompt
        ir_json = json.dumps(ir_data, indent=2)
        user_prompt = self.user_template.replace("{IR_JSON_HERE}", ir_json)
        
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
            
            return message.content[0].text
            
        except Exception as e:
            logger.warning(f"Claude generation failed: {e}, falling back to template")
            return self._generate_with_template(ir_data)
    
    def _generate_with_template(self, ir_data: Dict[str, Any]) -> str:
        """Generate DAG using Jinja2 template fallback.
        
        Args:
            ir_data: Prepared IR data
            
        Returns:
            Generated DAG Python code
        """
        logger.info("Generating DAG with template fallback")
        
        # Create a basic template for fallback
        template_content = """# Generated Airflow DAG from SSIS Package: {{ package_name }}
# Migration Tool: SSIS-to-Airflow Migrator
# Generated on: {{ generation_date }}

from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.models.param import Param
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

# Constants
SNOWFLAKE_CONN_ID = "snowflake_default"

# Default arguments
default_args = {
    'owner': 'ssis-migrator',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id='{{ safe_package_name }}',
    default_args=default_args,
    description='Migrated from SSIS package: {{ package_name }}',
    schedule_interval=None,
    catchup=False,
    params={
        {% for param in parameters %}
        "{{ param.name }}": Param({{ param.value | default("None") }}, type="{{ (param.type or 'str') | lower }}"),
        {% endfor %}
        {% for var in variables %}
        "{{ var.name }}": Param({{ var.value | default("None") }}, type="{{ (var.type or 'str') | lower }}"),
        {% endfor %}
    },
    render_template_as_native_obj=True,
    tags=['ssis-migration', '{{ package_name }}'],
)

# Helper functions
def log_task_start(task_name):
    logging.info(f"Starting SSIS migrated task: {task_name}")

def log_task_complete(task_name):
    logging.info(f"Completed SSIS migrated task: {task_name}")

{% for executable in executables %}
{% if "EXECUTE_SQL" in executable.type %}
# Task: {{ executable.object_name }} (Execute SQL)
{{ safe_name(executable.object_name) }} = SnowflakeOperator(
    task_id='{{ executable.task_id }}',
    sql=\"\"\"{{ executable.sql | default("-- TODO: Add SQL statement") }}\"\"\",
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag,
)

{% elif "DATA_FLOW" in executable.type %}
# Task Group: {{ executable.object_name }} (Data Flow)
with TaskGroup(group_id='{{ executable.task_id }}', dag=dag) as {{ safe_name(executable.object_name) }}:
    {% for component in executable.components %}
    # TODO: Implement component {{ component.name }} ({{ component.component_type }})
    {{ safe_name(component.name) }} = DummyOperator(
        task_id='{{ safe_name(component.name) }}',
        dag=dag,
    )
    {% endfor %}

{% elif "SCRIPT" in executable.type %}
# Task: {{ executable.object_name }} (Script Task)
def {{ safe_name(executable.object_name) }}_func(**context):
    # TODO: Implement script logic from SSIS Script Task
    log_task_start("{{ executable.object_name }}")
    # Add your Python code here
    log_task_complete("{{ executable.object_name }}")
    return "success"

{{ safe_name(executable.object_name) }} = PythonOperator(
    task_id='{{ executable.task_id }}',
    python_callable={{ safe_name(executable.object_name) }}_func,
    dag=dag,
)

{% else %}
# Task: {{ executable.object_name }} ({{ executable.type }})
# TODO: Implement {{ executable.type }} task type
{{ safe_name(executable.object_name) }} = DummyOperator(
    task_id='{{ executable.task_id }}',
    dag=dag,
)

{% endif %}
{% endfor %}

# Set up dependencies (precedence constraints)
{% for edge in edges %}
# Precedence: {{ edge.from_task }} -> {{ edge.to_task }} ({{ edge.condition }})
{% if edge.condition == "Success" %}
{{ edge.from_id }} >> {{ edge.to_id }}
{% elif edge.condition == "Failure" %}
{{ edge.to_id }}.set_upstream({{ edge.from_id }})
{{ edge.to_id }}.trigger_rule = TriggerRule.ONE_FAILED
{% elif edge.condition == "Completion" %}
{{ edge.to_id }}.set_upstream({{ edge.from_id }})
{{ edge.to_id }}.trigger_rule = TriggerRule.ALL_DONE
{% else %}
# TODO: Implement expression-based precedence for {{ edge.expression | default("complex condition") }}
{{ edge.from_id }} >> {{ edge.to_id }}
{% endif %}
{% endfor %}

# TODO: Review and test the generated DAG
# TODO: Configure Snowflake connections in Airflow
# TODO: Test all task implementations
# TODO: Add proper error handling and monitoring
"""
        
        template = Template(template_content)
        
        return template.render(
            package_name=ir_data.get("package_name", "unknown"),
            safe_package_name=self._safe_name(ir_data.get("package_name", "unknown")),
            generation_date=datetime.now().isoformat(),
            parameters=ir_data.get("parameters", []),
            variables=ir_data.get("variables", []),
            executables=ir_data.get("executables", []),
            edges=ir_data.get("edges", []),
            safe_name=self._safe_name,
        )
    
    def _map_to_airflow_operator(self, executable: Dict[str, Any]) -> str:
        """Map SSIS executable type to Airflow operator.
        
        Args:
            executable: Executable dictionary
            
        Returns:
            Airflow operator class name
        """
        exe_type = executable.get("type")
        
        mapping = {
            "ExecuteSQL": "SnowflakeOperator",
            "ScriptTask": "PythonOperator",
            "DataFlow": "TaskGroup",
            "SequenceContainer": "TaskGroup",
            "ForEachLoop": "PythonOperator",  # With dynamic mapping
            "BulkInsert": "SnowflakeOperator",
            "FileSystem": "BashOperator",
            "FTP": "FTPHook",
            "SendMail": "EmailOperator",
        }
        
        return mapping.get(exe_type, "DummyOperator")
    
    def _build_dependency_graph(self, edges: List[PrecedenceEdge]) -> Dict[str, List[str]]:
        """Build dependency graph from precedence edges."""
        graph = {}
        
        for edge in edges:
            if edge.from_task not in graph:
                graph[edge.from_task] = []
            graph[edge.from_task].append(edge.to_task)
        
        return graph
    
    def _identify_parallel_groups(self, edges: List[PrecedenceEdge]) -> Dict[str, List[str]]:
        """Identify groups of tasks that can run in parallel."""
        parallel_groups = {}
        
        # Group by source task
        by_source = {}
        for edge in edges:
            if edge.condition == PrecedenceCondition.SUCCESS:  # Only success paths can be parallel
                if edge.from_task not in by_source:
                    by_source[edge.from_task] = []
                by_source[edge.from_task].append(edge.to_task)
        
        # Find parallel groups (multiple success dependencies from same source)
        for source, targets in by_source.items():
            if len(targets) > 1:
                parallel_groups[source] = targets
        
        return parallel_groups
    
    def _generate_helper_files(self, ir_package: IRPackage, output_dir: Path) -> List[str]:
        """Generate helper files for the DAG.
        
        Args:
            ir_package: IR package
            output_dir: Output directory
            
        Returns:
            List of generated helper file paths
        """
        helper_files = []
        
        # Generate connection setup script
        if ir_package.connection_managers:
            conn_script = self._generate_connection_script(ir_package.connection_managers)
            conn_file = output_dir / f"{self._safe_name(ir_package.package_name)}_connections.py"
            
            with open(conn_file, "w", encoding="utf-8") as f:
                f.write(conn_script)
            
            helper_files.append(str(conn_file))
        
        # Generate requirements file
        requirements = self._generate_requirements()
        req_file = output_dir / "requirements.txt"
        
        with open(req_file, "w", encoding="utf-8") as f:
            f.write(requirements)
        
        helper_files.append(str(req_file))
        
        return helper_files
    
    def _generate_connection_script(self, connection_managers: List[Dict[str, Any]]) -> str:
        """Generate Airflow connection setup script."""
        return f"""#!/usr/bin/env python3
\"\"\"
Airflow connection setup for migrated SSIS package.
Run this script to create the required connections.
\"\"\"

from airflow.models import Connection
from airflow import settings

def setup_connections():
    session = settings.Session()
    
    connections = [
{self._format_connections_for_script(connection_managers)}
    ]
    
    for conn_data in connections:
        existing = session.query(Connection).filter(
            Connection.conn_id == conn_data['conn_id']
        ).first()
        
        if existing:
            session.delete(existing)
        
        conn = Connection(**conn_data)
        session.add(conn)
    
    session.commit()
    session.close()
    print(f"Created {{len(connections)}} connections")

if __name__ == '__main__':
    setup_connections()
"""
    
    def _format_connections_for_script(self, connection_managers: List[Dict[str, Any]]) -> str:
        """Format connection managers for Python script."""
        conn_lines = []
        
        for cm in connection_managers:
            safe_id = self._safe_name(cm.get("name", cm.get("id", "unknown")))
            conn_lines.append(f"""        {{
            'conn_id': 'ssis_{safe_id}',
            'conn_type': '{self._map_ssis_to_airflow_conn_type(cm.get("type", ""))}',
            'description': 'Migrated from SSIS: {cm.get("name", "Unknown")}',
            'host': 'TODO_UPDATE',
            'login': 'TODO_UPDATE',
            'password': 'TODO_UPDATE',
            'schema': 'TODO_UPDATE'
        }},""")
        
        return "\n".join(conn_lines)
    
    def _map_ssis_to_airflow_conn_type(self, ssis_type: str) -> str:
        """Map SSIS connection type to Airflow connection type."""
        mapping = {
            "OLEDB": "mssql",
            "ADONET": "mssql",
            "SNOWFLAKE": "snowflake",
            "HTTP": "http",
            "FTP": "ftp",
            "SMTP": "email"
        }
        return mapping.get(ssis_type.upper(), "generic")
    
    def _generate_requirements(self) -> str:
        """Generate requirements.txt for the DAG."""
        return """# Airflow requirements for SSIS migrated DAG
apache-airflow>=2.8.0
apache-airflow-providers-snowflake>=5.0.0
apache-airflow-providers-ftp>=3.0.0
apache-airflow-providers-email>=1.0.0
snowflake-connector-python>=3.0.0
"""
    
    def _safe_name(self, name: str) -> str:
        """Convert name to safe Python identifier."""
        import re
        # Replace spaces and special chars with underscore
        safe = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Ensure it doesn't start with a number
        if safe and safe[0].isdigit():
            safe = f"pkg_{safe}"
        return safe.lower()