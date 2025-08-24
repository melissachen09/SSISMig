"""Master DAG Generator - Creates orchestration DAGs for multi-package SSIS projects.

Generates Airflow DAGs that coordinate execution of multiple package DAGs
based on ExecutePackageTask dependencies and project structure.
"""
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta

from models.ir import IRProject, PackageDependency, PrecedenceCondition

logger = logging.getLogger(__name__)


class MasterDAGGenerator:
    """Generates master orchestration DAGs for multi-package SSIS projects."""
    
    def __init__(self, anthropic_key: Optional[str] = None):
        self.anthropic_key = anthropic_key
        self.logger = logging.getLogger(__name__)
    
    def generate_master_dag(self, ir_project: IRProject, output_dir: Path) -> Dict[str, str]:
        """Generate master DAG that orchestrates package DAGs.
        
        Args:
            ir_project: Complete project IR with dependencies
            output_dir: Directory to write DAG files
            
        Returns:
            Dict mapping DAG names to generated file paths
        """
        self.logger.info(f"Generating master DAG for project: {ir_project.project_name}")
        
        generated_files = {}
        
        # Analyze execution strategy
        strategy = ir_project.recommend_migration_strategy()
        
        if strategy['needs_master_dag']:
            # Generate master orchestration DAG
            master_dag_code = self._generate_master_orchestration_dag(ir_project)
            master_dag_file = output_dir / f"{ir_project.project_name.lower()}_master_dag.py"
            master_dag_file.write_text(master_dag_code, encoding='utf-8')
            generated_files['master'] = str(master_dag_file)
            self.logger.info(f"Generated master DAG: {master_dag_file}")
        
        # Generate individual package DAGs with coordination
        for pkg_name in ir_project.package_irs.keys():
            pkg_dag_code = self._generate_coordinated_package_dag(ir_project, pkg_name)
            pkg_dag_file = output_dir / f"{pkg_name.lower()}_dag.py"
            pkg_dag_file.write_text(pkg_dag_code, encoding='utf-8')
            generated_files[pkg_name] = str(pkg_dag_file)
            self.logger.debug(f"Generated package DAG: {pkg_dag_file}")
        
        # Generate project-level connections script
        conn_script = self._generate_project_connections_script(ir_project)
        conn_file = output_dir / f"{ir_project.project_name.lower()}_connections.py"
        conn_file.write_text(conn_script, encoding='utf-8')
        generated_files['connections'] = str(conn_file)
        
        # Generate requirements.txt
        requirements = self._generate_requirements_txt(ir_project)
        req_file = output_dir / "requirements.txt"
        req_file.write_text(requirements, encoding='utf-8')
        generated_files['requirements'] = str(req_file)
        
        self.logger.info(f"Generated {len(generated_files)} DAG files for project")
        return generated_files
    
    def _generate_master_orchestration_dag(self, ir_project: IRProject) -> str:
        """Generate the master DAG that coordinates package execution."""
        
        project_name = ir_project.project_name
        dag_id = f"{project_name.lower()}_master"
        
        # Build dependency mapping
        dependency_graph = ir_project.get_dependency_graph()
        
        dag_code = f'''"""Master DAG for SSIS Project: {project_name}

This DAG orchestrates the execution of multiple package DAGs based on
cross-package dependencies defined by ExecutePackageTask constraints.

Generated from SSIS project at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

# DAG configuration
DAG_CONFIG = {{
    'dag_id': '{dag_id}',
    'description': 'Master orchestration for {project_name} SSIS project',
    'schedule_interval': None,  # Triggered manually or by external system
    'start_date': days_ago(1),
    'catchup': False,
    'max_active_runs': 1,
    'tags': ['ssis-migration', 'master-dag', '{project_name.lower()}'],
}}

# Default task arguments
DEFAULT_ARGS = {{
    'owner': 'ssis-migration-tool',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

# Package DAG IDs
PACKAGE_DAGS = {{
{self._generate_package_dag_mapping(ir_project)}
}}

# Package dependencies from SSIS ExecutePackageTask
DEPENDENCIES = {{
{self._generate_dependency_mapping(ir_project)}
}}

def create_master_dag() -> DAG:
    """Create the master orchestration DAG."""
    
    dag = DAG(**DAG_CONFIG, default_args=DEFAULT_ARGS)
    
    # Start task
    start_task = DummyOperator(
        task_id='start_project_execution',
        dag=dag
    )
    
    # Create trigger tasks for each package
    package_tasks = {{}}
    sensor_tasks = {{}}
    
    for package_name, dag_id in PACKAGE_DAGS.items():
        
        # Task to trigger package DAG
        trigger_task = TriggerDagRunOperator(
            task_id=f'trigger_{{package_name.lower()}}_dag',
            trigger_dag_id=dag_id,
            conf={{'triggered_by': 'master_dag', 'project': '{project_name}'}},
            wait_for_completion=False,  # Don't block master DAG
            dag=dag
        )
        package_tasks[package_name] = trigger_task
        
        # Sensor to wait for package completion (for dependencies)
        sensor_task = ExternalTaskSensor(
            task_id=f'wait_{{package_name.lower()}}_completion',
            external_dag_id=dag_id,
            external_task_id='end_task',  # Assumes all package DAGs have end_task
            timeout=3600,  # 1 hour timeout
            poke_interval=60,  # Check every minute
            dag=dag
        )
        sensor_tasks[package_name] = sensor_task
        
        # Link trigger to sensor
        trigger_task >> sensor_task
    
    # Set up dependencies based on SSIS ExecutePackageTask relationships
    for parent_pkg, children in DEPENDENCIES.items():
        if parent_pkg in sensor_tasks:
            parent_sensor = sensor_tasks[parent_pkg]
            for child_pkg in children:
                if child_pkg in package_tasks:
                    child_trigger = package_tasks[child_pkg]
                    parent_sensor >> child_trigger
    
    # Connect entry points to start task
    entry_points = {ir_project.entry_points}
    for entry_pkg in entry_points:
        if entry_pkg in package_tasks:
            start_task >> package_tasks[entry_pkg]
    
    # End task
    end_task = DummyOperator(
        task_id='end_project_execution',
        dag=dag
    )
    
    # Connect all final sensors to end task
    for sensor_task in sensor_tasks.values():
        sensor_task >> end_task
    
    return dag

# Create the DAG instance
dag = create_master_dag()

# TODO: Configure email alerts for project-level failures
# TODO: Add project-level parameter passing between packages
# TODO: Consider adding data quality checks between package executions
# TODO: Add monitoring and logging for cross-package data dependencies
'''

        return dag_code
    
    def _generate_coordinated_package_dag(self, ir_project: IRProject, package_name: str) -> str:
        """Generate individual package DAG with coordination features."""
        
        pkg_ir = ir_project.package_irs[package_name]
        
        # Use existing single-package generator but add coordination
        from .airflow_gen import AirflowDAGGenerator
        single_dag_gen = AirflowDAGGenerator(anthropic_key=self.anthropic_key)
        
        # Generate base DAG
        base_dag_code = single_dag_gen.generate_dag(pkg_ir)
        
        # Add project coordination features
        coordination_code = f'''
# Project coordination features added by MasterDAGGenerator
import os
from airflow.models import Variable

# Project-level parameters (from SSIS project)
PROJECT_PARAMS = {{
{self._generate_project_parameters_dict(ir_project)}
}}

# Project-level connections available to this package
PROJECT_CONNECTIONS = {{
{self._generate_project_connections_dict(ir_project)}
}}

def get_project_parameter(param_name: str, default_value: Any = None) -> Any:
    """Get project parameter value, with fallback to Airflow Variable."""
    if param_name in PROJECT_PARAMS:
        return PROJECT_PARAMS[param_name]
    return Variable.get(f"project_{{param_name}}", default_var=default_value)

def get_project_connection(conn_name: str) -> str:
    """Get project connection string."""
    if conn_name in PROJECT_CONNECTIONS:
        return PROJECT_CONNECTIONS[conn_name]
    raise ValueError(f"Project connection '{{conn_name}}' not found")
'''
        
        # Insert coordination code after imports
        lines = base_dag_code.split('\\n')
        insert_point = 0
        for i, line in enumerate(lines):
            if line.startswith('from airflow') or line.startswith('import'):
                insert_point = i + 1
        
        lines.insert(insert_point + 1, coordination_code)
        
        return '\\n'.join(lines)
    
    def _generate_package_dag_mapping(self, ir_project: IRProject) -> str:
        """Generate the package DAG ID mapping."""
        mappings = []
        for pkg_name in ir_project.package_irs.keys():
            dag_id = f"{pkg_name.lower()}_dag"
            mappings.append(f"    '{pkg_name}': '{dag_id}',")
        return '\\n'.join(mappings)
    
    def _generate_dependency_mapping(self, ir_project: IRProject) -> str:
        """Generate the dependency mapping for the master DAG."""
        dependency_graph = ir_project.get_dependency_graph()
        mappings = []
        
        for parent_pkg, children in dependency_graph.items():
            if children:  # Only include packages that have dependencies
                child_list = [f"'{child}'" for child in children]
                mappings.append(f"    '{parent_pkg}': [{', '.join(child_list)}],")
        
        return '\\n'.join(mappings) if mappings else "    # No cross-package dependencies found"
    
    def _generate_project_parameters_dict(self, ir_project: IRProject) -> str:
        """Generate project parameters dictionary."""
        params = []
        for param in ir_project.project_parameters:
            if not param.sensitive:  # Don't include sensitive params in code
                params.append(f"    '{param.name}': '{param.value}',")
            else:
                params.append(f"    '{param.name}': Variable.get('project_{param.name}'),")
        
        return '\\n'.join(params) if params else "    # No project parameters defined"
    
    def _generate_project_connections_dict(self, ir_project: IRProject) -> str:
        """Generate project connections dictionary."""
        connections = []
        for conn in ir_project.project_connections:
            # Connection strings should come from Airflow connections, not hardcoded
            connections.append(f"    '{conn.name}': '{{{{ conn.{conn.name}.get_uri() }}}}',")
        
        return '\\n'.join(connections) if connections else "    # No project connections defined"
    
    def _generate_project_connections_script(self, ir_project: IRProject) -> str:
        """Generate script to set up project-level Airflow connections."""
        
        return f'''#!/usr/bin/env python3
"""Setup script for {ir_project.project_name} project connections.

This script creates Airflow connections for all project-level connection managers.
Run this script after deploying the DAGs to your Airflow environment.

Usage:
    python {ir_project.project_name.lower()}_connections.py
"""
import os
import sys
from airflow.models import Connection
from airflow.utils.db import create_session

def create_project_connections():
    """Create all project-level connections in Airflow."""
    
    connections = [
{self._generate_connection_definitions(ir_project)}
    ]
    
    with create_session() as session:
        for conn_data in connections:
            # Check if connection already exists
            existing = session.query(Connection).filter(
                Connection.conn_id == conn_data['conn_id']
            ).first()
            
            if existing:
                print(f"Connection '{{conn_data['conn_id']}}' already exists, skipping")
                continue
            
            # Create new connection
            conn = Connection(
                conn_id=conn_data['conn_id'],
                conn_type=conn_data['conn_type'],
                host=conn_data.get('host'),
                schema=conn_data.get('schema'),
                login=conn_data.get('login'),
                password=conn_data.get('password'),
                port=conn_data.get('port'),
                extra=conn_data.get('extra')
            )
            
            session.add(conn)
            print(f"Created connection: {{conn_data['conn_id']}}")
        
        session.commit()
        print(f"Setup completed for {len(connections)} connections")

if __name__ == '__main__':
    print(f"Setting up connections for project: {ir_project.project_name}")
    create_project_connections()
'''
    
    def _generate_connection_definitions(self, ir_project: IRProject) -> str:
        """Generate connection definitions for the setup script."""
        connections = []
        
        for conn in ir_project.project_connections:
            conn_def = f'''        {{
            'conn_id': '{conn.name}',
            'conn_type': '{self._map_conn_type_to_airflow(conn.type)}',
            'host': os.getenv('{conn.name.upper()}_HOST', 'localhost'),
            'schema': os.getenv('{conn.name.upper()}_SCHEMA', ''),
            'login': os.getenv('{conn.name.upper()}_USER', ''),
            'password': os.getenv('{conn.name.upper()}_PASSWORD', ''),
            'port': os.getenv('{conn.name.upper()}_PORT'),
            'extra': '{{}}',
        }},'''
            connections.append(conn_def)
        
        return '\\n'.join(connections) if connections else "        # No project connections to create"
    
    def _map_conn_type_to_airflow(self, conn_type) -> str:
        """Map SSIS connection type to Airflow connection type."""
        mapping = {
            'OLEDB': 'mssql',
            'ADONET': 'mssql', 
            'FLATFILE': 'fs',
            'SNOWFLAKE': 'snowflake',
            'HTTP': 'http',
            'FTP': 'ftp',
            'SMTP': 'email'
        }
        return mapping.get(str(conn_type), 'generic')
    
    def _generate_requirements_txt(self, ir_project: IRProject) -> str:
        """Generate requirements.txt for the project DAGs."""
        
        base_requirements = [
            "apache-airflow>=2.8.0",
            "apache-airflow-providers-snowflake>=5.0.0",
            "apache-airflow-providers-microsoft-mssql>=3.0.0",
            "apache-airflow-providers-ftp>=3.0.0",
            "apache-airflow-providers-http>=4.0.0",
            "apache-airflow-providers-email>=1.0.0",
        ]
        
        # Add dbt requirements if we have transformation packages
        transform_packages = ir_project.get_transformation_packages()
        if transform_packages:
            base_requirements.extend([
                "dbt-core>=1.7.0",
                "dbt-snowflake>=1.7.0",
                "apache-airflow-providers-dbt-cloud>=3.0.0"
            ])
        
        # Add project-specific requirements based on connection types
        for conn in ir_project.project_connections:
            if str(conn.type) == 'SNOWFLAKE':
                if "snowflake-connector-python" not in base_requirements:
                    base_requirements.append("snowflake-connector-python>=3.0.0")
        
        return '\\n'.join(sorted(base_requirements))