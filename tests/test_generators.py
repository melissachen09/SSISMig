"""Test generator functionality."""
import pytest
import json
from pathlib import Path
from generators import AirflowDAGGenerator, DBTProjectGenerator
from models.ir import IRPackage, Executable, ExecutableType, DataFlowComponent, ComponentType


@pytest.fixture
def sample_ir_package():
    """Create a sample IR package for testing."""
    return IRPackage(
        package_name="TestPackage",
        executables=[
            Executable(
                id="Package\\ExecuteSQL1",
                type=ExecutableType.EXECUTE_SQL,
                object_name="Clean Data",
                sql="DELETE FROM staging_table WHERE created_date < DATEADD(day, -30, GETDATE())"
            ),
            Executable(
                id="Package\\DataFlow1",
                type=ExecutableType.DATA_FLOW,
                object_name="Load Orders",
                components=[
                    DataFlowComponent(
                        id="src1",
                        component_type=ComponentType.OLEDB_SOURCE,
                        name="Orders Source",
                        sql="SELECT * FROM raw_orders WHERE order_date >= ?",
                        outputs=["path1"]
                    ),
                    DataFlowComponent(
                        id="transform1",
                        component_type=ComponentType.DERIVED_COLUMN,
                        name="Add Calculated Fields",
                        expression="order_total * 1.1",
                        inputs=["path1"],
                        outputs=["path2"]
                    ),
                    DataFlowComponent(
                        id="dest1",
                        component_type=ComponentType.OLEDB_DESTINATION,
                        name="Orders Destination",
                        table="processed_orders",
                        inputs=["path2"]
                    )
                ]
            )
        ]
    )


@pytest.fixture
def transform_only_package():
    """Create a transformation-only package for dbt testing."""
    return IRPackage(
        package_name="TransformPackage",
        executables=[
            Executable(
                id="Package\\DataFlow1",
                type=ExecutableType.DATA_FLOW,
                object_name="Transform Customer Data",
                components=[
                    DataFlowComponent(
                        id="src1",
                        component_type=ComponentType.OLEDB_SOURCE,
                        name="Customer Source",
                        sql="SELECT customer_id, customer_name, created_date FROM customers"
                    ),
                    DataFlowComponent(
                        id="dest1",
                        component_type=ComponentType.OLEDB_DESTINATION,
                        name="Customer Mart",
                        table="dim_customer"
                    )
                ]
            )
        ]
    )


def test_airflow_generator_initialization():
    """Test Airflow generator initialization."""
    generator = AirflowDAGGenerator()
    assert generator is not None
    assert generator.sql_converter is not None


def test_airflow_dag_generation(sample_ir_package, temp_dir):
    """Test Airflow DAG generation."""
    generator = AirflowDAGGenerator()
    result = generator.generate_dag(sample_ir_package, str(temp_dir))
    
    assert result["success"] == True
    assert result["dag_file"] is not None
    
    # Check that DAG file was created
    dag_file = Path(result["dag_file"])
    assert dag_file.exists()
    
    # Check DAG file content
    dag_content = dag_file.read_text(encoding='utf-8')
    assert "TestPackage" in dag_content.lower() or "testpackage" in dag_content
    assert "SnowflakeOperator" in dag_content or "snowflake" in dag_content.lower()
    assert "TaskGroup" in dag_content or "task_group" in dag_content.lower()


def test_airflow_helper_files(sample_ir_package, temp_dir):
    """Test Airflow helper file generation."""
    generator = AirflowDAGGenerator()
    result = generator.generate_dag(sample_ir_package, str(temp_dir))
    
    assert len(result["helper_files"]) > 0
    
    # Check for requirements.txt
    req_files = [f for f in result["helper_files"] if "requirements.txt" in f]
    assert len(req_files) > 0
    
    req_file = Path(req_files[0])
    assert req_file.exists()
    
    req_content = req_file.read_text()
    assert "apache-airflow" in req_content
    assert "snowflake" in req_content.lower()


def test_dbt_generator_initialization():
    """Test dbt generator initialization."""
    generator = DBTProjectGenerator()
    assert generator is not None
    assert generator.sql_converter is not None


def test_dbt_project_generation(transform_only_package, temp_dir):
    """Test dbt project generation."""
    generator = DBTProjectGenerator()
    result = generator.generate_project(transform_only_package, str(temp_dir))
    
    assert result["success"] == True
    assert len(result["project_files"]) > 0
    
    # Check for dbt_project.yml
    project_files = [f for f in result["project_files"] if "dbt_project.yml" in f]
    assert len(project_files) > 0
    
    project_file = Path(project_files[0])
    assert project_file.exists()
    
    # Check project file content
    project_content = project_file.read_text()
    assert "name:" in project_content
    assert "models:" in project_content


def test_dbt_model_generation(transform_only_package, temp_dir):
    """Test dbt model file generation."""
    generator = DBTProjectGenerator()
    result = generator.generate_project(transform_only_package, str(temp_dir))
    
    # Check for SQL model files
    sql_files = [f for f in result["project_files"] if f.endswith(".sql")]
    assert len(sql_files) > 0
    
    # Check model content
    for sql_file_path in sql_files:
        sql_file = Path(sql_file_path)
        assert sql_file.exists()
        
        sql_content = sql_file.read_text()
        assert "select" in sql_content.lower() or "SELECT" in sql_content


def test_dbt_schema_yml_generation(transform_only_package, temp_dir):
    """Test schema.yml generation."""
    generator = DBTProjectGenerator()
    result = generator.generate_project(transform_only_package, str(temp_dir))
    
    # Check for schema.yml
    schema_files = [f for f in result["project_files"] if "schema.yml" in f]
    assert len(schema_files) > 0
    
    schema_file = Path(schema_files[0])
    assert schema_file.exists()
    
    schema_content = schema_file.read_text()
    assert "version:" in schema_content
    assert "sources:" in schema_content or "models:" in schema_content


def test_transformation_filter(sample_ir_package):
    """Test filtering to transformation-only tasks."""
    generator = DBTProjectGenerator()
    
    # Sample package has mixed tasks, should filter to transformations
    transform_ir = generator._filter_to_transformations(sample_ir_package)
    
    assert "executables" in transform_ir
    # Should filter out non-transformation tasks
    exe_types = [exe["type"] for exe in transform_ir["executables"]]
    assert all(exe_type in ["DataFlow", "ExecuteSQL"] for exe_type in exe_types)


def test_sql_conversion_in_generators(sample_ir_package, temp_dir):
    """Test that generators convert SQL dialects."""
    generator = AirflowDAGGenerator()
    result = generator.generate_dag(sample_ir_package, str(temp_dir))
    
    assert result["success"] == True
    
    # Check that generated DAG has converted SQL (GETDATE -> CURRENT_TIMESTAMP)
    dag_file = Path(result["dag_file"])
    dag_content = dag_file.read_text()
    
    # Should not contain T-SQL functions
    assert "GETDATE()" not in dag_content
    # Should contain Snowflake equivalents or conversion comments
    assert "CURRENT_TIMESTAMP" in dag_content or "TODO" in dag_content