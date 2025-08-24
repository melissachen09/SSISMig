"""Test IR data models."""
import pytest
from models.ir import (
    IRPackage, Executable, ExecutableType, DataFlowComponent, ComponentType,
    PrecedenceEdge, PrecedenceCondition, ConnectionManager, ConnectionType,
    Parameter, Variable, Expression
)
from models import validate_ir_package, ir_to_json, ir_from_json


def test_ir_package_creation():
    """Test creating an IR package."""
    package = IRPackage(
        package_name="TestPackage",
        parameters=[Parameter(name="p1", type="String", value="test")],
        variables=[Variable(name="v1", type="Int32", value=0)]
    )
    
    assert package.package_name == "TestPackage"
    assert len(package.parameters) == 1
    assert len(package.variables) == 1
    assert package.is_transformation_only() == True  # No ingestion tasks


def test_executable_types():
    """Test executable type handling."""
    exe = Executable(
        id="Package\\Test1",
        type=ExecutableType.EXECUTE_SQL,
        object_name="Test SQL Task",
        sql="SELECT 1"
    )
    
    assert exe.type == ExecutableType.EXECUTE_SQL
    assert exe.sql == "SELECT 1"


def test_dataflow_component():
    """Test data flow component creation."""
    component = DataFlowComponent(
        id="src1",
        component_type=ComponentType.OLEDB_SOURCE,
        name="Source",
        sql="SELECT * FROM Orders",
        outputs=["path1"]
    )
    
    assert component.component_type == ComponentType.OLEDB_SOURCE
    assert "path1" in component.outputs


def test_precedence_edge():
    """Test precedence constraint edge."""
    edge = PrecedenceEdge(
        from_task="Package\\Task1",
        to_task="Package\\Task2",
        condition=PrecedenceCondition.SUCCESS
    )
    
    assert edge.condition == PrecedenceCondition.SUCCESS
    assert edge.logical_and == True


def test_ir_json_serialization():
    """Test IR package JSON serialization."""
    package = IRPackage(
        package_name="SerializationTest",
        executables=[
            Executable(
                id="Package\\Task1",
                type=ExecutableType.EXECUTE_SQL,
                object_name="SQL Task",
                sql="SELECT GETDATE()"
            )
        ]
    )
    
    # Test serialization
    json_str = ir_to_json(package)
    assert "SerializationTest" in json_str
    assert "ExecuteSQL" in json_str
    
    # Test deserialization
    package2 = ir_from_json(json_str)
    assert package2.package_name == "SerializationTest"
    assert len(package2.executables) == 1


def test_transformation_only_detection():
    """Test transformation-only package detection."""
    # Package with only SQL transformations
    transform_package = IRPackage(
        package_name="TransformOnly",
        executables=[
            Executable(
                id="Package\\DataFlow1",
                type=ExecutableType.DATA_FLOW,
                object_name="Transform Data",
                components=[
                    DataFlowComponent(
                        id="src1",
                        component_type=ComponentType.OLEDB_SOURCE,
                        name="DB Source"
                    ),
                    DataFlowComponent(
                        id="dest1", 
                        component_type=ComponentType.OLEDB_DESTINATION,
                        name="DB Destination"
                    )
                ]
            )
        ]
    )
    
    assert transform_package.is_transformation_only() == True
    
    # Package with file ingestion
    ingestion_package = IRPackage(
        package_name="WithIngestion",
        executables=[
            Executable(
                id="Package\\DataFlow1",
                type=ExecutableType.DATA_FLOW,
                object_name="File Load",
                components=[
                    DataFlowComponent(
                        id="src1",
                        component_type=ComponentType.FLAT_FILE_SOURCE,
                        name="File Source"
                    )
                ]
            )
        ]
    )
    
    assert ingestion_package.is_transformation_only() == False


def test_get_data_flows():
    """Test getting data flow tasks."""
    package = IRPackage(
        package_name="TestFlows",
        executables=[
            Executable(
                id="Package\\SQL1",
                type=ExecutableType.EXECUTE_SQL,
                object_name="SQL Task"
            ),
            Executable(
                id="Package\\DF1",
                type=ExecutableType.DATA_FLOW,
                object_name="Data Flow 1"
            ),
            Executable(
                id="Package\\DF2", 
                type=ExecutableType.DATA_FLOW,
                object_name="Data Flow 2"
            )
        ]
    )
    
    data_flows = package.get_data_flows()
    assert len(data_flows) == 2
    assert all(exe.type == ExecutableType.DATA_FLOW for exe in data_flows)


def test_ir_validation():
    """Test IR package validation."""
    # Valid package
    valid_package = IRPackage(package_name="Valid")
    validate_ir_package(valid_package.model_dump())  # Should not raise
    
    # Invalid package data
    invalid_data = {"package_name": "", "invalid_field": "value"}
    
    with pytest.raises(Exception):  # Validation should fail
        validate_ir_package(invalid_data)