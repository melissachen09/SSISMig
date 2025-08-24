"""Test DTSX parser functionality."""
import pytest
from pathlib import Path
from parser import DTSXReader, DTSXParseError, DTSXToIRConverter
from models.ir import ExecutableType, ComponentType


def test_dtsx_reader_initialization():
    """Test DTSX reader initialization."""
    reader = DTSXReader("test.dtsx")
    assert reader.file_path.name == "test.dtsx"
    assert reader.password is None


def test_dtsx_reader_load_invalid_file():
    """Test loading non-existent DTSX file."""
    reader = DTSXReader("nonexistent.dtsx")
    
    with pytest.raises(DTSXParseError):
        reader.load()


def test_dtsx_reader_simple_package(temp_dir, sample_dtsx_simple):
    """Test parsing a simple DTSX package."""
    # Create temporary DTSX file
    dtsx_file = temp_dir / "simple.dtsx"
    with open(dtsx_file, 'w', encoding='utf-8') as f:
        f.write(sample_dtsx_simple)
    
    reader = DTSXReader(str(dtsx_file))
    reader.load()
    
    # Test metadata extraction
    metadata = reader.get_package_metadata()
    assert metadata["package_name"] == "TestPackage"
    assert metadata["protection_level"].value == "DontSaveSensitive"
    
    # Test variables
    variables = reader.get_variables()
    assert len(variables) == 1
    assert variables[0].name == "User::BatchID"
    
    # Test connection managers
    connections = reader.get_connection_managers()
    assert len(connections) == 1
    assert connections[0].name == "TestDB"
    assert connections[0].type.value == "OLEDB"
    
    # Test executables
    executables = reader.get_executables()
    assert len(executables) == 1
    assert executables[0].type == ExecutableType.EXECUTE_SQL
    assert "SELECT GETDATE()" in executables[0].sql


def test_dtsx_reader_with_dataflow(temp_dir, sample_dtsx_with_dataflow):
    """Test parsing DTSX with data flow."""
    dtsx_file = temp_dir / "dataflow.dtsx"
    with open(dtsx_file, 'w', encoding='utf-8') as f:
        f.write(sample_dtsx_with_dataflow)
    
    reader = DTSXReader(str(dtsx_file))
    reader.load()
    
    executables = reader.get_executables()
    assert len(executables) == 1
    assert executables[0].type == ExecutableType.DATA_FLOW


def test_dtsx_to_ir_converter(temp_dir, sample_dtsx_simple):
    """Test full DTSX to IR conversion."""
    dtsx_file = temp_dir / "convert_test.dtsx"
    with open(dtsx_file, 'w', encoding='utf-8') as f:
        f.write(sample_dtsx_simple)
    
    converter = DTSXToIRConverter()
    ir_package, report = converter.convert_file(str(dtsx_file))
    
    assert report.success == True
    assert ir_package.package_name == "TestPackage"
    assert len(ir_package.executables) == 1
    assert len(ir_package.variables) == 1
    assert len(ir_package.connection_managers) == 1


def test_ir_converter_error_handling(temp_dir):
    """Test IR converter error handling."""
    # Create invalid DTSX file
    invalid_file = temp_dir / "invalid.dtsx"
    with open(invalid_file, 'w') as f:
        f.write("invalid xml content")
    
    converter = DTSXToIRConverter()
    ir_package, report = converter.convert_file(str(invalid_file))
    
    assert report.success == False
    assert len(report.errors) > 0