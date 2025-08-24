"""Test SQL dialect conversion."""
import pytest
from generators.sql_converter import SQLDialectConverter


def test_sql_converter_initialization():
    """Test SQL converter initialization."""
    converter = SQLDialectConverter()
    assert converter.function_mappings is not None
    assert converter.datatype_mappings is not None


def test_tsql_function_conversion():
    """Test T-SQL function conversion to Snowflake."""
    converter = SQLDialectConverter()
    
    # Test GETDATE() conversion
    sql = "SELECT GETDATE() as CurrentTime"
    converted = converter.convert_to_snowflake(sql, "tsql")
    assert "CURRENT_TIMESTAMP()" in converted
    assert "GETDATE()" not in converted
    
    # Test ISNULL conversion
    sql = "SELECT ISNULL(col1, 'default') FROM table1"
    converted = converter.convert_to_snowflake(sql, "tsql")
    assert "COALESCE" in converted
    assert "ISNULL" not in converted
    
    # Test LEN conversion
    sql = "SELECT LEN(column_name) FROM table1"
    converted = converter.convert_to_snowflake(sql, "tsql")
    assert "LENGTH" in converted


def test_top_clause_conversion():
    """Test TOP clause conversion to LIMIT."""
    converter = SQLDialectConverter()
    
    sql = "SELECT TOP 10 * FROM Orders ORDER BY OrderDate"
    converted = converter.convert_to_snowflake(sql, "tsql")
    
    assert "TOP 10" not in converted
    assert "LIMIT 10" in converted


def test_temp_table_conversion():
    """Test temporary table conversion."""
    converter = SQLDialectConverter()
    
    sql = "SELECT * FROM #TempTable WHERE ID > 0"
    converted = converter.convert_to_snowflake(sql, "tsql")
    
    assert "#TempTable" not in converted
    assert "temp_TempTable" in converted


def test_select_into_conversion():
    """Test SELECT INTO conversion."""
    converter = SQLDialectConverter()
    
    sql = "SELECT col1, col2 INTO NewTable FROM SourceTable"
    converted = converter.convert_to_snowflake(sql, "tsql")
    
    assert "SELECT INTO" not in converted.upper()
    assert "CREATE TABLE" in converted


def test_identity_conversion():
    """Test IDENTITY column conversion."""
    converter = SQLDialectConverter()
    
    sql = "CREATE TABLE test (ID INT IDENTITY(1,1), Name VARCHAR(50))"
    converted = converter.convert_to_snowflake(sql, "tsql")
    
    assert "IDENTITY(1,1)" not in converted
    assert "AUTOINCREMENT" in converted


def test_square_bracket_conversion():
    """Test square bracket identifier conversion."""
    converter = SQLDialectConverter()
    
    sql = "SELECT [Column Name] FROM [Table Name]"
    converted = converter.convert_to_snowflake(sql, "tsql")
    
    assert "[Column Name]" not in converted
    assert '"Column Name"' in converted


def test_datepart_conversion():
    """Test DATEPART function conversion."""
    converter = SQLDialectConverter()
    
    sql = "SELECT DATEPART(YEAR, OrderDate) FROM Orders"
    converted = converter.convert_to_snowflake(sql, "tsql")
    
    assert "DATEPART" not in converted
    assert "EXTRACT(YEAR FROM" in converted


def test_data_type_conversion():
    """Test data type conversion.""" 
    converter = SQLDialectConverter()
    
    sql = "CREATE TABLE test (dt DATETIME, money_col MONEY)"
    converted = converter.convert_to_snowflake(sql, "tsql")
    
    assert "DATETIME" not in converted
    assert "TIMESTAMP" in converted
    assert "MONEY" not in converted
    assert "NUMBER(19,4)" in converted


def test_string_concatenation():
    """Test string concatenation conversion."""
    converter = SQLDialectConverter()
    
    sql = "SELECT 'Hello' + ' ' + 'World'"
    converted = converter.convert_to_snowflake(sql, "tsql")
    
    assert "||" in converted or "CONCAT" in converted


def test_conversion_notes():
    """Test that conversion notes are generated."""
    converter = SQLDialectConverter()
    
    sql = "SELECT TOP 5 GETDATE(), ISNULL(col1, 'default') FROM #temp"
    converted = converter.convert_to_snowflake(sql, "tsql")
    
    notes = converter.get_conversion_notes()
    assert len(notes) > 0
    assert any("GETDATE" in note or "TOP" in note for note in notes)


def test_snowflake_compatibility_validation():
    """Test Snowflake compatibility validation."""
    converter = SQLDialectConverter()
    
    # SQL with potential issues
    sql = "SELECT GETDATE() FROM #temp WHERE @variable = 1"
    issues = converter.validate_snowflake_compatibility(sql)
    
    assert len(issues) > 0
    assert any("GETDATE" in issue for issue in issues)
    assert any("temp" in issue.lower() for issue in issues)


def test_no_conversion_needed():
    """Test SQL that doesn't need conversion."""
    converter = SQLDialectConverter()
    
    sql = "SELECT col1, col2 FROM table1 WHERE col3 > 100"
    converted = converter.convert_to_snowflake(sql, "ansi")
    
    # Should be mostly unchanged (just cleanup)
    assert "SELECT" in converted
    assert "FROM table1" in converted
    assert "WHERE col3" in converted