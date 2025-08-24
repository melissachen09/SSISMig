"""SQL dialect converter for T-SQL to Snowflake conversion.

Converts T-SQL statements to Snowflake-compatible SQL according to 
section 9 of the migration plan.
"""
import re
import logging
from typing import Dict, List, Tuple, Optional
import sqlparse
from sqlparse import sql, tokens

logger = logging.getLogger(__name__)


class SQLDialectConverter:
    """Converter for SQL dialect transformations."""
    
    def __init__(self):
        """Initialize the SQL converter with conversion rules."""
        # Function mappings from T-SQL to Snowflake
        self.function_mappings = {
            'GETDATE()': 'CURRENT_TIMESTAMP()',
            'GETUTCDATE()': 'CURRENT_TIMESTAMP()',
            'SYSDATETIME()': 'CURRENT_TIMESTAMP()',
            'SYSUTCDATETIME()': 'CURRENT_TIMESTAMP()',
            'ISNULL': 'COALESCE',
            'LEN': 'LENGTH',
            'CHARINDEX': 'POSITION',
            'STUFF': 'INSERT',
            'PATINDEX': 'REGEXP_INSTR',
            'NEWID()': 'UUID_STRING()',
            'RAND()': 'RANDOM()',
            'CEILING': 'CEIL',
            'SQUARE': 'POW',
            'POWER': 'POW',
            'DATALENGTH': 'OCTET_LENGTH',
        }
        
        # Date function conversions
        self.date_functions = {
            'YEAR': 'EXTRACT(YEAR FROM {})',
            'MONTH': 'EXTRACT(MONTH FROM {})',
            'DAY': 'EXTRACT(DAY FROM {})',
            'DATEPART': self._convert_datepart,
            'DATEADD': self._convert_dateadd,
            'DATEDIFF': self._convert_datediff,
        }
        
        # Data type mappings
        self.datatype_mappings = {
            'DATETIME': 'TIMESTAMP',
            'DATETIME2': 'TIMESTAMP',
            'SMALLDATETIME': 'TIMESTAMP',
            'MONEY': 'NUMBER(19,4)',
            'SMALLMONEY': 'NUMBER(10,4)',
            'UNIQUEIDENTIFIER': 'VARCHAR(36)',
            'TEXT': 'VARCHAR',
            'NTEXT': 'VARCHAR',
            'IMAGE': 'BINARY',
            'TINYINT': 'NUMBER(3,0)',
            'SMALLINT': 'NUMBER(5,0)',
            'INT': 'NUMBER(10,0)',
            'INTEGER': 'NUMBER(10,0)',
            'BIGINT': 'NUMBER(19,0)',
            'REAL': 'FLOAT',
            'FLOAT': 'FLOAT',
            'NUMERIC': 'NUMBER',
            'DECIMAL': 'NUMBER',
        }
        
        self.conversion_notes = []
    
    def convert_to_snowflake(self, sql: str, source_dialect: str = "tsql") -> str:
        """Convert SQL from source dialect to Snowflake.
        
        Args:
            sql: SQL statement to convert
            source_dialect: Source dialect (tsql, ansi, etc.)
            
        Returns:
            Converted SQL statement
        """
        if not sql or not sql.strip():
            return sql
        
        logger.debug(f"Converting SQL from {source_dialect} to Snowflake")
        self.conversion_notes = []
        
        # Clean and normalize the SQL
        converted = sql.strip()
        
        # Apply conversions based on source dialect
        if source_dialect.lower() in ['tsql', 'mssql', 'sqlserver']:
            converted = self._convert_tsql_to_snowflake(converted)
        
        # Common conversions for all dialects
        converted = self._apply_common_conversions(converted)
        
        # Final cleanup
        converted = self._cleanup_sql(converted)
        
        if self.conversion_notes:
            logger.info(f"SQL conversion notes: {', '.join(self.conversion_notes)}")
        
        return converted
    
    def _convert_tsql_to_snowflake(self, sql: str) -> str:
        """Convert T-SQL specific constructs to Snowflake.
        
        Args:
            sql: T-SQL statement
            
        Returns:
            Converted SQL
        """
        converted = sql
        
        # Convert TOP clause
        converted = self._convert_top_clause(converted)
        
        # Convert temporary tables
        converted = self._convert_temp_tables(converted)
        
        # Convert SELECT INTO
        converted = self._convert_select_into(converted)
        
        # Convert IDENTITY columns
        converted = self._convert_identity(converted)
        
        # Convert OUTPUT clause
        converted = self._convert_output_clause(converted)
        
        # Convert MERGE statement differences
        converted = self._convert_merge_statement(converted)
        
        # Convert square bracket identifiers
        converted = self._convert_square_brackets(converted)
        
        # Convert variable declarations
        converted = self._convert_variable_declarations(converted)
        
        # Convert IF EXISTS patterns
        converted = self._convert_if_exists(converted)
        
        return converted
    
    def _apply_common_conversions(self, sql: str) -> str:
        """Apply common function and syntax conversions.
        
        Args:
            sql: SQL to convert
            
        Returns:
            Converted SQL
        """
        converted = sql
        
        # Convert functions
        converted = self._convert_functions(converted)
        
        # Convert date functions
        converted = self._convert_date_functions(converted)
        
        # Convert data types in CREATE/ALTER statements
        converted = self._convert_data_types(converted)
        
        # Convert string concatenation
        converted = self._convert_string_concat(converted)
        
        return converted
    
    def _convert_top_clause(self, sql: str) -> str:
        """Convert T-SQL TOP clause to LIMIT."""
        # Pattern: SELECT TOP n ... -> SELECT ... LIMIT n
        pattern = r'\bSELECT\s+TOP\s+(\d+)\s+'
        
        def replace_top(match):
            n = match.group(1)
            self.conversion_notes.append(f"Converted TOP {n} to LIMIT {n}")
            return 'SELECT '
        
        converted = re.sub(pattern, replace_top, sql, flags=re.IGNORECASE)
        
        # Add LIMIT clause if TOP was found
        if pattern in sql.upper():
            # Extract the number from the original pattern
            match = re.search(pattern, sql, re.IGNORECASE)
            if match:
                n = match.group(1)
                # Add LIMIT at the end if not already there
                if not re.search(r'\bLIMIT\s+\d+', converted, re.IGNORECASE):
                    converted = converted.rstrip(';') + f' LIMIT {n}'
        
        return converted
    
    def _convert_temp_tables(self, sql: str) -> str:
        """Convert temporary tables from #temp to proper CTEs or temp tables."""
        # Pattern: #tablename -> temp_tablename (or use CTEs)
        pattern = r'#(\w+)'
        
        def replace_temp(match):
            table_name = match.group(1)
            self.conversion_notes.append(f"Converted temp table #{table_name} to temp_{table_name}")
            return f'temp_{table_name}'
        
        return re.sub(pattern, replace_temp, sql)
    
    def _convert_select_into(self, sql: str) -> str:
        """Convert SELECT INTO to CREATE TABLE AS SELECT."""
        pattern = r'\bSELECT\b(.*?)\bINTO\s+([^\s]+)\s+FROM\b'
        
        def replace_select_into(match):
            select_clause = match.group(1).strip()
            table_name = match.group(2)
            self.conversion_notes.append(f"Converted SELECT INTO {table_name} to CREATE TABLE AS SELECT")
            return f'CREATE TABLE {table_name} AS SELECT{select_clause} FROM'
        
        return re.sub(pattern, replace_select_into, sql, flags=re.IGNORECASE | re.DOTALL)
    
    def _convert_identity(self, sql: str) -> str:
        """Convert IDENTITY columns to AUTOINCREMENT."""
        pattern = r'\bIDENTITY\s*\(\s*\d+\s*,\s*\d+\s*\)'
        
        def replace_identity(match):
            self.conversion_notes.append("Converted IDENTITY to AUTOINCREMENT")
            return 'AUTOINCREMENT'
        
        return re.sub(pattern, replace_identity, sql, flags=re.IGNORECASE)
    
    def _convert_output_clause(self, sql: str) -> str:
        """Convert OUTPUT clause - mostly remove as Snowflake doesn't support it."""
        pattern = r'\bOUTPUT\b.*?(?=\bINTO\b|\bWHERE\b|\bGROUP\s+BY\b|\bORDER\s+BY\b|$)'
        
        if re.search(pattern, sql, re.IGNORECASE):
            self.conversion_notes.append("Removed OUTPUT clause - not supported in Snowflake")
            return re.sub(pattern, '', sql, flags=re.IGNORECASE | re.DOTALL)
        
        return sql
    
    def _convert_merge_statement(self, sql: str) -> str:
        """Convert MERGE statement differences."""
        # Snowflake MERGE syntax is similar but has some differences
        # This is a simplified conversion
        if 'MERGE' in sql.upper():
            self.conversion_notes.append("MERGE statement detected - manual review recommended")
        
        return sql
    
    def _convert_square_brackets(self, sql: str) -> str:
        """Convert [identifier] to identifier or "identifier"."""
        # Pattern: [identifier] -> "identifier"
        pattern = r'\[([^\]]+)\]'
        
        def replace_brackets(match):
            identifier = match.group(1)
            # If identifier contains spaces or special chars, use double quotes
            if re.search(r'[^a-zA-Z0-9_]', identifier):
                return f'"{identifier}"'
            else:
                return identifier
        
        return re.sub(pattern, replace_brackets, sql)
    
    def _convert_variable_declarations(self, sql: str) -> str:
        """Convert T-SQL variable declarations."""
        # Pattern: DECLARE @var datatype -> SET var = value pattern
        pattern = r'\bDECLARE\s+@(\w+)\s+(\w+(?:\([^)]*\))?)'
        
        def replace_declare(match):
            var_name = match.group(1)
            data_type = match.group(2)
            snowflake_type = self.datatype_mappings.get(data_type.upper(), data_type)
            self.conversion_notes.append(f"Variable @{var_name} declared as {data_type}")
            return f'-- Variable {var_name} ({snowflake_type}) - implement as needed'
        
        return re.sub(pattern, replace_declare, sql, flags=re.IGNORECASE)
    
    def _convert_if_exists(self, sql: str) -> str:
        """Convert IF EXISTS patterns."""
        # Pattern: IF EXISTS (SELECT...) DROP TABLE... 
        pattern = r'\bIF\s+EXISTS\s*\([^)]+\)\s*DROP\s+TABLE\s+(\w+)'
        
        def replace_if_exists(match):
            table_name = match.group(1)
            self.conversion_notes.append(f"Converted IF EXISTS DROP TABLE to DROP TABLE IF EXISTS")
            return f'DROP TABLE IF EXISTS {table_name}'
        
        return re.sub(pattern, replace_if_exists, sql, flags=re.IGNORECASE | re.DOTALL)
    
    def _convert_functions(self, sql: str) -> str:
        """Convert function names."""
        converted = sql
        
        for tsql_func, snowflake_func in self.function_mappings.items():
            # Handle functions with parentheses
            if tsql_func.endswith('()'):
                pattern = r'\b' + re.escape(tsql_func[:-2]) + r'\s*\(\s*\)'
                converted = re.sub(pattern, snowflake_func, converted, flags=re.IGNORECASE)
            else:
                # Handle functions that take parameters
                pattern = r'\b' + re.escape(tsql_func) + r'\s*\('
                replacement = snowflake_func + '('
                converted = re.sub(pattern, replacement, converted, flags=re.IGNORECASE)
        
        return converted
    
    def _convert_date_functions(self, sql: str) -> str:
        """Convert date-related functions."""
        converted = sql
        
        # Convert DATEPART function
        pattern = r'\bDATEPART\s*\(\s*(\w+)\s*,\s*([^)]+)\)'
        
        def replace_datepart(match):
            part = match.group(1).upper()
            date_expr = match.group(2)
            
            part_mapping = {
                'YEAR': 'YEAR',
                'YY': 'YEAR', 
                'YYYY': 'YEAR',
                'MONTH': 'MONTH',
                'MM': 'MONTH',
                'DAY': 'DAY',
                'DD': 'DAY',
                'HOUR': 'HOUR',
                'HH': 'HOUR',
                'MINUTE': 'MINUTE',
                'MI': 'MINUTE',
                'SECOND': 'SECOND',
                'SS': 'SECOND'
            }
            
            snowflake_part = part_mapping.get(part, part)
            self.conversion_notes.append(f"Converted DATEPART({part}) to EXTRACT({snowflake_part})")
            return f'EXTRACT({snowflake_part} FROM {date_expr})'
        
        converted = re.sub(pattern, replace_datepart, converted, flags=re.IGNORECASE)
        
        return converted
    
    def _convert_datepart(self, match_obj) -> str:
        """Helper for DATEPART conversion."""
        return "EXTRACT"  # Placeholder - actual implementation in _convert_date_functions
    
    def _convert_dateadd(self, match_obj) -> str:
        """Helper for DATEADD conversion."""
        return "DATEADD"  # Snowflake supports DATEADD with similar syntax
    
    def _convert_datediff(self, match_obj) -> str:
        """Helper for DATEDIFF conversion.""" 
        return "DATEDIFF"  # Snowflake supports DATEDIFF with similar syntax
    
    def _convert_data_types(self, sql: str) -> str:
        """Convert T-SQL data types to Snowflake equivalents."""
        converted = sql
        
        for tsql_type, snowflake_type in self.datatype_mappings.items():
            # Pattern for data type declarations
            pattern = r'\b' + re.escape(tsql_type) + r'\b'
            converted = re.sub(pattern, snowflake_type, converted, flags=re.IGNORECASE)
        
        return converted
    
    def _convert_string_concat(self, sql: str) -> str:
        """Convert T-SQL string concatenation to Snowflake."""
        # Convert + operator for strings to CONCAT or ||
        # This is a simplified approach - full implementation would need proper parsing
        
        # Pattern: 'string1' + 'string2' -> 'string1' || 'string2'
        pattern = r"('[^']*')\s*\+\s*('[^']*')"
        
        def replace_concat(match):
            str1 = match.group(1)
            str2 = match.group(2)
            self.conversion_notes.append("Converted string concatenation + to ||")
            return f"{str1} || {str2}"
        
        return re.sub(pattern, replace_concat, sql)
    
    def _cleanup_sql(self, sql: str) -> str:
        """Final cleanup of the converted SQL."""
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', sql).strip()
        
        # Ensure proper line breaks for readability
        cleaned = re.sub(r'\s+(SELECT|FROM|WHERE|GROUP BY|ORDER BY|HAVING)\s+', r'\n\1 ', cleaned, flags=re.IGNORECASE)
        
        return cleaned
    
    def get_conversion_notes(self) -> List[str]:
        """Get notes about the conversions performed.
        
        Returns:
            List of conversion notes
        """
        return self.conversion_notes.copy()
    
    def validate_snowflake_compatibility(self, sql: str) -> List[str]:
        """Validate converted SQL for Snowflake compatibility issues.
        
        Args:
            sql: Converted SQL to validate
            
        Returns:
            List of potential compatibility issues
        """
        issues = []
        
        # Check for remaining T-SQL patterns
        tsql_patterns = [
            (r'\bGETDATE\s*\(\)', "GETDATE() function not converted"),
            (r'#\w+', "Temporary table reference not converted"),
            (r'@\w+', "Variable reference may need conversion"),
            (r'\bIDENTITY\s*\(', "IDENTITY not converted"),
            (r'\bOUTPUT\b', "OUTPUT clause not supported"),
            (r'\[\w+\]', "Square bracket identifiers not converted"),
        ]
        
        for pattern, message in tsql_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                issues.append(message)
        
        return issues