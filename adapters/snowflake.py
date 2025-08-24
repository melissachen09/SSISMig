"""Snowflake-specific adapters and utilities.

Provides Snowflake-specific functionality for the migration tool.
"""
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class SnowflakeAdapter:
    """Adapter for Snowflake-specific functionality."""
    
    def __init__(self, connection_params: Optional[Dict[str, str]] = None):
        """Initialize Snowflake adapter.
        
        Args:
            connection_params: Optional Snowflake connection parameters
        """
        self.connection_params = connection_params or {}
    
    def generate_connection_config(self, ssis_connections: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate Snowflake connection configuration.
        
        Args:
            ssis_connections: List of SSIS connection managers
            
        Returns:
            Snowflake connection configuration
        """
        # Find Snowflake connections from SSIS
        snowflake_connections = [
            conn for conn in ssis_connections
            if 'snowflake' in conn.get('type', '').lower()
        ]
        
        config = {
            "connections": {},
            "profiles": {},
            "warehouses": set(),
            "databases": set(),
            "schemas": set()
        }
        
        for conn in snowflake_connections:
            conn_name = conn.get('name', conn.get('id', 'default'))
            properties = conn.get('properties', {})
            
            # Extract Snowflake-specific properties
            account = properties.get('account', 'YOUR_ACCOUNT')
            database = properties.get('database', 'YOUR_DATABASE')
            warehouse = properties.get('warehouse', 'YOUR_WAREHOUSE')
            schema = properties.get('schema', 'PUBLIC')
            
            config["connections"][conn_name] = {
                "account": account,
                "database": database,
                "warehouse": warehouse,
                "schema": schema,
                "role": properties.get('role', 'PUBLIC'),
                "user": "{{ env_var('SNOWFLAKE_USER') }}",
                "password": "{{ env_var('SNOWFLAKE_PASSWORD') }}"
            }
            
            # Collect unique values for validation
            if account != 'YOUR_ACCOUNT':
                config["warehouses"].add(warehouse)
                config["databases"].add(database)  
                config["schemas"].add(schema)
        
        # Convert sets to lists for JSON serialization
        config["warehouses"] = list(config["warehouses"])
        config["databases"] = list(config["databases"])
        config["schemas"] = list(config["schemas"])
        
        return config
    
    def generate_stage_sql(self, table_name: str, file_format: str = "CSV") -> str:
        """Generate SQL for creating Snowflake stages.
        
        Args:
            table_name: Target table name
            file_format: File format (CSV, JSON, PARQUET, etc.)
            
        Returns:
            SQL for stage creation
        """
        stage_name = f"STG_{table_name.upper()}"
        
        format_options = {
            "CSV": "TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1",
            "JSON": "TYPE = 'JSON'",
            "PARQUET": "TYPE = 'PARQUET'",
            "DELIMITED": "TYPE = 'CSV' FIELD_DELIMITER = '|'"
        }
        
        format_sql = format_options.get(file_format.upper(), format_options["CSV"])
        
        return f"""-- Create stage for {table_name}
CREATE STAGE IF NOT EXISTS {stage_name}
FILE_FORMAT = (
    {format_sql}
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
);

-- Example COPY INTO command
-- COPY INTO {table_name}
-- FROM @{stage_name}
-- PATTERN = '.*\\.{file_format.lower()}'
-- ON_ERROR = 'CONTINUE';
"""
    
    def generate_merge_sql(self, target_table: str, source_query: str, 
                          key_columns: List[str], update_columns: List[str]) -> str:
        """Generate Snowflake MERGE statement.
        
        Args:
            target_table: Target table name
            source_query: Source query or table reference
            key_columns: Columns for matching (join keys)
            update_columns: Columns to update
            
        Returns:
            MERGE SQL statement
        """
        # Build join condition
        join_conditions = [f"target.{col} = source.{col}" for col in key_columns]
        join_clause = " AND ".join(join_conditions)
        
        # Build update clause
        update_assignments = [f"{col} = source.{col}" for col in update_columns]
        update_clause = ", ".join(update_assignments)
        
        # Build insert columns and values
        all_columns = key_columns + update_columns
        insert_columns = ", ".join(all_columns)
        insert_values = ", ".join([f"source.{col}" for col in all_columns])
        
        return f"""MERGE INTO {target_table} AS target
USING (
    {source_query}
) AS source
ON {join_clause}
WHEN MATCHED THEN
    UPDATE SET {update_clause}
WHEN NOT MATCHED THEN
    INSERT ({insert_columns})
    VALUES ({insert_values});
"""
    
    def optimize_table_ddl(self, ddl: str) -> str:
        """Optimize table DDL for Snowflake.
        
        Args:
            ddl: Original DDL statement
            
        Returns:
            Optimized DDL for Snowflake
        """
        optimizations = []
        optimized_ddl = ddl
        
        # Add clustering keys for large tables (heuristic based)
        if "ORDER_DATE" in ddl.upper() or "TRANSACTION_DATE" in ddl.upper():
            optimizations.append("Consider adding clustering key on date columns")
        
        # Suggest data retention for staging tables
        if "STG_" in ddl.upper() or "STAGING" in ddl.upper():
            optimizations.append("Consider setting DATA_RETENTION_TIME_IN_DAYS = 1 for staging tables")
        
        # Add optimization comments
        if optimizations:
            comment_block = "-- Snowflake Optimization Suggestions:\n"
            comment_block += "\n".join([f"-- {opt}" for opt in optimizations])
            optimized_ddl = comment_block + "\n\n" + optimized_ddl
        
        return optimized_ddl
    
    def generate_monitoring_queries(self, table_names: List[str]) -> str:
        """Generate monitoring queries for migrated tables.
        
        Args:
            table_names: List of table names to monitor
            
        Returns:
            SQL queries for monitoring
        """
        queries = []
        
        # Row count monitoring
        queries.append("-- Row count monitoring")
        for table in table_names:
            queries.append(f"SELECT '{table}' AS table_name, COUNT(*) AS row_count FROM {table};")
        
        queries.append("\n-- Data freshness check")
        for table in table_names:
            queries.append(f"""SELECT '{table}' AS table_name, 
       MAX(UPDATED_AT) AS last_updated,
       DATEDIFF(HOUR, MAX(UPDATED_AT), CURRENT_TIMESTAMP()) AS hours_since_update
FROM {table}
WHERE UPDATED_AT IS NOT NULL;""")
        
        queries.append("\n-- Storage usage")
        queries.append("""SELECT table_name, 
       row_count,
       bytes,
       bytes / 1024 / 1024 / 1024 AS size_gb
FROM information_schema.tables 
WHERE table_schema = CURRENT_SCHEMA()
  AND table_name IN ('{}')
ORDER BY bytes DESC;""".format("', '".join(table_names)))
        
        return "\n".join(queries)
    
    def validate_schema_migration(self, source_schema: Dict[str, Any], 
                                 target_schema: Dict[str, Any]) -> List[str]:
        """Validate schema migration between source and target.
        
        Args:
            source_schema: Source schema definition
            target_schema: Target schema definition
            
        Returns:
            List of validation issues
        """
        issues = []
        
        source_tables = set(source_schema.get('tables', {}).keys())
        target_tables = set(target_schema.get('tables', {}).keys())
        
        # Check for missing tables
        missing_tables = source_tables - target_tables
        if missing_tables:
            issues.append(f"Missing tables in target: {', '.join(missing_tables)}")
        
        # Check for extra tables
        extra_tables = target_tables - source_tables
        if extra_tables:
            issues.append(f"Extra tables in target: {', '.join(extra_tables)}")
        
        # Check column mappings for common tables
        common_tables = source_tables & target_tables
        for table in common_tables:
            source_cols = set(source_schema['tables'][table].get('columns', {}).keys())
            target_cols = set(target_schema['tables'][table].get('columns', {}).keys())
            
            missing_cols = source_cols - target_cols
            if missing_cols:
                issues.append(f"Table {table} missing columns: {', '.join(missing_cols)}")
        
        return issues
    
    def generate_data_quality_checks(self, table_name: str, 
                                    key_columns: List[str] = None) -> str:
        """Generate data quality check queries.
        
        Args:
            table_name: Table name to check
            key_columns: Key columns for uniqueness checks
            
        Returns:
            Data quality check SQL
        """
        checks = [
            f"-- Data Quality Checks for {table_name}",
            "",
            "-- Null value check",
            f"SELECT column_name, COUNT(*) AS null_count",
            f"FROM information_schema.columns c",
            f"LEFT JOIN {table_name} t ON 1=1 AND t.column_name IS NULL", 
            f"WHERE c.table_name = '{table_name.upper()}'",
            f"GROUP BY column_name;",
            "",
            "-- Row count and basic stats",
            f"SELECT COUNT(*) AS total_rows,",
            f"       COUNT(DISTINCT {key_columns[0] if key_columns else '1'}) AS unique_keys,",
            f"       MIN(created_date) AS earliest_record,", 
            f"       MAX(created_date) AS latest_record",
            f"FROM {table_name};",
        ]
        
        if key_columns:
            checks.extend([
                "",
                "-- Duplicate key check", 
                f"SELECT {', '.join(key_columns)}, COUNT(*) AS duplicate_count",
                f"FROM {table_name}",
                f"GROUP BY {', '.join(key_columns)}",
                f"HAVING COUNT(*) > 1",
                f"ORDER BY duplicate_count DESC;",
            ])
        
        return "\n".join(checks)