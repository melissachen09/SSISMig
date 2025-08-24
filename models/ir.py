"""Intermediate Representation (IR) data models for SSIS migration.

These models define the stable JSON schema produced by the parser 
and consumed by generators as specified in section 2 of the migration plan.
"""
from typing import List, Optional, Dict, Any, Literal, Union
from pydantic import BaseModel, Field
from enum import Enum


class ProtectionLevel(str, Enum):
    """SSIS package protection levels."""
    DONT_SAVE_SENSITIVE = "DontSaveSensitive"
    ENCRYPT_SENSITIVE_WITH_PASSWORD = "EncryptSensitiveWithPassword"
    ENCRYPT_SENSITIVE_WITH_USER_KEY = "EncryptSensitiveWithUserKey"
    ENCRYPT_ALL_WITH_PASSWORD = "EncryptAllWithPassword"
    ENCRYPT_ALL_WITH_USER_KEY = "EncryptAllWithUserKey"


class ConnectionType(str, Enum):
    """Supported connection manager types."""
    OLEDB = "OLEDB"
    ADONET = "ADO.NET"
    FLATFILE = "FLATFILE"
    SNOWFLAKE = "SNOWFLAKE"
    FILE = "FILE"
    HTTP = "HTTP"
    FTP = "FTP"
    SMTP = "SMTP"


class ExecutableType(str, Enum):
    """SSIS executable task types."""
    EXECUTE_SQL = "ExecuteSQL"
    DATA_FLOW = "DataFlow"
    SCRIPT_TASK = "ScriptTask"
    SEQUENCE_CONTAINER = "SequenceContainer"
    FOREACH_LOOP = "ForEachLoop"
    FOR_LOOP = "ForLoop"
    BULK_INSERT = "BulkInsert"
    EXECUTE_PACKAGE = "ExecutePackage"
    FILE_SYSTEM = "FileSystem"
    FTP = "FTP"
    SEND_MAIL = "SendMail"
    WEB_SERVICE = "WebService"


class ComponentType(str, Enum):
    """Data flow component types."""
    OLEDB_SOURCE = "OLEDBSource"
    ADONET_SOURCE = "ADONETSource"
    FLAT_FILE_SOURCE = "FlatFileSource"
    SCRIPT_SOURCE = "ScriptSource"
    OLEDB_DESTINATION = "OLEDBDestination"
    ADONET_DESTINATION = "ADONETDestination"
    FLAT_FILE_DESTINATION = "FlatFileDestination"
    SNOWFLAKE_DEST = "SnowflakeDest"
    DERIVED_COLUMN = "DerivedColumn"
    LOOKUP = "Lookup"
    CONDITIONAL_SPLIT = "ConditionalSplit"
    UNION_ALL = "UnionAll"
    SORT = "Sort"
    AGGREGATE = "Aggregate"
    MERGE_JOIN = "MergeJoin"
    MULTICAST = "Multicast"
    ROW_COUNT = "RowCount"
    SCRIPT_COMPONENT = "ScriptComponent"


class PrecedenceCondition(str, Enum):
    """Precedence constraint conditions."""
    SUCCESS = "Success"
    FAILURE = "Failure"
    COMPLETION = "Completion"
    EXPRESSION = "Expression"


class SqlDialect(str, Enum):
    """SQL dialect indicators."""
    TSQL = "tsql"
    ANSI = "ansi"
    SNOWFLAKE = "snowflake"


class Parameter(BaseModel):
    """Package parameter definition."""
    name: str
    type: str = Field(description="Data type (e.g., DateTime, Int32, String)")
    value: Optional[Any] = None
    description: Optional[str] = None


class Variable(BaseModel):
    """Package variable definition."""
    name: str
    type: str = Field(description="Data type (e.g., DateTime, Int32, String)")
    value: Optional[Any] = None
    description: Optional[str] = None
    scope: str = "Package"


class ConnectionManager(BaseModel):
    """Connection manager definition."""
    id: str = Field(description="Unique identifier")
    name: str = Field(description="Display name")
    type: ConnectionType
    properties: Dict[str, Any] = Field(default_factory=dict, description="Connection properties")
    sensitive: bool = False
    description: Optional[str] = None


class DataFlowComponent(BaseModel):
    """Data flow pipeline component."""
    id: str = Field(description="Component ID within the data flow")
    component_type: ComponentType
    name: str = Field(description="Component display name")
    sql: Optional[str] = None
    table: Optional[str] = None
    expression: Optional[str] = None
    properties: Dict[str, Any] = Field(default_factory=dict)
    inputs: List[str] = Field(default_factory=list, description="Input path IDs")
    outputs: List[str] = Field(default_factory=list, description="Output path IDs")
    join_on: List[str] = Field(default_factory=list, description="Join keys for lookup/merge")
    ref: Optional[str] = Field(description="Reference table for lookup")
    mode: Optional[str] = Field(description="Insert mode: append, merge, truncate")


class Executable(BaseModel):
    """SSIS executable task."""
    id: str = Field(description="Unique task ID (e.g., Package\\ExecSQL1)")
    type: ExecutableType
    object_name: str = Field(description="Display name")
    dialect: Optional[SqlDialect] = None
    sql: Optional[str] = None
    parameter_refs: List[str] = Field(default_factory=list)
    variable_refs: List[str] = Field(default_factory=list)
    components: List[DataFlowComponent] = Field(default_factory=list, description="For DataFlow tasks")
    properties: Dict[str, Any] = Field(default_factory=dict)
    connection_ref: Optional[str] = None
    delay_validation: bool = False
    disabled: bool = False


class PrecedenceEdge(BaseModel):
    """Precedence constraint edge."""
    from_task: str = Field(description="Source task ID")
    to_task: str = Field(description="Target task ID")
    condition: PrecedenceCondition
    expression: Optional[str] = Field(description="Expression for conditional precedence")
    logical_and: bool = True


class Expression(BaseModel):
    """Property expression override."""
    scope: str = Field(description="Task or package scope")
    property: str = Field(description="Property name")
    expression: str = Field(description="SSIS expression")


class IRPackage(BaseModel):
    """Complete Intermediate Representation of an SSIS package."""
    package_name: str
    protection_level: ProtectionLevel = ProtectionLevel.DONT_SAVE_SENSITIVE
    parameters: List[Parameter] = Field(default_factory=list)
    variables: List[Variable] = Field(default_factory=list)
    connection_managers: List[ConnectionManager] = Field(default_factory=list)
    executables: List[Executable] = Field(default_factory=list)
    edges: List[PrecedenceEdge] = Field(default_factory=list)
    expressions: List[Expression] = Field(default_factory=list)
    version_build: Optional[str] = None
    version_comments: Optional[str] = None
    creator_name: Optional[str] = None
    creation_date: Optional[str] = None
    
    class Config:
        """Pydantic configuration."""
        use_enum_values = True
        
    def is_transformation_only(self) -> bool:
        """Check if package contains only transformation tasks (suitable for dbt)."""
        ingestion_types = {
            ExecutableType.BULK_INSERT,
            ExecutableType.FILE_SYSTEM,
            ExecutableType.FTP,
            ExecutableType.WEB_SERVICE,
        }
        
        for exe in self.executables:
            if exe.type in ingestion_types:
                return False
            if exe.type == ExecutableType.DATA_FLOW:
                # Check if data flow has file sources or external destinations
                for comp in exe.components:
                    if comp.component_type in {
                        ComponentType.FLAT_FILE_SOURCE,
                        ComponentType.FLAT_FILE_DESTINATION,
                    }:
                        return False
        return True
    
    def get_data_flows(self) -> List[Executable]:
        """Get all data flow tasks."""
        return [exe for exe in self.executables if exe.type == ExecutableType.DATA_FLOW]
    
    def get_sql_tasks(self) -> List[Executable]:
        """Get all Execute SQL tasks."""
        return [exe for exe in self.executables if exe.type == ExecutableType.EXECUTE_SQL]


class MigrationReport(BaseModel):
    """Migration analysis and coverage report."""
    package_name: str
    source_file: str
    migration_mode: Literal["airflow", "dbt", "mixed"] 
    total_executables: int
    supported_executables: int
    unsupported_executables: List[str] = Field(default_factory=list)
    encrypted_properties: List[str] = Field(default_factory=list)
    manual_review_items: List[str] = Field(default_factory=list)
    sql_dialect_conversions: List[str] = Field(default_factory=list)
    generated_files: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    success: bool = True