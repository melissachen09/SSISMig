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


class ProjectParameter(BaseModel):
    """Project-scoped parameter available to all packages."""
    name: str
    data_type: str = Field(description="Data type (e.g., DateTime, Int32, String)")
    value: Optional[Any] = None
    description: Optional[str] = None
    sensitive: bool = False


class ProjectConnectionManager(BaseModel):
    """Project-scoped connection manager available to all packages."""
    id: str = Field(description="Unique identifier")
    name: str = Field(description="Display name")
    type: ConnectionType
    connection_string: str
    provider: str
    description: Optional[str] = None
    protection_level: ProtectionLevel = ProtectionLevel.ENCRYPT_SENSITIVE_WITH_USER_KEY


class PackageDependency(BaseModel):
    """Cross-package dependency via ExecutePackageTask."""
    parent_package: str = Field(description="Package containing ExecutePackageTask")
    child_package: str = Field(description="Package being executed")
    task_name: str = Field(description="Name of ExecutePackageTask")
    task_id: str = Field(description="Full task ID")
    precedence_constraint: Optional[PrecedenceCondition] = None
    condition_expression: Optional[str] = None
    

class PackageReference(BaseModel):
    """Reference to a package within the project."""
    name: str
    file_path: str
    relative_path: str = Field(description="Path relative to project root")
    entry_point: bool = False
    transformation_only: bool = False


class IRProject(BaseModel):
    """Complete Intermediate Representation of an SSIS project with multiple packages."""
    project_name: str
    project_version: str = "1.0"
    target_server_version: str = "SQL2019"
    protection_level: ProtectionLevel = ProtectionLevel.ENCRYPT_SENSITIVE_WITH_USER_KEY
    
    # Project-level artifacts
    project_parameters: List[ProjectParameter] = Field(default_factory=list)
    project_connections: List[ProjectConnectionManager] = Field(default_factory=list)
    
    # Package structure
    packages: List[PackageReference] = Field(default_factory=list)
    package_irs: Dict[str, IRPackage] = Field(default_factory=dict, description="IR for each package")
    dependencies: List[PackageDependency] = Field(default_factory=list)
    
    # Analysis results
    entry_points: List[str] = Field(default_factory=list, description="Packages that are not called by others")
    execution_chains: List[Dict[str, Any]] = Field(default_factory=list)
    isolated_packages: List[str] = Field(default_factory=list)
    
    class Config:
        """Pydantic configuration."""
        use_enum_values = True
    
    def get_transformation_packages(self) -> List[str]:
        """Get packages that only contain transformations (suitable for dbt)."""
        transform_packages = []
        for pkg_name, pkg_ir in self.package_irs.items():
            if pkg_ir.is_transformation_only():
                transform_packages.append(pkg_name)
        return transform_packages
    
    def get_orchestration_packages(self) -> List[str]:
        """Get packages that contain orchestration logic (need Airflow)."""
        orchestration_packages = []
        for pkg_name, pkg_ir in self.package_irs.items():
            if not pkg_ir.is_transformation_only():
                orchestration_packages.append(pkg_name)
        return orchestration_packages
    
    def has_cross_package_dependencies(self) -> bool:
        """Check if project has ExecutePackageTask dependencies between packages."""
        return len(self.dependencies) > 0
    
    def get_dependency_graph(self) -> Dict[str, List[str]]:
        """Build dependency graph for package execution order."""
        graph = {pkg.name: [] for pkg in self.packages}
        for dep in self.dependencies:
            if dep.parent_package in graph:
                graph[dep.parent_package].append(dep.child_package)
        return graph
    
    def recommend_migration_strategy(self) -> Dict[str, Any]:
        """Analyze project and recommend migration approach."""
        transform_pkgs = self.get_transformation_packages()
        orchestration_pkgs = self.get_orchestration_packages()
        has_dependencies = self.has_cross_package_dependencies()
        
        recommendations = {
            'strategy': 'mixed',
            'components': [],
            'rationale': [],
            'dbt_packages': transform_pkgs,
            'airflow_packages': orchestration_pkgs,
            'needs_master_dag': has_dependencies and len(self.entry_points) > 1
        }
        
        if len(transform_pkgs) == len(self.packages):
            # All transformation packages
            if has_dependencies:
                recommendations['strategy'] = 'dbt_with_orchestration'
                recommendations['components'] = ['dbt', 'airflow_orchestrator']
                recommendations['rationale'].append("All packages are transformation-only but have dependencies")
            else:
                recommendations['strategy'] = 'dbt_only'
                recommendations['components'] = ['dbt']
                recommendations['rationale'].append("All packages are independent transformations")
        
        elif len(orchestration_pkgs) == len(self.packages):
            # All orchestration packages
            recommendations['strategy'] = 'airflow_only'
            recommendations['components'] = ['airflow']
            recommendations['rationale'].append("All packages contain orchestration logic")
        
        else:
            # Mixed packages
            recommendations['strategy'] = 'mixed'
            recommendations['components'] = ['airflow', 'dbt']
            recommendations['rationale'].append("Mix of transformation and orchestration packages")
        
        if has_dependencies:
            recommendations['rationale'].append("Cross-package dependencies require orchestration")
            
        return recommendations


class MigrationReport(BaseModel):
    """Migration analysis and coverage report."""
    package_name: str
    source_file: str
    migration_mode: Literal["airflow", "dbt", "mixed", "dbt_only", "dbt_with_orchestration", "airflow_only"] 
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


class ProjectMigrationReport(BaseModel):
    """Complete project migration analysis and report."""
    project_name: str
    project_version: str
    source_path: str
    total_packages: int
    transformation_packages: List[str] = Field(default_factory=list)
    orchestration_packages: List[str] = Field(default_factory=list)
    dependencies_count: int
    migration_strategy: Dict[str, Any] = Field(default_factory=dict)
    package_reports: Dict[str, MigrationReport] = Field(default_factory=dict)
    generated_artifacts: Dict[str, List[str]] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    success: bool = True


def ir_to_json(ir_obj: Union[IRPackage, IRProject]) -> str:
    """Convert IR object to JSON string."""
    return ir_obj.model_dump_json(indent=2)


def json_to_ir_package(json_str: str) -> IRPackage:
    """Convert JSON string to IRPackage."""
    return IRPackage.model_validate_json(json_str)


def json_to_ir_project(json_str: str) -> IRProject:
    """Convert JSON string to IRProject."""
    return IRProject.model_validate_json(json_str)