"""Main IR converter that orchestrates all parsers.

Converts DTSX files to Intermediate Representation (IR) by coordinating
all the specialized parsers.
"""
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from lxml import etree

from models.ir import IRPackage, MigrationReport
from .dtsx_reader import DTSXReader, DTSXParseError
from .dataflow_parser import DataFlowParser
from .precedence_parser import PrecedenceParser  
from .expressions import SSISExpressionParser
from .secure import SSISSecurityHandler

logger = logging.getLogger(__name__)


class DTSXToIRConverter:
    """Main converter from DTSX to Intermediate Representation."""
    
    def __init__(self, password: Optional[str] = None):
        """Initialize converter.
        
        Args:
            password: Optional password for encrypted packages
        """
        self.password = password
        self.security_handler = SSISSecurityHandler(password)
        
    def convert_file(self, dtsx_path: str) -> tuple[IRPackage, MigrationReport]:
        """Convert a DTSX file to IR.
        
        Args:
            dtsx_path: Path to the DTSX file
            
        Returns:
            Tuple of (IRPackage, MigrationReport)
            
        Raises:
            DTSXParseError: If conversion fails
        """
        dtsx_path = Path(dtsx_path)
        logger.info(f"Converting DTSX file: {dtsx_path}")
        
        # Initialize report
        report = MigrationReport(
            package_name=dtsx_path.stem,
            source_file=str(dtsx_path),
            migration_mode="airflow",  # Will be determined later
            total_executables=0,
            supported_executables=0
        )
        
        try:
            # Load and parse DTSX file
            reader = DTSXReader(str(dtsx_path), self.password)
            reader.load()
            
            # Get package metadata
            metadata = reader.get_package_metadata()
            
            # Parse package components
            parameters = reader.get_parameters()
            variables = reader.get_variables()
            connection_managers = reader.get_connection_managers()
            executables = reader.get_executables()
            
            # Parse precedence constraints
            precedence_parser = PrecedenceParser(reader.namespaces)
            edges = precedence_parser.parse_precedence_constraints(reader.root)
            
            # Parse expressions
            expr_parser = SSISExpressionParser(reader.namespaces)
            expressions = expr_parser.parse_expressions(reader.root)
            
            # Parse data flow components
            dataflow_parser = DataFlowParser(reader.namespaces)
            for executable in executables:
                if str(executable.type) == "DataFlow":
                    # Find the corresponding XML element
                    exe_elements = reader.root.xpath(
                        f".//DTS:Executable[@DTS:refId='{executable.id}']",
                        namespaces=reader.namespaces
                    )
                    if exe_elements:
                        obj_data = exe_elements[0].find("DTS:ObjectData", namespaces=reader.namespaces)
                        if obj_data is not None:
                            dataflow_parser.parse_dataflow(obj_data, executable)
            
            # Handle security/encryption
            secured_connections = []
            for cm in connection_managers:
                secured_props = self.security_handler.handle_protection_level(
                    metadata.get("protection_level"),
                    cm.properties
                )
                cm.properties = secured_props
                secured_connections.append(cm)
            
            # Create IR package
            ir_package = IRPackage(
                package_name=metadata["package_name"],
                protection_level=metadata.get("protection_level"),
                parameters=parameters,
                variables=variables, 
                connection_managers=secured_connections,
                executables=executables,
                edges=edges,
                expressions=expressions,
                version_build=metadata.get("version_build"),
                version_comments=metadata.get("version_comments"),
                creator_name=metadata.get("creator_name"),
                creation_date=metadata.get("creation_date")
            )
            
            # Update report
            report.total_executables = len(executables)
            report.supported_executables = len([exe for exe in executables if exe.type])
            report.unsupported_executables = [
                exe.object_name for exe in executables 
                if not exe.type or str(exe.type) == "Unknown"
            ]
            
            # Check for encrypted properties
            for cm in connection_managers:
                for prop_name, prop_value in cm.properties.items():
                    if prop_value == "[REDACTED]":
                        report.encrypted_properties.append(f"{cm.name}.{prop_name}")
            
            # Determine migration mode
            if ir_package.is_transformation_only():
                report.migration_mode = "dbt"
            else:
                # Check if mixed (has both ingestion and transformation)
                has_dataflows = len(ir_package.get_data_flows()) > 0
                has_sql_tasks = len(ir_package.get_sql_tasks()) > 0
                
                if has_dataflows and has_sql_tasks:
                    report.migration_mode = "mixed"
                else:
                    report.migration_mode = "airflow"
            
            # Add security warnings
            security_warnings = self.security_handler.validate_security_compliance(
                ir_package.model_dump()
            )
            report.warnings.extend(security_warnings)
            
            # Validate precedence constraints
            all_task_ids = [exe.id for exe in executables]
            constraint_issues = precedence_parser.validate_constraints(edges, all_task_ids)
            if constraint_issues:
                report.warnings.extend(constraint_issues)
            
            # Add manual review items
            report.manual_review_items = self._identify_manual_review_items(ir_package)
            
            report.success = True
            logger.info(f"Successfully converted {dtsx_path} to IR")
            
            return ir_package, report
            
        except Exception as e:
            logger.error(f"Failed to convert {dtsx_path}: {e}")
            report.success = False
            report.errors.append(str(e))
            
            # Return minimal IR package on error
            minimal_package = IRPackage(package_name=dtsx_path.stem)
            return minimal_package, report
    
    def _identify_manual_review_items(self, ir_package: IRPackage) -> list[str]:
        """Identify items that require manual review.
        
        Args:
            ir_package: IR package to analyze
            
        Returns:
            List of manual review items
        """
        items = []
        
        # Check for script tasks
        script_tasks = [exe for exe in ir_package.executables if str(exe.type) == "ScriptTask"]
        if script_tasks:
            items.append(f"Found {len(script_tasks)} Script Tasks - manual conversion required")
        
        # Check for complex expressions
        for expr in ir_package.expressions:
            if len(expr.expression) > 100:  # Arbitrary complexity threshold
                items.append(f"Complex expression in {expr.scope}.{expr.property} - review required")
        
        # Check for unsupported connection types
        unsupported_conn_types = ["FTP", "HTTP", "SMTP"]
        unsupported_conns = [
            cm for cm in ir_package.connection_managers 
            if str(cm.type) in unsupported_conn_types
        ]
        if unsupported_conns:
            items.append(f"Found {len(unsupported_conns)} connections with limited support")
        
        # Check for encrypted packages
        if str(ir_package.protection_level) != "DontSaveSensitive":
            items.append("Encrypted package - verify all sensitive data is properly handled")
        
        # Check for loops (complex control flow)
        loop_tasks = [
            exe for exe in ir_package.executables 
            if str(exe.type) in ["ForEachLoop", "ForLoop"]
        ]
        if loop_tasks:
            items.append(f"Found {len(loop_tasks)} loop containers - verify dynamic mapping")
        
        return items
    
    def validate_ir(self, ir_package: IRPackage) -> list[str]:
        """Validate the generated IR package.
        
        Args:
            ir_package: IR package to validate
            
        Returns:
            List of validation errors
        """
        errors = []
        
        try:
            # Pydantic validation
            ir_package.model_validate(ir_package.model_dump())
        except Exception as e:
            errors.append(f"IR validation failed: {e}")
        
        # Check for orphaned edges
        task_ids = {exe.id for exe in ir_package.executables}
        for edge in ir_package.edges:
            if edge.from_task not in task_ids:
                errors.append(f"Edge references unknown source task: {edge.from_task}")
            if edge.to_task not in task_ids:
                errors.append(f"Edge references unknown target task: {edge.to_task}")
        
        # Check for data flow consistency
        for exe in ir_package.executables:
            if str(exe.type) == "DataFlow":
                # Validate component wiring
                component_ids = {comp.id for comp in exe.components}
                for comp in exe.components:
                    for input_id in comp.inputs:
                        # Input should be an output of another component or external
                        if not any(input_id in other.outputs for other in exe.components):
                            logger.warning(f"Component {comp.id} has unresolved input: {input_id}")
        
        return errors