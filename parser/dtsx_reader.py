"""Core DTSX XML reader and parser.

Handles loading DTSX files, namespace management, and basic XML parsing 
according to section 8 of the migration plan.
"""
import logging
from pathlib import Path
from typing import Dict, Optional, List, Any
from lxml import etree
from models.ir import (
    IRPackage, Parameter, Variable, ConnectionManager, Executable,
    ProtectionLevel, ConnectionType, ExecutableType
)

logger = logging.getLogger(__name__)


# SSIS XML namespaces
SSIS_NAMESPACES = {
    "DTS": "www.microsoft.com/SqlServer/Dts",
    "SQLTask": "www.microsoft.com/sqlserver/dts/tasks/sqltask", 
    "Pipeline": "www.microsoft.com/sqlserver/dts/pipeline",
    "ScriptTask": "www.microsoft.com/sqlserver/dts/tasks/scripttask",
    "ForEach": "www.microsoft.com/sqlserver/dts/tasks/foreachloop",
    "Sequence": "www.microsoft.com/sqlserver/dts/tasks/sequence",
}


class DTSXParseError(Exception):
    """Exception raised during DTSX parsing."""
    pass


class DTSXReader:
    """Main DTSX file reader and parser."""
    
    def __init__(self, file_path: str, password: Optional[str] = None):
        """Initialize DTSX reader.
        
        Args:
            file_path: Path to the .dtsx file
            password: Optional password for encrypted packages
        """
        self.file_path = Path(file_path)
        self.password = password
        self.tree: Optional[etree._ElementTree] = None
        self.root: Optional[etree._Element] = None
        self.namespaces = SSIS_NAMESPACES
        
    def load(self) -> None:
        """Load and parse the DTSX XML file.
        
        Raises:
            DTSXParseError: If file cannot be loaded or parsed
        """
        try:
            if not self.file_path.exists():
                raise DTSXParseError(f"DTSX file not found: {self.file_path}")
                
            logger.info(f"Loading DTSX file: {self.file_path}")
            
            # Parse XML with namespace awareness
            parser = etree.XMLParser(ns_clean=True, recover=True)
            self.tree = etree.parse(str(self.file_path), parser)
            self.root = self.tree.getroot()
            
            # Validate root element
            if self.root.tag != f"{{{self.namespaces['DTS']}}}Executable":
                raise DTSXParseError(f"Invalid DTSX root element: {self.root.tag}")
                
            logger.info("DTSX file loaded successfully")
            
        except etree.XMLSyntaxError as e:
            raise DTSXParseError(f"XML syntax error in DTSX file: {e}")
        except Exception as e:
            raise DTSXParseError(f"Failed to load DTSX file: {e}")
    
    def get_package_metadata(self) -> Dict[str, Any]:
        """Extract package-level metadata.
        
        Returns:
            Dictionary containing package metadata
        """
        if not self.root:
            raise DTSXParseError("DTSX file not loaded")
            
        metadata = {}
        
        # Package name (from ObjectName or filename)
        obj_name = self.root.get(f"{{{self.namespaces['DTS']}}}ObjectName")
        metadata["package_name"] = obj_name or self.file_path.stem
        
        # Protection level
        protection = self.root.get(f"{{{self.namespaces['DTS']}}}ProtectionLevel")
        if protection:
            try:
                metadata["protection_level"] = ProtectionLevel(protection)
            except ValueError:
                logger.warning(f"Unknown protection level: {protection}")
                metadata["protection_level"] = ProtectionLevel.DONT_SAVE_SENSITIVE
        else:
            metadata["protection_level"] = ProtectionLevel.DONT_SAVE_SENSITIVE
            
        # Version information
        metadata["version_build"] = self.root.get(f"{{{self.namespaces['DTS']}}}VersionBuild")
        metadata["version_comments"] = self.root.get(f"{{{self.namespaces['DTS']}}}VersionComments")
        metadata["creator_name"] = self.root.get(f"{{{self.namespaces['DTS']}}}CreatorName")
        metadata["creation_date"] = self.root.get(f"{{{self.namespaces['DTS']}}}CreationDate")
        
        return metadata
    
    def get_parameters(self) -> List[Parameter]:
        """Extract package parameters.
        
        Returns:
            List of Parameter objects
        """
        if not self.root:
            raise DTSXParseError("DTSX file not loaded")
            
        parameters = []
        
        # Parameters are typically under DTS:Property elements with Name="Parameters"
        param_props = self.root.xpath(
            ".//DTS:Property[@DTS:Name='Parameters']", 
            namespaces=self.namespaces
        )
        
        for prop in param_props:
            # Parameters collection contains individual parameter elements
            param_elements = prop.xpath(".//DTS:Parameter", namespaces=self.namespaces)
            
            for param_elem in param_elements:
                name = param_elem.get(f"{{{self.namespaces['DTS']}}}ObjectName")
                data_type = param_elem.get(f"{{{self.namespaces['DTS']}}}DataType")
                
                if name:
                    # Extract default value if present
                    value = None
                    value_elem = param_elem.find(
                        f".//{{{self.namespaces['DTS']}}}PropertyExpression[@DTS:Name='Value']", 
                        namespaces=self.namespaces
                    )
                    if value_elem is not None:
                        value = value_elem.text
                    
                    parameters.append(Parameter(
                        name=name,
                        type=data_type or "String",
                        value=value
                    ))
                    
        logger.info(f"Found {len(parameters)} parameters")
        return parameters
    
    def get_variables(self) -> List[Variable]:
        """Extract package variables.
        
        Returns:
            List of Variable objects
        """
        if not self.root:
            raise DTSXParseError("DTSX file not loaded")
            
        variables = []
        
        # Variables are under DTS:Property with Name="Variables"
        var_props = self.root.xpath(
            ".//DTS:Property[@DTS:Name='Variables']",
            namespaces=self.namespaces
        )
        
        for prop in var_props:
            var_elements = prop.xpath(".//DTS:Variable", namespaces=self.namespaces)
            
            for var_elem in var_elements:
                name = var_elem.get(f"{{{self.namespaces['DTS']}}}ObjectName")
                data_type = var_elem.get(f"{{{self.namespaces['DTS']}}}DataType")
                
                if name:
                    # Extract value
                    value = None
                    value_elem = var_elem.find(
                        f".//{{{self.namespaces['DTS']}}}PropertyExpression[@DTS:Name='Value']",
                        namespaces=self.namespaces
                    )
                    if value_elem is not None:
                        value = value_elem.text
                    
                    variables.append(Variable(
                        name=name,
                        type=data_type or "String",
                        value=value,
                        scope="Package"
                    ))
                    
        logger.info(f"Found {len(variables)} variables")
        return variables
    
    def get_connection_managers(self) -> List[ConnectionManager]:
        """Extract connection managers.
        
        Returns:
            List of ConnectionManager objects
        """
        if not self.root:
            raise DTSXParseError("DTSX file not loaded")
            
        connections = []
        
        # Connection managers are under DTS:ConnectionManagers
        conn_managers = self.root.xpath(
            ".//DTS:ConnectionManager",
            namespaces=self.namespaces
        )
        
        for cm in conn_managers:
            cm_id = cm.get(f"{{{self.namespaces['DTS']}}}refId") 
            obj_name = cm.get(f"{{{self.namespaces['DTS']}}}ObjectName")
            creation_name = cm.get(f"{{{self.namespaces['DTS']}}}CreationName")
            
            if cm_id and obj_name:
                # Map creation name to connection type
                conn_type = self._map_connection_type(creation_name)
                
                # Extract properties (may be encrypted)
                properties = {}
                prop_elements = cm.xpath(".//DTS:Property", namespaces=self.namespaces)
                
                for prop in prop_elements:
                    prop_name = prop.get(f"{{{self.namespaces['DTS']}}}Name")
                    if prop_name and prop.text:
                        # Don't store sensitive properties in plain text
                        if prop_name.lower() in ['password', 'connectionstring']:
                            properties[prop_name] = "[REDACTED]"
                        else:
                            properties[prop_name] = prop.text
                
                connections.append(ConnectionManager(
                    id=cm_id,
                    name=obj_name,
                    type=conn_type,
                    properties=properties,
                    sensitive=creation_name in ['OLEDB', 'ADO.NET', 'SQLCLIENT']
                ))
        
        logger.info(f"Found {len(connections)} connection managers")
        return connections
    
    def get_executables(self) -> List[Executable]:
        """Extract executable tasks.
        
        Returns:
            List of Executable objects
        """
        if not self.root:
            raise DTSXParseError("DTSX file not loaded")
            
        executables = []
        
        # Find all executable elements
        exe_elements = self.root.xpath(".//DTS:Executable", namespaces=self.namespaces)
        
        for exe in exe_elements:
            exe_type = exe.get(f"{{{self.namespaces['DTS']}}}ExecutableType")
            exe_id = exe.get(f"{{{self.namespaces['DTS']}}}refId")
            obj_name = exe.get(f"{{{self.namespaces['DTS']}}}ObjectName")
            
            if exe_type and exe_id and obj_name:
                # Map SSIS executable type to our enum
                mapped_type = self._map_executable_type(exe_type)
                
                if mapped_type:
                    executable = Executable(
                        id=exe_id,
                        type=mapped_type,
                        object_name=obj_name
                    )
                    
                    # Extract task-specific properties
                    self._extract_task_properties(exe, executable)
                    
                    executables.append(executable)
                else:
                    logger.warning(f"Unsupported executable type: {exe_type}")
        
        logger.info(f"Found {len(executables)} executables")
        return executables
    
    def _map_connection_type(self, creation_name: Optional[str]) -> ConnectionType:
        """Map SSIS connection creation name to our enum."""
        if not creation_name:
            return ConnectionType.OLEDB
            
        mapping = {
            "OLEDB": ConnectionType.OLEDB,
            "ADO.NET": ConnectionType.ADONET, 
            "FLATFILE": ConnectionType.FLATFILE,
            "FILE": ConnectionType.FILE,
            "HTTP": ConnectionType.HTTP,
            "FTP": ConnectionType.FTP,
            "SMTP": ConnectionType.SMTP,
        }
        
        return mapping.get(creation_name, ConnectionType.OLEDB)
    
    def _map_executable_type(self, exe_type: str) -> Optional[ExecutableType]:
        """Map SSIS executable type to our enum."""
        mapping = {
            "Microsoft.ExecuteSQLTask": ExecutableType.EXECUTE_SQL,
            "Microsoft.Pipeline": ExecutableType.DATA_FLOW,
            "Microsoft.ScriptTask": ExecutableType.SCRIPT_TASK,
            "Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ExecuteSQLTask": ExecutableType.EXECUTE_SQL,
            "Microsoft.SqlServer.Dts.Pipeline.Wrapper.TaskHost": ExecutableType.DATA_FLOW,
            "STOCK:SEQUENCE": ExecutableType.SEQUENCE_CONTAINER,
            "STOCK:FOREACHLOOP": ExecutableType.FOREACH_LOOP,
            "STOCK:FORLOOP": ExecutableType.FOR_LOOP,
            "Microsoft.BulkInsertTask": ExecutableType.BULK_INSERT,
            "Microsoft.FileSystemTask": ExecutableType.FILE_SYSTEM,
            "Microsoft.FtpTask": ExecutableType.FTP,
            "Microsoft.SendMailTask": ExecutableType.SEND_MAIL,
        }
        
        return mapping.get(exe_type)
    
    def _extract_task_properties(self, exe_element: etree._Element, executable: Executable) -> None:
        """Extract task-specific properties from executable element."""
        obj_data = exe_element.find("DTS:ObjectData", namespaces=self.namespaces)
        
        if obj_data is None:
            return
            
        if executable.type == ExecutableType.EXECUTE_SQL:
            self._extract_sql_task_properties(obj_data, executable)
        elif executable.type == ExecutableType.DATA_FLOW:
            self._extract_dataflow_properties(obj_data, executable)
        elif executable.type == ExecutableType.SCRIPT_TASK:
            self._extract_script_task_properties(obj_data, executable)
    
    def _extract_sql_task_properties(self, obj_data: etree._Element, executable: Executable) -> None:
        """Extract Execute SQL Task properties."""
        sql_task = obj_data.find("SQLTask:SqlTaskData", namespaces=self.namespaces)
        
        if sql_task is not None:
            # Get SQL statement
            sql_stmt = sql_task.get("SqlStatementSource")
            if sql_stmt:
                executable.sql = sql_stmt
                # Detect dialect (rough heuristic)
                if any(keyword in sql_stmt.upper() for keyword in ['GETDATE()', 'ISNULL(', 'TOP ']):
                    executable.dialect = "tsql"
                else:
                    executable.dialect = "ansi"
    
    def _extract_dataflow_properties(self, obj_data: etree._Element, executable: Executable) -> None:
        """Extract Data Flow Task properties - handled by dataflow_parser.py."""
        # This will be implemented in the separate dataflow parser
        pass
    
    def _extract_script_task_properties(self, obj_data: etree._Element, executable: Executable) -> None:
        """Extract Script Task properties."""
        script_task = obj_data.find("ScriptTask:ScriptTaskData", namespaces=self.namespaces)
        
        if script_task is not None:
            # Extract script properties
            script_lang = script_task.get("ScriptLanguage")
            if script_lang:
                executable.properties["script_language"] = script_lang
                
            # Script source (may be embedded or in separate file)  
            script_src = script_task.get("ScriptProjectName")
            if script_src:
                executable.properties["script_project"] = script_src