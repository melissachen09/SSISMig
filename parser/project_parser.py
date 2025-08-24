"""SSIS Project Parser - handles .ispac files and project folders.

Parses SSIS Integration Services projects (.ispac files or project directories)
to extract project-level metadata, connection managers, parameters, and package dependencies.
"""
import json
import logging
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union
import xml.etree.ElementTree as ET
from dataclasses import dataclass

from lxml import etree

logger = logging.getLogger(__name__)

# SSIS project XML namespaces
PROJECT_NAMESPACES = {
    'DTS': 'www.microsoft.com/SqlServer/Dts',
    'Project': 'www.microsoft.com/SqlServer/Dts/Project',
    'SSIS': 'www.microsoft.com/SqlServer/Dts/SSIS'
}


@dataclass
class ProjectParameter:
    """Project-level parameter definition."""
    name: str
    data_type: str
    value: str
    sensitive: bool = False
    description: str = ""


@dataclass
class ProjectConnectionManager:
    """Project-level connection manager."""
    name: str
    connection_string: str
    provider: str
    description: str = ""
    protection_level: str = "EncryptSensitiveWithUserKey"


@dataclass
class PackageReference:
    """Reference to a package within the project."""
    name: str
    file_path: str
    entry_point: bool = False  # True if package is executed directly (not via ExecutePackageTask)
    

@dataclass
class PackageDependency:
    """Dependency between packages via ExecutePackageTask."""
    parent_package: str
    child_package: str
    task_name: str
    precedence_constraint: Optional[str] = None


@dataclass
class SSISProject:
    """Complete SSIS project metadata and package structure."""
    project_name: str
    project_version: str
    packages: List[PackageReference]
    parameters: Dict[str, ProjectParameter]
    connection_managers: Dict[str, ProjectConnectionManager]
    dependencies: List[PackageDependency]
    target_server_version: str = "SQL2019"
    protection_level: str = "EncryptSensitiveWithUserKey"


class SSISProjectParser:
    """Parser for SSIS Integration Services projects."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def parse_project(self, project_path: Union[str, Path]) -> SSISProject:
        """Parse an SSIS project from .ispac file or project directory.
        
        Args:
            project_path: Path to .ispac file or project directory
            
        Returns:
            SSISProject with complete project metadata
        """
        project_path = Path(project_path)
        
        if project_path.suffix.lower() == '.ispac':
            return self._parse_ispac_file(project_path)
        elif project_path.is_dir():
            return self._parse_project_directory(project_path)
        else:
            raise ValueError(f"Invalid project path: {project_path}")
    
    def _parse_ispac_file(self, ispac_path: Path) -> SSISProject:
        """Parse .ispac (SSIS Deployment Package) file.
        
        .ispac files are ZIP archives containing:
        - Project.params (project parameters)
        - [PackageName].dtsx files
        - Project.conmgr (connection managers)  
        - @Project.manifest (project metadata)
        """
        self.logger.info(f"Parsing ISPAC file: {ispac_path}")
        
        with zipfile.ZipFile(ispac_path, 'r') as ispac:
            # List all files in the archive
            file_list = ispac.namelist()
            self.logger.debug(f"ISPAC contents: {file_list}")
            
            # Parse project manifest
            manifest_content = ispac.read('@Project.manifest').decode('utf-8')
            project_info = self._parse_project_manifest(manifest_content)
            
            # Parse project parameters
            parameters = {}
            if 'Project.params' in file_list:
                params_content = ispac.read('Project.params').decode('utf-8')
                parameters = self._parse_project_parameters(params_content)
            
            # Parse project connection managers  
            connection_managers = {}
            if 'Project.conmgr' in file_list:
                conmgr_content = ispac.read('Project.conmgr').decode('utf-8')
                connection_managers = self._parse_project_connections(conmgr_content)
            
            # Find all .dtsx packages
            packages = []
            for file_name in file_list:
                if file_name.endswith('.dtsx'):
                    package_name = Path(file_name).stem
                    packages.append(PackageReference(
                        name=package_name,
                        file_path=file_name
                    ))
            
            # Analyze package dependencies by parsing each .dtsx for ExecutePackageTask
            dependencies = []
            for package_ref in packages:
                dtsx_content = ispac.read(package_ref.file_path).decode('utf-8')
                pkg_deps = self._extract_package_dependencies(package_ref.name, dtsx_content)
                dependencies.extend(pkg_deps)
        
        return SSISProject(
            project_name=project_info['name'],
            project_version=project_info['version'],
            packages=packages,
            parameters=parameters,
            connection_managers=connection_managers,
            dependencies=dependencies,
            target_server_version=project_info.get('target_version', 'SQL2019'),
            protection_level=project_info.get('protection_level', 'EncryptSensitiveWithUserKey')
        )
    
    def _parse_project_directory(self, project_dir: Path) -> SSISProject:
        """Parse SSIS project from directory structure.
        
        Expected structure:
        ProjectName/
        ├── ProjectName.dtproj  (project file)
        ├── Project.params      (parameters)
        ├── Project.conmgr      (connections) 
        └── *.dtsx             (packages)
        """
        self.logger.info(f"Parsing project directory: {project_dir}")
        
        # Find project file (.dtproj)
        project_files = list(project_dir.glob('*.dtproj'))
        if not project_files:
            raise ValueError(f"No .dtproj file found in {project_dir}")
        
        project_file = project_files[0]
        project_name = project_file.stem
        
        # Parse project file for metadata
        project_info = self._parse_dtproj_file(project_file)
        
        # Parse parameters
        parameters = {}
        params_file = project_dir / 'Project.params'
        if params_file.exists():
            parameters = self._parse_project_parameters(params_file.read_text(encoding='utf-8'))
        
        # Parse connection managers
        connection_managers = {}
        conmgr_file = project_dir / 'Project.conmgr'
        if conmgr_file.exists():
            connection_managers = self._parse_project_connections(conmgr_file.read_text(encoding='utf-8'))
        
        # Find all .dtsx packages
        packages = []
        for dtsx_file in project_dir.glob('*.dtsx'):
            packages.append(PackageReference(
                name=dtsx_file.stem,
                file_path=str(dtsx_file.relative_to(project_dir))
            ))
        
        # Analyze dependencies
        dependencies = []
        for package_ref in packages:
            dtsx_path = project_dir / package_ref.file_path
            dtsx_content = dtsx_path.read_text(encoding='utf-8')
            pkg_deps = self._extract_package_dependencies(package_ref.name, dtsx_content)
            dependencies.extend(pkg_deps)
        
        return SSISProject(
            project_name=project_name,
            project_version=project_info.get('version', '1.0'),
            packages=packages,
            parameters=parameters,
            connection_managers=connection_managers,
            dependencies=dependencies,
            target_server_version=project_info.get('target_version', 'SQL2019'),
            protection_level=project_info.get('protection_level', 'EncryptSensitiveWithUserKey')
        )
    
    def _parse_project_manifest(self, manifest_xml: str) -> Dict:
        """Parse @Project.manifest XML for project metadata."""
        try:
            root = etree.fromstring(manifest_xml.encode('utf-8'))
            
            # Extract basic project info
            project_info = {
                'name': root.get('Name', 'UnknownProject'),
                'version': root.get('Version', '1.0'),
                'protection_level': root.get('ProtectionLevel', 'EncryptSensitiveWithUserKey'),
                'target_version': root.get('TargetServerVersion', 'SQL2019')
            }
            
            return project_info
            
        except etree.XMLSyntaxError as e:
            self.logger.warning(f"Failed to parse project manifest: {e}")
            return {'name': 'UnknownProject', 'version': '1.0'}
    
    def _parse_dtproj_file(self, dtproj_path: Path) -> Dict:
        """Parse .dtproj project file for metadata."""
        try:
            content = dtproj_path.read_text(encoding='utf-8')
            root = etree.fromstring(content.encode('utf-8'))
            
            # Extract project properties
            project_info = {
                'version': '1.0',
                'target_version': 'SQL2019',
                'protection_level': 'EncryptSensitiveWithUserKey'
            }
            
            # Look for MSBuild properties
            for prop_group in root.xpath('.//PropertyGroup', namespaces=PROJECT_NAMESPACES):
                for prop in prop_group:
                    if prop.tag.endswith('TargetServerVersion'):
                        project_info['target_version'] = prop.text
                    elif prop.tag.endswith('ProtectionLevel'):
                        project_info['protection_level'] = prop.text
            
            return project_info
            
        except (etree.XMLSyntaxError, FileNotFoundError) as e:
            self.logger.warning(f"Failed to parse .dtproj file: {e}")
            return {'version': '1.0', 'target_version': 'SQL2019'}
    
    def _parse_project_parameters(self, params_xml: str) -> Dict[str, ProjectParameter]:
        """Parse Project.params XML for project-level parameters."""
        parameters = {}
        
        try:
            root = etree.fromstring(params_xml.encode('utf-8'))
            
            for param_node in root.xpath('.//Parameter', namespaces=PROJECT_NAMESPACES):
                name = param_node.get('Name')
                data_type = param_node.get('DataType', 'String')
                
                # Get parameter value
                value = ""
                value_node = param_node.find('.//ParameterValue', PROJECT_NAMESPACES)
                if value_node is not None:
                    value = value_node.text or ""
                
                # Check if sensitive
                sensitive = param_node.get('Sensitive', 'false').lower() == 'true'
                
                parameters[name] = ProjectParameter(
                    name=name,
                    data_type=data_type,
                    value=value,
                    sensitive=sensitive,
                    description=param_node.get('Description', '')
                )
                
        except etree.XMLSyntaxError as e:
            self.logger.warning(f"Failed to parse project parameters: {e}")
        
        return parameters
    
    def _parse_project_connections(self, conmgr_xml: str) -> Dict[str, ProjectConnectionManager]:
        """Parse Project.conmgr XML for project-level connection managers."""
        connections = {}
        
        try:
            root = etree.fromstring(conmgr_xml.encode('utf-8'))
            
            for conn_node in root.xpath('.//ConnectionManager', namespaces=PROJECT_NAMESPACES):
                name = conn_node.get('Name')
                
                # Get connection string (might be encrypted)
                conn_string = ""
                conn_string_node = conn_node.find('.//ConnectionString', PROJECT_NAMESPACES)
                if conn_string_node is not None:
                    conn_string = conn_string_node.text or ""
                
                # Get provider info
                provider = conn_node.get('CreationName', 'Unknown')
                
                connections[name] = ProjectConnectionManager(
                    name=name,
                    connection_string=conn_string,
                    provider=provider,
                    description=conn_node.get('Description', ''),
                    protection_level=conn_node.get('ProtectionLevel', 'EncryptSensitiveWithUserKey')
                )
                
        except etree.XMLSyntaxError as e:
            self.logger.warning(f"Failed to parse project connections: {e}")
        
        return connections
    
    def _extract_package_dependencies(self, package_name: str, dtsx_xml: str) -> List[PackageDependency]:
        """Extract ExecutePackageTask dependencies from a .dtsx file."""
        dependencies = []
        
        try:
            root = etree.fromstring(dtsx_xml.encode('utf-8'))
            
            # Look for ExecutePackageTask executables
            for exec_node in root.xpath('.//DTS:Executable[@DTS:ExecutableType="SSIS.ExecutePackageTask"]', 
                                      namespaces={'DTS': 'www.microsoft.com/SqlServer/Dts'}):
                
                task_name = exec_node.get('{www.microsoft.com/SqlServer/Dts}ObjectName', 'ExecutePackageTask')
                
                # Find the package being executed
                for obj_data in exec_node.xpath('.//DTS:ObjectData', 
                                              namespaces={'DTS': 'www.microsoft.com/SqlServer/Dts'}):
                    
                    # Parse ExecutePackageTask properties
                    for prop in obj_data.xpath('.//ExecutePackageTask:Property', 
                                             namespaces={'ExecutePackageTask': 'www.microsoft.com/SqlServer/Dts/Tasks/ExecutePackageTask'}):
                        
                        prop_name = prop.get('Name')
                        if prop_name == 'PackageName':
                            target_package = prop.text
                            if target_package:
                                dependencies.append(PackageDependency(
                                    parent_package=package_name,
                                    child_package=target_package.replace('.dtsx', ''),
                                    task_name=task_name
                                ))
                
        except etree.XMLSyntaxError as e:
            self.logger.warning(f"Failed to parse package dependencies for {package_name}: {e}")
        
        return dependencies
    
    def analyze_project_structure(self, project: SSISProject) -> Dict:
        """Analyze project structure and execution patterns.
        
        Returns:
            Analysis with entry points, execution chains, and recommendations
        """
        analysis = {
            'entry_points': [],
            'execution_chains': [],
            'isolated_packages': [],
            'circular_dependencies': [],
            'recommendations': []
        }
        
        # Build dependency graph
        dependency_graph = {}
        for pkg in project.packages:
            dependency_graph[pkg.name] = []
        
        for dep in project.dependencies:
            if dep.parent_package in dependency_graph:
                dependency_graph[dep.parent_package].append(dep.child_package)
        
        # Find entry points (packages not executed by others)
        executed_packages = {dep.child_package for dep in project.dependencies}
        entry_points = [pkg.name for pkg in project.packages if pkg.name not in executed_packages]
        analysis['entry_points'] = entry_points
        
        # Find isolated packages (no dependencies in or out)
        isolated = []
        for pkg_name in dependency_graph:
            has_incoming = pkg_name in executed_packages
            has_outgoing = len(dependency_graph[pkg_name]) > 0
            if not has_incoming and not has_outgoing:
                isolated.append(pkg_name)
        analysis['isolated_packages'] = isolated
        
        # Build execution chains
        for entry_point in entry_points:
            chain = self._trace_execution_chain(entry_point, dependency_graph)
            if len(chain) > 1:  # Only include chains with dependencies
                analysis['execution_chains'].append({
                    'entry_point': entry_point,
                    'chain': chain,
                    'length': len(chain)
                })
        
        # Generate recommendations
        if len(entry_points) > 1:
            analysis['recommendations'].append(
                "Multiple entry points detected - consider creating a master DAG for orchestration"
            )
        
        if isolated:
            analysis['recommendations'].append(
                f"Isolated packages found: {isolated} - these can be independent DAGs"
            )
        
        if not project.dependencies:
            analysis['recommendations'].append(
                "No package dependencies found - all packages can run independently"
            )
        
        return analysis
    
    def _trace_execution_chain(self, start_package: str, dependency_graph: Dict[str, List[str]], 
                             visited: Optional[Set[str]] = None) -> List[str]:
        """Trace execution chain starting from a package."""
        if visited is None:
            visited = set()
        
        if start_package in visited:
            return []  # Circular dependency detected
        
        visited.add(start_package)
        chain = [start_package]
        
        # Follow all dependencies
        for child in dependency_graph.get(start_package, []):
            child_chain = self._trace_execution_chain(child, dependency_graph, visited.copy())
            if child_chain:
                chain.extend(child_chain)
        
        return chain