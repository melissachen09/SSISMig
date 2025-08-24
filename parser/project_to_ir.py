"""Project to IR Converter - Orchestrates parsing of entire SSIS projects.

This module ties together project-level parsing and individual package parsing
to produce complete IRProject objects with cross-package dependency analysis.
"""
import logging
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Union

from .dtsx_reader import DTSXReader
from .to_ir import DTSXToIRConverter
from .project_parser import SSISProjectParser, SSISProject, PackageDependency
from models.ir import (
    IRProject, IRPackage, PackageReference, PackageDependency as IRPackageDependency,
    ProjectParameter, ProjectConnectionManager, PrecedenceCondition,
    ConnectionType, ProtectionLevel
)

logger = logging.getLogger(__name__)


class ProjectToIRConverter:
    """Converts complete SSIS projects to IR including cross-package dependencies."""
    
    def __init__(self, password: Optional[str] = None, anthropic_key: Optional[str] = None):
        self.password = password
        self.anthropic_key = anthropic_key
        self.project_parser = SSISProjectParser()
        self.dtsx_converter = DTSXToIRConverter(password=password, anthropic_key=anthropic_key)
        self.logger = logging.getLogger(__name__)
    
    def convert_project(self, project_path: Union[str, Path], save_ir: bool = False) -> IRProject:
        """Convert entire SSIS project to IR.
        
        Args:
            project_path: Path to .ispac file or project directory
            save_ir: Whether to save intermediate IR files
            
        Returns:
            Complete IRProject with all packages and dependencies
        """
        project_path = Path(project_path)
        self.logger.info(f"Converting SSIS project: {project_path}")
        
        # Step 1: Parse project structure
        ssis_project = self.project_parser.parse_project(project_path)
        self.logger.info(f"Found {len(ssis_project.packages)} packages in project {ssis_project.project_name}")
        
        # Step 2: Convert project metadata to IR format
        ir_project = self._build_ir_project_metadata(ssis_project)
        
        # Step 3: Parse each package to IR
        package_irs = {}
        if project_path.suffix.lower() == '.ispac':
            package_irs = self._convert_packages_from_ispac(project_path, ssis_project)
        else:
            package_irs = self._convert_packages_from_directory(project_path, ssis_project)
        
        ir_project.package_irs = package_irs
        
        # Step 4: Analyze dependencies and execution patterns
        self._analyze_project_dependencies(ir_project, ssis_project)
        
        # Step 5: Update package references with analysis results
        self._update_package_references(ir_project)
        
        # Step 6: Save IR files if requested
        if save_ir:
            self._save_ir_files(ir_project, project_path)
        
        self.logger.info(f"Project conversion completed: {len(package_irs)} packages processed")
        return ir_project
    
    def _build_ir_project_metadata(self, ssis_project: SSISProject) -> IRProject:
        """Convert SSIS project metadata to IR format."""
        
        # Convert project parameters
        project_params = []
        for param_name, param_obj in ssis_project.parameters.items():
            project_params.append(ProjectParameter(
                name=param_obj.name,
                data_type=param_obj.data_type,
                value=param_obj.value,
                description=param_obj.description,
                sensitive=param_obj.sensitive
            ))
        
        # Convert project connections
        project_connections = []
        for conn_name, conn_obj in ssis_project.connection_managers.items():
            # Map provider to ConnectionType
            conn_type = self._map_connection_type(conn_obj.provider)
            
            project_connections.append(ProjectConnectionManager(
                id=f"Project.{conn_name}",
                name=conn_obj.name,
                type=conn_type,
                connection_string=conn_obj.connection_string,
                provider=conn_obj.provider,
                description=conn_obj.description,
                protection_level=ProtectionLevel(conn_obj.protection_level)
            ))
        
        # Convert package references
        package_refs = []
        for pkg_ref in ssis_project.packages:
            package_refs.append(PackageReference(
                name=pkg_ref.name,
                file_path=pkg_ref.file_path,
                relative_path=pkg_ref.file_path,
                entry_point=pkg_ref.entry_point
            ))
        
        return IRProject(
            project_name=ssis_project.project_name,
            project_version=ssis_project.project_version,
            target_server_version=ssis_project.target_server_version,
            protection_level=ProtectionLevel(ssis_project.protection_level),
            project_parameters=project_params,
            project_connections=project_connections,
            packages=package_refs
        )
    
    def _convert_packages_from_ispac(self, ispac_path: Path, ssis_project: SSISProject) -> Dict[str, IRPackage]:
        """Convert all packages from .ispac file."""
        package_irs = {}
        
        with zipfile.ZipFile(ispac_path, 'r') as ispac:
            for package_ref in ssis_project.packages:
                try:
                    self.logger.info(f"Converting package: {package_ref.name}")
                    dtsx_content = ispac.read(package_ref.file_path).decode('utf-8')
                    
                    # Convert DTSX content to IR
                    ir_package = self.dtsx_converter.dtsx_string_to_ir(
                        dtsx_content, 
                        package_name=package_ref.name
                    )
                    package_irs[package_ref.name] = ir_package
                    
                except Exception as e:
                    self.logger.error(f"Failed to convert package {package_ref.name}: {e}")
                    # Create minimal IR package for failed conversions
                    package_irs[package_ref.name] = IRPackage(
                        package_name=package_ref.name,
                        executables=[],
                        connection_managers=[],
                        parameters=[],
                        variables=[]
                    )
        
        return package_irs
    
    def _convert_packages_from_directory(self, project_dir: Path, ssis_project: SSISProject) -> Dict[str, IRPackage]:
        """Convert all packages from project directory."""
        package_irs = {}
        
        for package_ref in ssis_project.packages:
            try:
                self.logger.info(f"Converting package: {package_ref.name}")
                dtsx_path = project_dir / package_ref.file_path
                
                # Convert DTSX file to IR
                ir_package = self.dtsx_converter.dtsx_to_ir(dtsx_path)
                package_irs[package_ref.name] = ir_package
                
            except Exception as e:
                self.logger.error(f"Failed to convert package {package_ref.name}: {e}")
                # Create minimal IR package for failed conversions
                package_irs[package_ref.name] = IRPackage(
                    package_name=package_ref.name,
                    executables=[],
                    connection_managers=[],
                    parameters=[],
                    variables=[]
                )
        
        return package_irs
    
    def _analyze_project_dependencies(self, ir_project: IRProject, ssis_project: SSISProject):
        """Analyze cross-package dependencies and execution patterns."""
        
        # Convert package dependencies to IR format
        ir_dependencies = []
        for dep in ssis_project.dependencies:
            # Try to find the precedence constraint for this ExecutePackageTask
            precedence_condition = self._find_execute_package_precedence(
                ir_project.package_irs.get(dep.parent_package),
                dep.task_name
            )
            
            ir_dep = IRPackageDependency(
                parent_package=dep.parent_package,
                child_package=dep.child_package,
                task_name=dep.task_name,
                task_id=f"{dep.parent_package}\\{dep.task_name}",
                precedence_constraint=precedence_condition
            )
            ir_dependencies.append(ir_dep)
        
        ir_project.dependencies = ir_dependencies
        
        # Analyze project structure
        analysis = self.project_parser.analyze_project_structure(ssis_project)
        ir_project.entry_points = analysis['entry_points']
        ir_project.execution_chains = analysis['execution_chains']
        ir_project.isolated_packages = analysis['isolated_packages']
        
        self.logger.info(f"Dependency analysis: {len(ir_dependencies)} dependencies, "
                        f"{len(analysis['entry_points'])} entry points")
    
    def _find_execute_package_precedence(self, package_ir: Optional[IRPackage], task_name: str) -> Optional[PrecedenceCondition]:
        """Find precedence constraint condition for ExecutePackageTask."""
        if not package_ir:
            return None
        
        # Look for ExecutePackageTask in executables
        execute_pkg_task = None
        for exe in package_ir.executables:
            if exe.object_name == task_name and exe.type.value == "ExecutePackage":
                execute_pkg_task = exe
                break
        
        if not execute_pkg_task:
            return PrecedenceCondition.SUCCESS  # Default
        
        # Look for precedence constraint targeting this task
        for edge in package_ir.edges:
            if edge.to_task == execute_pkg_task.id:
                return edge.condition
        
        return PrecedenceCondition.SUCCESS  # Default if no constraint found
    
    def _update_package_references(self, ir_project: IRProject):
        """Update package references with analysis results."""
        
        for pkg_ref in ir_project.packages:
            # Check if package is transformation-only
            pkg_ir = ir_project.package_irs.get(pkg_ref.name)
            if pkg_ir:
                pkg_ref.transformation_only = pkg_ir.is_transformation_only()
            
            # Mark entry points
            pkg_ref.entry_point = pkg_ref.name in ir_project.entry_points
    
    def _map_connection_type(self, provider: str) -> ConnectionType:
        """Map SSIS connection provider to ConnectionType enum."""
        provider_mapping = {
            'OLEDB': ConnectionType.OLEDB,
            'ADONET': ConnectionType.ADONET,
            'FLATFILE': ConnectionType.FLATFILE,
            'FILE': ConnectionType.FILE,
            'HTTP': ConnectionType.HTTP,
            'FTP': ConnectionType.FTP,
            'SMTP': ConnectionType.SMTP,
        }
        
        # Try exact match first
        if provider in provider_mapping:
            return provider_mapping[provider]
        
        # Try partial matches
        provider_upper = provider.upper()
        for key, value in provider_mapping.items():
            if key in provider_upper:
                return value
        
        # Default to OLEDB for database connections
        return ConnectionType.OLEDB
    
    def _save_ir_files(self, ir_project: IRProject, project_path: Path):
        """Save IR files for debugging and inspection."""
        
        # Create output directory
        output_dir = project_path.parent / f"{project_path.stem}_ir"
        output_dir.mkdir(exist_ok=True)
        
        # Save project-level IR
        project_ir_file = output_dir / "project.json"
        project_ir_file.write_text(ir_project.json(indent=2), encoding='utf-8')
        self.logger.info(f"Saved project IR: {project_ir_file}")
        
        # Save individual package IRs
        packages_dir = output_dir / "packages"
        packages_dir.mkdir(exist_ok=True)
        
        for pkg_name, pkg_ir in ir_project.package_irs.items():
            pkg_file = packages_dir / f"{pkg_name}.json"
            pkg_file.write_text(pkg_ir.json(indent=2), encoding='utf-8')
            self.logger.debug(f"Saved package IR: {pkg_file}")
        
        self.logger.info(f"IR files saved to: {output_dir}")


def convert_project_to_ir(
    project_path: Union[str, Path],
    password: Optional[str] = None,
    anthropic_key: Optional[str] = None,
    save_ir: bool = False
) -> IRProject:
    """Convert SSIS project to IR - main entry point function.
    
    Args:
        project_path: Path to .ispac file or project directory
        password: Password for encrypted packages
        anthropic_key: Claude API key for enhanced generation
        save_ir: Whether to save intermediate IR files
        
    Returns:
        Complete IRProject with all packages and dependencies
    """
    converter = ProjectToIRConverter(password=password, anthropic_key=anthropic_key)
    return converter.convert_project(project_path, save_ir=save_ir)