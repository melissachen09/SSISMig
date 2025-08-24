"""Test suite for SSIS project migration functionality.

Tests the complete project migration pipeline from .ispac parsing 
to DAG and dbt project generation.
"""
import pytest
import tempfile
import zipfile
import json
from pathlib import Path
from unittest.mock import Mock, patch

from parser.project_parser import SSISProjectParser, SSISProject
from parser.project_to_ir import ProjectToIRConverter
from generators.master_dag_gen import MasterDAGGenerator
from generators.project_dbt_gen import ProjectDBTGenerator
from models.ir import IRProject, IRPackage, ExecutableType, ComponentType
from ssis_project_migrator import cli


@pytest.fixture
def sample_ispac_content():
    """Sample ISPAC file content for testing."""
    return {
        '@Project.manifest': '''<?xml version="1.0" encoding="utf-8"?>
<Project Name="TestProject" Version="1.0" ProtectionLevel="DontSaveSensitive" 
         TargetServerVersion="SQL2019" xmlns="www.microsoft.com/SqlServer/Dts/Project" />
''',
        'Project.params': '''<?xml version="1.0" encoding="utf-8"?>
<Parameters xmlns="www.microsoft.com/SqlServer/Dts">
  <Parameter Name="ProjectParam1" DataType="String" Sensitive="false">
    <ParameterValue>TestValue</ParameterValue>
  </Parameter>
</Parameters>
''',
        'Project.conmgr': '''<?xml version="1.0" encoding="utf-8"?>
<ConnectionManagers xmlns="www.microsoft.com/SqlServer/Dts">
  <ConnectionManager Name="TestConnection" CreationName="OLEDB">
    <ConnectionString>Data Source=localhost;Initial Catalog=TestDB</ConnectionString>
  </ConnectionManager>
</ConnectionManagers>
''',
        'Package1.dtsx': '''<?xml version="1.0" encoding="utf-8"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts">
  <DTS:Property DTS:Name="PackageName">Package1</DTS:Property>
  <DTS:Executables>
    <DTS:Executable DTS:ObjectName="ExecuteSQL1" DTS:ExecutableType="Microsoft.ExecuteSQLTask">
      <DTS:ObjectData>
        <SQLTask:SqlTaskData xmlns:SQLTask="www.microsoft.com/SqlServer/Dts/Tasks/SQLTask">
          <SQLTask:SqlStatementSource>SELECT * FROM TestTable</SQLTask:SqlStatementSource>
        </SQLTask:SqlTaskData>
      </DTS:ObjectData>
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>
''',
        'Package2.dtsx': '''<?xml version="1.0" encoding="utf-8"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts">
  <DTS:Property DTS:Name="PackageName">Package2</DTS:Property>
  <DTS:Executables>
    <DTS:Executable DTS:ObjectName="ExecutePackage1" DTS:ExecutableType="SSIS.ExecutePackageTask">
      <DTS:ObjectData>
        <ExecutePackageTask:ExecutePackageTaskData xmlns:ExecutePackageTask="www.microsoft.com/SqlServer/Dts/Tasks/ExecutePackageTask">
          <ExecutePackageTask:Property Name="PackageName">Package1.dtsx</ExecutePackageTask:Property>
        </ExecutePackageTask:ExecutePackageTaskData>
      </DTS:ObjectData>
    </DTS:Executable>
    <DTS:Executable DTS:ObjectName="DataFlow1" DTS:ExecutableType="SSIS.Pipeline.2">
      <DTS:ObjectData>
        <pipeline xmlns="www.microsoft.com/SqlServer/Dts/Pipeline">
          <components>
            <component name="OLEDBSource" componentClassID="DTSAdapter.OLEDBSource.1">
              <properties>
                <property name="SqlCommand">SELECT * FROM SourceTable</property>
              </properties>
            </component>
            <component name="OLEDBDestination" componentClassID="DTSAdapter.OLEDBDestination.1">
              <properties>
                <property name="OpenRowset">[dbo].[DestinationTable]</property>
              </properties>
            </component>
          </components>
        </pipeline>
      </DTS:ObjectData>
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>
'''
    }


@pytest.fixture
def temp_ispac_file(sample_ispac_content):
    """Create a temporary .ispac file for testing."""
    with tempfile.NamedTemporaryFile(suffix='.ispac', delete=False) as f:
        with zipfile.ZipFile(f.name, 'w') as zipf:
            for filename, content in sample_ispac_content.items():
                zipf.writestr(filename, content)
        
        yield Path(f.name)
    
    # Cleanup
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def temp_project_dir(sample_ispac_content):
    """Create a temporary project directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        project_dir = Path(temp_dir)
        
        # Create project file
        (project_dir / "TestProject.dtproj").write_text('''<?xml version="1.0" encoding="utf-8"?>
<Project xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <PropertyGroup>
    <TargetServerVersion>SQL2019</TargetServerVersion>
    <ProtectionLevel>DontSaveSensitive</ProtectionLevel>
  </PropertyGroup>
</Project>
''')
        
        # Create other files (excluding manifest)
        for filename, content in sample_ispac_content.items():
            if not filename.startswith('@'):
                (project_dir / filename).write_text(content)
        
        yield project_dir


class TestSSISProjectParser:
    """Test SSIS project parsing functionality."""
    
    def test_parse_ispac_file(self, temp_ispac_file):
        """Test parsing .ispac file."""
        parser = SSISProjectParser()
        project = parser.parse_project(temp_ispac_file)
        
        assert project.project_name == "TestProject"
        assert project.project_version == "1.0"
        assert len(project.packages) == 2
        assert len(project.parameters) == 1
        assert len(project.connection_managers) == 1
        assert len(project.dependencies) == 1
        
        # Check dependency
        dep = project.dependencies[0]
        assert dep.parent_package == "Package2"
        assert dep.child_package == "Package1"
    
    def test_parse_project_directory(self, temp_project_dir):
        """Test parsing project directory."""
        parser = SSISProjectParser()
        project = parser.parse_project(temp_project_dir)
        
        assert project.project_name == "TestProject"
        assert len(project.packages) == 2
    
    def test_analyze_project_structure(self, temp_ispac_file):
        """Test project structure analysis."""
        parser = SSISProjectParser()
        project = parser.parse_project(temp_ispac_file)
        
        analysis = parser.analyze_project_structure(project)
        
        assert 'entry_points' in analysis
        assert 'execution_chains' in analysis
        assert 'Package2' in analysis['entry_points']  # Not executed by others
        assert len(analysis['execution_chains']) > 0


class TestProjectToIRConverter:
    """Test project-to-IR conversion."""
    
    def test_convert_ispac_to_ir(self, temp_ispac_file):
        """Test converting .ispac to IR."""
        converter = ProjectToIRConverter()
        ir_project = converter.convert_project(temp_ispac_file)
        
        assert isinstance(ir_project, IRProject)
        assert ir_project.project_name == "TestProject"
        assert len(ir_project.packages) == 2
        assert len(ir_project.package_irs) == 2
        assert len(ir_project.dependencies) == 1
        assert len(ir_project.project_parameters) == 1
        assert len(ir_project.project_connections) == 1
    
    def test_dependency_analysis(self, temp_ispac_file):
        """Test cross-package dependency analysis."""
        converter = ProjectToIRConverter()
        ir_project = converter.convert_project(temp_ispac_file)
        
        # Check dependency analysis
        assert len(ir_project.entry_points) == 1
        assert 'Package2' in ir_project.entry_points
        
        # Check migration strategy recommendation
        strategy = ir_project.recommend_migration_strategy()
        assert 'strategy' in strategy
        assert 'components' in strategy


class TestMasterDAGGenerator:
    """Test master DAG generation."""
    
    @pytest.fixture
    def sample_ir_project(self):
        """Create sample IR project for testing."""
        from models.ir import (
            IRProject, IRPackage, PackageReference, 
            ProjectParameter, ProjectConnectionManager,
            PackageDependency as IRPackageDependency,
            Executable, PrecedenceCondition, ConnectionType, ProtectionLevel
        )
        
        # Create sample packages
        pkg1_ir = IRPackage(
            package_name="Package1",
            executables=[
                Executable(
                    id="Package1\\ExecuteSQL1",
                    type=ExecutableType.EXECUTE_SQL,
                    object_name="ExecuteSQL1",
                    sql="SELECT * FROM TestTable"
                )
            ]
        )
        
        pkg2_ir = IRPackage(
            package_name="Package2",
            executables=[
                Executable(
                    id="Package2\\ExecutePackage1",
                    type=ExecutableType.EXECUTE_PACKAGE,
                    object_name="ExecutePackage1"
                ),
                Executable(
                    id="Package2\\DataFlow1",
                    type=ExecutableType.DATA_FLOW,
                    object_name="DataFlow1"
                )
            ]
        )
        
        ir_project = IRProject(
            project_name="TestProject",
            project_version="1.0",
            packages=[
                PackageReference(name="Package1", file_path="Package1.dtsx", relative_path="Package1.dtsx"),
                PackageReference(name="Package2", file_path="Package2.dtsx", relative_path="Package2.dtsx")
            ],
            package_irs={
                "Package1": pkg1_ir,
                "Package2": pkg2_ir
            },
            dependencies=[
                IRPackageDependency(
                    parent_package="Package2",
                    child_package="Package1",
                    task_name="ExecutePackage1",
                    task_id="Package2\\ExecutePackage1",
                    precedence_constraint=PrecedenceCondition.SUCCESS
                )
            ],
            entry_points=["Package2"],
            project_parameters=[
                ProjectParameter(name="ProjectParam1", data_type="String", value="TestValue")
            ],
            project_connections=[
                ProjectConnectionManager(
                    id="Project.TestConnection",
                    name="TestConnection",
                    type=ConnectionType.OLEDB,
                    connection_string="Data Source=localhost;Initial Catalog=TestDB",
                    provider="OLEDB"
                )
            ]
        )
        
        return ir_project
    
    def test_generate_master_dag(self, sample_ir_project):
        """Test master DAG generation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            
            generator = MasterDAGGenerator()
            generated_files = generator.generate_master_dag(sample_ir_project, output_dir)
            
            assert 'master' in generated_files
            assert 'Package1' in generated_files
            assert 'Package2' in generated_files
            assert 'connections' in generated_files
            
            # Verify master DAG was created
            master_dag_file = Path(generated_files['master'])
            assert master_dag_file.exists()
            
            # Check content contains expected elements
            content = master_dag_file.read_text()
            assert 'testproject_master' in content
            assert 'TriggerDagRunOperator' in content
            assert 'ExternalTaskSensor' in content


class TestProjectDBTGenerator:
    """Test project-level dbt generation."""
    
    def test_generate_unified_dbt_project(self, sample_ir_project):
        """Test generating unified dbt project."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            
            generator = ProjectDBTGenerator()
            result = generator.generate_project_dbt(sample_ir_project, output_dir)
            
            # Check basic structure was created
            assert (output_dir / "dbt_project.yml").exists()
            assert (output_dir / "profiles.yml.template").exists()
            assert (output_dir / "README.md").exists()
            assert (output_dir / "models").exists()
            
            # Check if models were generated for packages
            if result['success']:
                assert len(result['models']) > 0


class TestCLIIntegration:
    """Test CLI integration."""
    
    def test_analyze_command(self, temp_ispac_file):
        """Test the analyze command."""
        from click.testing import CliRunner
        
        runner = CliRunner()
        result = runner.invoke(cli, ['analyze', str(temp_ispac_file), '--verbose'])
        
        assert result.exit_code == 0
        assert 'SSIS Project Analysis' in result.output
        assert 'TestProject' in result.output
    
    def test_migrate_command_auto_mode(self, temp_ispac_file):
        """Test the migrate command in auto mode."""
        from click.testing import CliRunner
        
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = CliRunner()
            result = runner.invoke(cli, [
                'migrate',
                '--project', str(temp_ispac_file),
                '--out', temp_dir,
                '--mode', 'auto',
                '--verbose'
            ])
            
            # Command should complete (may have warnings but not fail)
            assert result.exit_code == 0
            
            # Check that some output files were created
            output_path = Path(temp_dir)
            generated_files = list(output_path.rglob('*'))
            assert len(generated_files) > 0


class TestEndToEndMigration:
    """End-to-end migration tests."""
    
    def test_complete_migration_workflow(self, temp_ispac_file):
        """Test complete migration from .ispac to generated artifacts."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            
            # Step 1: Parse project
            from parser.project_to_ir import convert_project_to_ir
            ir_project = convert_project_to_ir(temp_ispac_file)
            
            assert ir_project.project_name == "TestProject"
            assert len(ir_project.packages) == 2
            
            # Step 2: Generate Airflow DAGs
            from generators.master_dag_gen import MasterDAGGenerator
            airflow_dir = output_dir / 'airflow'
            airflow_dir.mkdir()
            
            dag_gen = MasterDAGGenerator()
            airflow_files = dag_gen.generate_master_dag(ir_project, airflow_dir)
            
            assert len(airflow_files) > 0
            assert any('.py' in str(f) for f in airflow_files.values())
            
            # Step 3: Check migration strategy
            strategy = ir_project.recommend_migration_strategy()
            assert strategy['strategy'] in ['airflow_only', 'mixed', 'dbt_only', 'dbt_with_orchestration']
            
            # Step 4: Verify files exist
            for file_path in airflow_files.values():
                assert Path(file_path).exists()


@pytest.fixture
def complex_project_example():
    """Create a more complex project example for testing."""
    # This would be expanded with more complex scenarios
    pass


# Integration tests
class TestRealWorldScenarios:
    """Test real-world migration scenarios."""
    
    def test_large_project_performance(self):
        """Test performance with larger projects."""
        # This would test with projects having 10+ packages
        pass
    
    def test_encrypted_package_handling(self):
        """Test handling of encrypted packages."""
        pass
    
    def test_complex_dependency_chains(self):
        """Test complex dependency chains and circular dependencies."""
        pass
    
    def test_mixed_transformation_orchestration(self):
        """Test projects with both transformation and orchestration packages."""
        pass


if __name__ == '__main__':
    pytest.main([__file__, '-v'])