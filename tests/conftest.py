"""Test configuration and utilities."""
import pytest
import tempfile
from pathlib import Path
from typing import Generator
import logging

# Configure test logging
logging.basicConfig(level=logging.DEBUG)


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Provide a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture  
def sample_dtsx_simple() -> str:
    """Simple DTSX XML for testing."""
    return '''<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
                xmlns:SQLTask="www.microsoft.com/sqlserver/dts/tasks/sqltask">
  <DTS:Property DTS:Name="PackageFormatVersion">8</DTS:Property>
  <DTS:Property DTS:Name="VersionBuild">1</DTS:Property>
  <DTS:Property DTS:Name="VersionGUID">{12345678-1234-1234-1234-123456789012}</DTS:Property>
  <DTS:Property DTS:Name="PackageType">5</DTS:Property>
  <DTS:Property DTS:Name="ProtectionLevel">1</DTS:Property>
  <DTS:Property DTS:Name="ObjectName">TestPackage</DTS:Property>
  <DTS:Property DTS:Name="DTSID">{12345678-1234-1234-1234-123456789012}</DTS:Property>
  <DTS:Property DTS:Name="CreationName">Microsoft.Package</DTS:Property>
  
  <DTS:Variables>
    <DTS:Variable>
      <DTS:Property DTS:Name="ObjectName">User::BatchID</DTS:Property>
      <DTS:Property DTS:Name="DTSID">{87654321-4321-4321-4321-210987654321}</DTS:Property>
      <DTS:Property DTS:Name="DataType">3</DTS:Property>
      <DTS:Property DTS:Name="Value">0</DTS:Property>
    </DTS:Variable>
  </DTS:Variables>
  
  <DTS:ConnectionManagers>
    <DTS:ConnectionManager>
      <DTS:Property DTS:Name="refId">Package.ConnectionManagers[TestDB]</DTS:Property>
      <DTS:Property DTS:Name="ObjectName">TestDB</DTS:Property>
      <DTS:Property DTS:Name="DTSID">{ABCDEF12-3456-7890-ABCD-EF1234567890}</DTS:Property>
      <DTS:Property DTS:Name="CreationName">OLEDB</DTS:Property>
    </DTS:ConnectionManager>
  </DTS:ConnectionManagers>
  
  <DTS:Executables>
    <DTS:Executable DTS:refId="Package\\ExecuteSQL1" 
                   DTS:ExecutableType="Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ExecuteSQLTask"
                   DTS:ObjectName="ExecuteSQL1">
      <DTS:ObjectData>
        <SQLTask:SqlTaskData SQLTask:SqlStatementSource="SELECT GETDATE() as CurrentTime" 
                            SQLTask:ResultType="ResultSetType_None" />
      </DTS:ObjectData>
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>'''


@pytest.fixture
def sample_dtsx_with_dataflow() -> str:
    """DTSX XML with data flow for testing."""
    return '''<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
                xmlns:Pipeline="www.microsoft.com/sqlserver/dts/pipeline">
  <DTS:Property DTS:Name="ObjectName">DataFlowPackage</DTS:Property>
  <DTS:Property DTS:Name="ProtectionLevel">1</DTS:Property>
  
  <DTS:Executables>
    <DTS:Executable DTS:refId="Package\\DataFlow1"
                   DTS:ExecutableType="Microsoft.SqlServer.Dts.Pipeline.Wrapper.TaskHost"
                   DTS:ObjectName="DataFlow1">
      <DTS:ObjectData>
        <Pipeline:Pipeline>
          <Pipeline:components>
            <Pipeline:component id="1" name="OLEDBSource" componentClassID="Microsoft.OLEDBSource">
              <Pipeline:properties>
                <Pipeline:property name="SqlCommand">SELECT * FROM Orders</Pipeline:property>
              </Pipeline:properties>
            </Pipeline:component>
            <Pipeline:component id="2" name="DerivedColumn1" componentClassID="Microsoft.DerivedColumn">
              <Pipeline:properties>
                <Pipeline:property name="Expression">OrderDate + 30</Pipeline:property>
              </Pipeline:properties>
            </Pipeline:component>
            <Pipeline:component id="3" name="OLEDBDestination" componentClassID="Microsoft.OLEDBDestination">
              <Pipeline:properties>
                <Pipeline:property name="TableOrViewName">ProcessedOrders</Pipeline:property>
              </Pipeline:properties>
            </Pipeline:component>
          </Pipeline:components>
          <Pipeline:paths>
            <Pipeline:path id="path1" startId="1" endId="2" />
            <Pipeline:path id="path2" startId="2" endId="3" />
          </Pipeline:paths>
        </Pipeline:Pipeline>
      </DTS:ObjectData>
    </DTS:Executable>
  </DTS:Executables>
  
  <DTS:PrecedenceConstraints>
    <DTS:PrecedenceConstraint DTS:From="Package\\DataFlow1" 
                            DTS:To="Package\\DataFlow2" 
                            DTS:Value="Success" />
  </DTS:PrecedenceConstraints>
</DTS:Executable>'''