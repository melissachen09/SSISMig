"""Sample DTSX files for testing and examples."""

# Simple package with one Execute SQL task
SIMPLE_PACKAGE = '''<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
                xmlns:SQLTask="www.microsoft.com/sqlserver/dts/tasks/sqltask"
                DTS:refId="Package"
                DTS:ExecutableType="Microsoft.Package"
                DTS:ObjectName="SimpleETLPackage"
                DTS:DTSID="{12345678-ABCD-1234-EFGH-123456789012}"
                DTS:VersionGUID="{87654321-DCBA-4321-HGFE-210987654321}">
  
  <DTS:Property DTS:Name="PackageFormatVersion">8</DTS:Property>
  <DTS:Property DTS:Name="ProtectionLevel">1</DTS:Property>
  <DTS:Property DTS:Name="CreationDate">2024-01-01T10:00:00</DTS:Property>
  <DTS:Property DTS:Name="CreatorName">Migration Tool</DTS:Property>
  
  <DTS:Variables>
    <DTS:Variable DTS:refId="Package.Variables[User::BatchID]"
                  DTS:ObjectName="User::BatchID"
                  DTS:DTSID="{11111111-2222-3333-4444-555555555555}">
      <DTS:Property DTS:Name="DataType">3</DTS:Property>
      <DTS:Property DTS:Name="Value">1001</DTS:Property>
    </DTS:Variable>
    <DTS:Variable DTS:refId="Package.Variables[User::ProcessDate]"
                  DTS:ObjectName="User::ProcessDate"
                  DTS:DTSID="{22222222-3333-4444-5555-666666666666}">
      <DTS:Property DTS:Name="DataType">7</DTS:Property>
      <DTS:Property DTS:Name="Value">2024-01-01</DTS:Property>
    </DTS:Variable>
  </DTS:Variables>
  
  <DTS:ConnectionManagers>
    <DTS:ConnectionManager DTS:refId="Package.ConnectionManagers[SourceDB]"
                          DTS:ObjectName="SourceDB"
                          DTS:DTSID="{AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE}">
      <DTS:Property DTS:Name="CreationName">OLEDB</DTS:Property>
      <DTS:PropertyExpression DTS:Name="ConnectionString">"Data Source=localhost;Initial Catalog=SourceDB;Provider=SQLNCLI11;Integrated Security=SSPI;"</DTS:PropertyExpression>
    </DTS:ConnectionManager>
    <DTS:ConnectionManager DTS:refId="Package.ConnectionManagers[TargetDB]"
                          DTS:ObjectName="TargetDB"
                          DTS:DTSID="{BBBBBBBB-CCCC-DDDD-EEEE-FFFFFFFFFFFF}">
      <DTS:Property DTS:Name="CreationName">SNOWFLAKE</DTS:Property>
    </DTS:ConnectionManager>
  </DTS:ConnectionManagers>
  
  <DTS:Executables>
    <DTS:Executable DTS:refId="Package\\CleanupTask"
                   DTS:ExecutableType="Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ExecuteSQLTask"
                   DTS:ObjectName="CleanupTask"
                   DTS:DTSID="{CCCCCCCC-DDDD-EEEE-FFFF-111111111111}">
      <DTS:ObjectData>
        <SQLTask:SqlTaskData SQLTask:SqlStatementSource="DELETE FROM staging_orders WHERE process_date &lt; DATEADD(day, -7, GETDATE())"
                            SQLTask:Connection="Package.ConnectionManagers[SourceDB]" />
      </DTS:ObjectData>
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>'''


# Complex package with data flow
COMPLEX_DATAFLOW_PACKAGE = '''<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
                xmlns:SQLTask="www.microsoft.com/sqlserver/dts/tasks/sqltask"
                xmlns:Pipeline="www.microsoft.com/sqlserver/dts/pipeline"
                DTS:refId="Package"
                DTS:ExecutableType="Microsoft.Package"
                DTS:ObjectName="OrderProcessingETL"
                DTS:DTSID="{ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB}">
  
  <DTS:Property DTS:Name="PackageFormatVersion">8</DTS:Property>
  <DTS:Property DTS:Name="ProtectionLevel">1</DTS:Property>
  
  <DTS:Variables>
    <DTS:Variable DTS:refId="Package.Variables[User::StartDate]"
                  DTS:ObjectName="User::StartDate"
                  DTS:DTSID="{12121212-3434-5656-7878-909090909090}">
      <DTS:Property DTS:Name="DataType">7</DTS:Property>
      <DTS:Property DTS:Name="Value">2024-01-01</DTS:Property>
    </DTS:Variable>
  </DTS:Variables>
  
  <DTS:ConnectionManagers>
    <DTS:ConnectionManager DTS:refId="Package.ConnectionManagers[OrdersDB]"
                          DTS:ObjectName="OrdersDB"
                          DTS:DTSID="{FEDCBA98-7654-3210-FEDC-BA9876543210}">
      <DTS:Property DTS:Name="CreationName">OLEDB</DTS:Property>
    </DTS:ConnectionManager>
  </DTS:ConnectionManagers>
  
  <DTS:Executables>
    <!-- Execute SQL Task -->
    <DTS:Executable DTS:refId="Package\\PrepareStaging"
                   DTS:ExecutableType="Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ExecuteSQLTask"
                   DTS:ObjectName="PrepareStaging"
                   DTS:DTSID="{11111111-1111-1111-1111-111111111111}">
      <DTS:ObjectData>
        <SQLTask:SqlTaskData SQLTask:SqlStatementSource="TRUNCATE TABLE staging_orders"
                            SQLTask:Connection="Package.ConnectionManagers[OrdersDB]" />
      </DTS:ObjectData>
    </DTS:Executable>
    
    <!-- Data Flow Task -->
    <DTS:Executable DTS:refId="Package\\LoadOrders"
                   DTS:ExecutableType="Microsoft.SqlServer.Dts.Pipeline.Wrapper.TaskHost"
                   DTS:ObjectName="LoadOrders"
                   DTS:DTSID="{22222222-2222-2222-2222-222222222222}">
      <DTS:ObjectData>
        <Pipeline:Pipeline>
          <Pipeline:components>
            
            <!-- Source Component -->
            <Pipeline:component id="1" name="OrdersSource" componentClassID="Microsoft.OLEDBSource">
              <Pipeline:properties>
                <Pipeline:property name="SqlCommand">
                  SELECT order_id, customer_id, order_date, order_total, status
                  FROM orders 
                  WHERE order_date >= ? AND status = 'ACTIVE'
                </Pipeline:property>
                <Pipeline:property name="Connection">Package.ConnectionManagers[OrdersDB]</Pipeline:property>
              </Pipeline:properties>
            </Pipeline:component>
            
            <!-- Derived Column Transformation -->
            <Pipeline:component id="2" name="AddCalculatedFields" componentClassID="Microsoft.DerivedColumn">
              <Pipeline:properties>
                <Pipeline:property name="Expression">order_total * 0.08</Pipeline:property>
                <Pipeline:property name="FriendlyExpression">Tax Amount</Pipeline:property>
              </Pipeline:properties>
            </Pipeline:component>
            
            <!-- Lookup Transformation -->
            <Pipeline:component id="3" name="CustomerLookup" componentClassID="Microsoft.Lookup">
              <Pipeline:properties>
                <Pipeline:property name="SqlCommand">SELECT customer_id, customer_name, customer_type FROM dim_customer</Pipeline:property>
                <Pipeline:property name="JoinKeys">customer_id</Pipeline:property>
              </Pipeline:properties>
            </Pipeline:component>
            
            <!-- Conditional Split -->
            <Pipeline:component id="4" name="SplitByAmount" componentClassID="Microsoft.ConditionalSplit">
              <Pipeline:properties>
                <Pipeline:property name="Expression">order_total > 1000</Pipeline:property>
              </Pipeline:properties>
            </Pipeline:component>
            
            <!-- High Value Orders Destination -->
            <Pipeline:component id="5" name="HighValueDest" componentClassID="Microsoft.OLEDBDestination">
              <Pipeline:properties>
                <Pipeline:property name="TableOrViewName">high_value_orders</Pipeline:property>
                <Pipeline:property name="AccessMode">FastLoad</Pipeline:property>
                <Pipeline:property name="Connection">Package.ConnectionManagers[OrdersDB]</Pipeline:property>
              </Pipeline:properties>
            </Pipeline:component>
            
            <!-- Regular Orders Destination -->
            <Pipeline:component id="6" name="RegularOrdersDest" componentClassID="Microsoft.OLEDBDestination">
              <Pipeline:properties>
                <Pipeline:property name="TableOrViewName">regular_orders</Pipeline:property>
                <Pipeline:property name="AccessMode">FastLoad</Pipeline:property>
                <Pipeline:property name="Connection">Package.ConnectionManagers[OrdersDB]</Pipeline:property>
              </Pipeline:properties>
            </Pipeline:component>
            
          </Pipeline:components>
          
          <Pipeline:paths>
            <Pipeline:path id="path1" startId="1" endId="2" />
            <Pipeline:path id="path2" startId="2" endId="3" />
            <Pipeline:path id="path3" startId="3" endId="4" />
            <Pipeline:path id="path4" startId="4" endId="5" name="HighValue" />
            <Pipeline:path id="path5" startId="4" endId="6" name="RegularValue" />
          </Pipeline:paths>
          
        </Pipeline:Pipeline>
      </DTS:ObjectData>
    </DTS:Executable>
    
    <!-- Final Cleanup Task -->
    <DTS:Executable DTS:refId="Package\\FinalCleanup"
                   DTS:ExecutableType="Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ExecuteSQLTask"
                   DTS:ObjectName="FinalCleanup"
                   DTS:DTSID="{33333333-3333-3333-3333-333333333333}">
      <DTS:ObjectData>
        <SQLTask:SqlTaskData SQLTask:SqlStatementSource="UPDATE processing_log SET end_time = GETDATE() WHERE batch_id = ?"
                            SQLTask:Connection="Package.ConnectionManagers[OrdersDB]" />
      </DTS:ObjectData>
    </DTS:Executable>
    
  </DTS:Executables>
  
  <DTS:PrecedenceConstraints>
    <DTS:PrecedenceConstraint DTS:refId="Package.PrecedenceConstraints[Constraint1]"
                             DTS:From="Package\\PrepareStaging"
                             DTS:To="Package\\LoadOrders"
                             DTS:Value="Success"
                             DTS:LogicalAnd="True" />
    
    <DTS:PrecedenceConstraint DTS:refId="Package.PrecedenceConstraints[Constraint2]"
                             DTS:From="Package\\LoadOrders"
                             DTS:To="Package\\FinalCleanup"
                             DTS:Value="Success"
                             DTS:LogicalAnd="True" />
  </DTS:PrecedenceConstraints>
  
</DTS:Executable>'''


# Package with containers and loops  
CONTAINER_PACKAGE = '''<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
                xmlns:ForEach="www.microsoft.com/sqlserver/dts/tasks/foreachloop"
                DTS:refId="Package"
                DTS:ExecutableType="Microsoft.Package"
                DTS:ObjectName="FileProcessingPackage">
  
  <DTS:Property DTS:Name="PackageFormatVersion">8</DTS:Property>
  <DTS:Property DTS:Name="ProtectionLevel">1</DTS:Property>
  
  <DTS:ConnectionManagers>
    <DTS:ConnectionManager DTS:refId="Package.ConnectionManagers[FileConnection]"
                          DTS:ObjectName="FileConnection"
                          DTS:DTSID="{FILE1111-2222-3333-4444-555555555555}">
      <DTS:Property DTS:Name="CreationName">FILE</DTS:Property>
    </DTS:ConnectionManager>
  </DTS:ConnectionManagers>
  
  <DTS:Executables>
    
    <!-- Sequence Container -->
    <DTS:Executable DTS:refId="Package\\ProcessFiles"
                   DTS:ExecutableType="STOCK:SEQUENCE"
                   DTS:ObjectName="ProcessFiles"
                   DTS:DTSID="{SEQ11111-2222-3333-4444-555555555555}">
      
      <DTS:Executables>
        
        <!-- ForEach Loop Container -->
        <DTS:Executable DTS:refId="Package\\ProcessFiles\\FileLoop"
                       DTS:ExecutableType="STOCK:FOREACHLOOP"
                       DTS:ObjectName="FileLoop"
                       DTS:DTSID="{LOOP1111-2222-3333-4444-555555555555}">
          
          <DTS:ObjectData>
            <ForEach:ForEachData>
              <ForEach:ForEachFileEnumerator>
                <ForEach:Property name="Directory">C:\\Input\\Files</ForEach:Property>
                <ForEach:Property name="FileSpec">*.csv</ForEach:Property>
              </ForEach:ForEachFileEnumerator>
            </ForEach:ForEachData>
          </DTS:ObjectData>
          
          <DTS:Executables>
            
            <!-- File System Task inside loop -->
            <DTS:Executable DTS:refId="Package\\ProcessFiles\\FileLoop\\MoveFile"
                           DTS:ExecutableType="Microsoft.FileSystemTask"
                           DTS:ObjectName="MoveFile"
                           DTS:DTSID="{MOVE1111-2222-3333-4444-555555555555}" />
                           
          </DTS:Executables>
          
        </DTS:Executable>
        
      </DTS:Executables>
      
    </DTS:Executable>
    
  </DTS:Executables>
  
</DTS:Executable>'''


def write_sample_files(output_dir: str):
    """Write sample DTSX files to output directory."""
    from pathlib import Path
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    samples = {
        "simple_etl_package.dtsx": SIMPLE_PACKAGE,
        "complex_dataflow_package.dtsx": COMPLEX_DATAFLOW_PACKAGE, 
        "container_package.dtsx": CONTAINER_PACKAGE
    }
    
    created_files = []
    
    for filename, content in samples.items():
        file_path = output_path / filename
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        created_files.append(str(file_path))
    
    return created_files


if __name__ == "__main__":
    # Create sample files in samples directory
    created = write_sample_files("./samples")
    print(f"Created {len(created)} sample DTSX files:")
    for file_path in created:
        print(f"  - {file_path}")