# SSIS Project Migration Examples

This directory contains complete examples of SSIS project migrations using the enhanced project migration tool.

## Example 1: Multi-Package ETL Project

### Source Structure
```
DataWarehouse.ispac
├── @Project.manifest
├── Project.params
├── Project.conmgr  
├── Extract_Package.dtsx     # Data extraction from multiple sources
├── Transform_Package.dtsx   # Data transformations (dbt candidate)
├── Load_Package.dtsx        # Data loading and aggregations
└── Master_Package.dtsx      # Orchestrates Extract → Transform → Load
```

### Migration Command
```bash
ssis-project-migrate --project DataWarehouse.ispac --out ./migrated --mode auto
```

### Generated Output
```
migrated/
├── MIGRATION_SUMMARY.md
├── airflow/
│   ├── datawarehouse_master_dag.py     # Master orchestration DAG
│   ├── extract_package_dag.py          # Individual package DAGs
│   ├── transform_package_dag.py
│   ├── load_package_dag.py
│   ├── master_package_dag.py
│   ├── datawarehouse_connections.py    # Connection setup script
│   └── requirements.txt
├── dbt/
│   ├── dbt_project.yml                 # Unified dbt project
│   ├── profiles.yml.template
│   ├── models/
│   │   ├── extract_package/            # Models from Extract package
│   │   │   └── staging/
│   │   ├── transform_package/          # Models from Transform package
│   │   │   ├── staging/
│   │   │   ├── intermediate/
│   │   │   └── marts/
│   │   ├── load_package/               # Models from Load package
│   │   │   └── marts/
│   │   ├── shared/                     # Cross-package dependencies
│   │   └── schema.yml
│   ├── docs/
│   │   └── migration_report.md
│   └── README.md
└── DataWarehouse_migration_report.json
```

## Example 2: Transform-Only Project (dbt-only output)

### Source Structure
```
Analytics.ispac
├── Customer_Analytics.dtsx    # Customer data transformations
├── Sales_Analytics.dtsx       # Sales metrics and KPIs
└── Financial_Reports.dtsx     # Financial reporting models
```

### Migration Result
- **Strategy**: `dbt_only` 
- **Rationale**: All packages contain only data transformations
- **Output**: Unified dbt project with models organized by source package

## Example 3: Complex Orchestration Project

### Source Structure  
```
Integration.ispac
├── FTP_Ingestion.dtsx         # File transfer and validation
├── API_Extraction.dtsx        # REST API data extraction
├── Data_Quality.dtsx          # Data validation and cleansing
├── Transform_Facts.dtsx       # Fact table transformations
├── Transform_Dimensions.dtsx  # Dimension table transformations
├── Load_Warehouse.dtsx        # Final warehouse loading
└── Orchestrator.dtsx          # Master orchestration with complex precedence
```

### Migration Result
- **Strategy**: `mixed`
- **Components**: `['airflow', 'dbt']`
- **Master DAG**: Coordinates 7 package DAGs with complex dependencies
- **dbt Integration**: Transformation packages integrated as dbt models within Airflow

## Running the Examples

### Prerequisites
```bash
# Install the enhanced migration tool
pip install -e .

# Verify installation
ssis-project-migrate --help
```

### Example Commands

#### Analyze Before Migration
```bash
ssis-project-migrate analyze examples/DataWarehouse.ispac --verbose
```

#### Full Migration with AI Enhancement
```bash
export ANTHROPIC_API_KEY="your_claude_api_key"
ssis-project-migrate --project examples/DataWarehouse.ispac \
                     --out ./output \
                     --mode auto \
                     --save-ir \
                     --verbose
```

#### Encrypted Project
```bash
ssis-project-migrate --project secure_project.ispac \
                     --out ./output \
                     --password "project_password" \
                     --mode mixed
```

#### Force Specific Mode
```bash
# Force Airflow-only (skip dbt generation)
ssis-project-migrate --project project.ispac --out ./output --mode airflow

# Force dbt-only (transformation packages only)
ssis-project-migrate --project project.ispac --out ./output --mode dbt
```

## Understanding Migration Strategies

The tool automatically analyzes your project and recommends the best migration strategy:

### Strategy: `dbt_only`
- **When**: All packages contain only data transformations
- **Output**: Single unified dbt project
- **Use Case**: Analytics and reporting pipelines

### Strategy: `airflow_only` 
- **When**: Packages contain orchestration, file operations, or API calls
- **Output**: Individual Airflow DAGs with master orchestrator
- **Use Case**: ETL pipelines with external system integration

### Strategy: `mixed`
- **When**: Mix of transformation and orchestration packages
- **Output**: Airflow DAGs + integrated dbt project
- **Use Case**: Complete data platforms

### Strategy: `dbt_with_orchestration`
- **When**: All transformations but with package dependencies
- **Output**: dbt project + Airflow orchestrator
- **Use Case**: Complex transformation pipelines with execution order requirements

## Advanced Features

### Cross-Package Dependencies
The tool automatically detects `ExecutePackageTask` dependencies and creates:
- Master DAGs with proper task dependencies
- External task sensors for coordination
- Parameter passing between package DAGs

### Project-Level Resources
Project parameters and connections are:
- Extracted to Airflow Variables and Connections
- Made available to all package DAGs
- Documented in setup scripts

### Migration Reports
Every migration generates:
- **JSON Report**: Machine-readable migration details
- **Markdown Summary**: Human-readable migration overview  
- **dbt Documentation**: Model lineage and descriptions
- **TODO Comments**: Manual review items in generated code

## Deployment Guide

### Airflow Deployment
1. **Upload DAGs**: Copy generated `.py` files to your Airflow `dags/` folder
2. **Setup Connections**: Run the generated `*_connections.py` script
3. **Install Requirements**: `pip install -r requirements.txt`
4. **Test DAGs**: `airflow dags test master_dag 2024-01-01`

### dbt Deployment
1. **Setup Profile**: Copy `profiles.yml.template` to `~/.dbt/profiles.yml`
2. **Configure Credentials**: Update with your Snowflake/warehouse details
3. **Test Connection**: `dbt debug`
4. **Run Models**: `dbt run --target dev`
5. **Run Tests**: `dbt test`

### Production Considerations
- **Resource Sizing**: Review Airflow worker and dbt thread configurations
- **Monitoring**: Set up alerts for DAG failures and dbt test failures  
- **Performance**: Optimize SQL queries and add appropriate indexes
- **Security**: Ensure connections use proper authentication and encryption

## Troubleshooting

### Common Issues

#### Package Parsing Errors
```
Error: Failed to parse package MyPackage.dtsx
```
- **Solution**: Check for unsupported SSIS components or encryption issues
- **Workaround**: Use `--save-ir` flag to debug parsing issues

#### Dependency Analysis Issues
```
Warning: Circular dependency detected between Package1 and Package2
```
- **Solution**: Review SSIS ExecutePackageTask configurations
- **Impact**: May require manual DAG adjustment

#### dbt Model Generation
```
Error: Failed to convert data flow to dbt models
```  
- **Solution**: Check for unsupported SSIS transformations
- **Workaround**: Generated TODO comments indicate manual review needed

### Getting Help
- Review generated `MIGRATION_SUMMARY.md` for specific guidance
- Check migration logs: `tail -f ssis_project_migration.log`
- Use `--verbose` flag for detailed debugging output
- Consult TODO comments in generated code for manual review items