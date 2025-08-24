# SSIS to Airflow/dbt Migration Tool - User Guide

## Overview

The SSIS to Airflow/dbt Migration Tool is a comprehensive solution for converting SQL Server Integration Services (SSIS) packages into modern data engineering workflows using Apache Airflow and dbt (data build tool).

## Key Features

- **Complete DTSX Parsing**: Reads and parses SSIS .dtsx XML files
- **Intelligent Mode Detection**: Automatically determines whether to generate Airflow DAGs, dbt projects, or both
- **SQL Dialect Conversion**: Converts T-SQL to Snowflake-compatible SQL
- **Security Handling**: Properly handles encrypted packages and sensitive data
- **AI-Assisted Generation**: Optional Claude AI integration for enhanced code generation
- **Comprehensive Reporting**: Detailed migration analysis and recommendations

## Architecture

The tool follows a three-stage architecture:

1. **Parser**: Converts DTSX XML to Intermediate Representation (IR) JSON
2. **Generators**: Convert IR to Airflow DAGs or dbt projects  
3. **CLI**: Orchestrates the migration process

```
SSIS (.dtsx XML)
        │
        ▼
[Deterministic Parser]
        │
        ▼
Intermediate Representation (IR, JSON Graph)
        │
        ├───────────────► [Generator: Airflow]
        │                    - DAGs with TaskGroups
        │                    - Snowflake operators
        │                    - Dynamic task mapping
        │
        └───────────────► [Generator: dbt]
                             - SQL models  
                             - Sources and tests
                             - Incremental materializations
```

## Installation

### Requirements
- Python 3.8+
- Git (for development)

### Install from Source

```bash
git clone <repository-url>
cd ssis-migrator
pip install -e .
```

### Dependencies
The tool automatically installs required dependencies:
- `lxml` for XML parsing
- `pydantic` for data validation
- `click` for CLI interface
- `jinja2` for templating
- `anthropic` (optional) for AI code generation

## Quick Start

### Basic Usage

```bash
# Migrate a simple SSIS package  
ssis-migrate --in my_package.dtsx --out ./output

# Specify migration mode
ssis-migrate --in my_package.dtsx --mode airflow --out ./output

# Use with encrypted packages
ssis-migrate --in encrypted_package.dtsx --password mypassword --out ./output

# Enable AI-assisted generation
ssis-migrate --in my_package.dtsx --anthropic-key your_api_key --out ./output
```

### Analyze Package First

```bash
# Get package information without migration
ssis-migrate analyze my_package.dtsx
```

### Create Example Files

```bash
# Generate sample DTSX files for testing
ssis-migrate create-example --output ./examples
```

## Migration Modes

### Auto Mode (Default)
The tool automatically determines the best migration approach:
- **dbt only**: For transformation-only packages
- **Airflow only**: For ingestion-heavy packages
- **Mixed**: Airflow for orchestration + dbt for transformations

### Airflow Mode
Generates Apache Airflow DAGs with:
- `SnowflakeOperator` for SQL execution
- `TaskGroup` for SSIS containers
- `PythonOperator` for script tasks
- Dynamic task mapping for loops
- Proper precedence constraint handling

### dbt Mode  
Generates dbt projects with:
- Staging models from sources
- Intermediate transformation models
- Mart models for destinations
- Proper source definitions and tests
- Incremental materializations where appropriate

## SSIS Component Mappings

### Control Flow Tasks

| SSIS Task | Airflow Equivalent | dbt Equivalent |
|-----------|-------------------|----------------|
| Execute SQL Task | `SnowflakeOperator` | SQL model |
| Data Flow Task | `TaskGroup` + operators | Multiple models |
| Script Task | `PythonOperator` | Custom macro |
| Sequence Container | `TaskGroup` | Model dependencies |
| ForEach Loop | Dynamic task mapping | Incremental models |
| Bulk Insert Task | `SnowflakeOperator` (COPY) | `source()` definition |

### Data Flow Components

| SSIS Component | Airflow Approach | dbt Approach |
|----------------|------------------|--------------|
| OLE DB Source | Stage + COPY INTO | `source()` reference |
| Flat File Source | File upload + COPY | External table |
| Derived Column | Column transformation | SELECT expressions |
| Lookup | JOIN operation | `ref()` JOIN |
| Conditional Split | Branching logic | Multiple models + WHERE |
| Union All | Multiple inputs | UNION ALL SQL |
| Aggregate | Grouped operations | GROUP BY model |
| Destinations | Final COPY/INSERT | Model materialization |

## Configuration

### Environment Variables

```bash
# Optional: Claude AI API key
export ANTHROPIC_API_KEY="your_api_key_here"

# Snowflake connection (for testing)
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user" 
export SNOWFLAKE_PASSWORD="your_password"
```

### Airflow Connections

After generating Airflow DAGs, set up connections:

```python
# Run the generated connection setup script
python output/airflow/my_package_connections.py
```

### dbt Profiles

Configure `~/.dbt/profiles.yml` for dbt:

```yaml
ssis_migration:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE') }}"
```

## Advanced Features

### AI-Assisted Generation

When you provide a Claude API key, the tool uses AI to:
- Generate more sophisticated Airflow DAGs
- Create optimized dbt models
- Handle complex transformations better
- Provide better error handling

### SQL Dialect Conversion

The tool automatically converts T-SQL to Snowflake:
- `GETDATE()` → `CURRENT_TIMESTAMP()`
- `ISNULL(a,b)` → `COALESCE(a,b)`
- `TOP n` → `LIMIT n`
- `LEN()` → `LENGTH()`
- Square brackets → Double quotes for identifiers

### Security Handling

- Detects and handles encrypted SSIS packages
- Redacts sensitive connection information
- Generates secure connection setup scripts
- Provides security compliance warnings

### Migration Reports

Comprehensive reports include:
- Package complexity analysis
- Component mapping details
- Risk assessment
- Manual review items
- Next steps and recommendations

## Output Structure

### Airflow Output
```
output/airflow/
├── my_package_dag.py          # Main DAG file
├── my_package_connections.py  # Connection setup
├── requirements.txt           # Python dependencies
└── README.md                  # Deployment instructions
```

### dbt Output
```
output/dbt/
├── dbt_project.yml           # dbt project config
├── models/
│   ├── staging/             # Source models
│   ├── intermediate/        # Transform models
│   ├── marts/              # Final models
│   └── schema.yml          # Tests and docs
├── macros/                 # Custom macros
└── profiles.yml.template   # Connection template
```

## Testing and Validation

### Test Generated Code

```bash
# Test Airflow DAG
airflow dags test my_package_dag 2024-01-01

# Test dbt models  
cd output/dbt
dbt compile
dbt test
```

### Validate Migration

```bash
# Run with validation only
ssis-migrate --in my_package.dtsx --validate-only

# Save IR for inspection
ssis-migrate --in my_package.dtsx --save-ir --out ./output
```

## Troubleshooting

### Common Issues

1. **XML Parsing Errors**
   - Ensure DTSX file is not corrupted
   - Check for proper XML encoding
   - Verify file permissions

2. **Encrypted Packages**
   - Provide package password with `--password`
   - Some encryption types are not supported
   - Check protection level in migration report

3. **Complex Components**
   - Script tasks require manual conversion
   - Check manual review items in report
   - Review generated TODO comments

### Debug Mode

```bash
# Enable verbose logging
ssis-migrate --in my_package.dtsx --verbose --out ./output

# Check migration log
tail -f ssis_migration.log
```

## Best Practices

### Before Migration
1. Inventory all SSIS packages
2. Document package dependencies
3. Test with sample packages first
4. Plan target architecture

### During Migration  
1. Review migration reports carefully
2. Address manual review items
3. Test generated code thoroughly
4. Validate data transformations

### After Migration
1. Set up monitoring and alerting
2. Create operational runbooks
3. Train team on new tools
4. Plan decommissioning of SSIS

## Support and Contribution

### Getting Help
- Check migration reports for specific guidance
- Review generated TODO comments
- Consult Airflow and dbt documentation
- Examine sample DTSX files for patterns

### Contributing
- Fork the repository
- Add tests for new features
- Follow existing code style
- Submit pull requests with documentation

## Limitations

### Current Limitations
- Script tasks require manual conversion
- Some advanced SSIS features not supported
- Limited support for custom components
- Encrypted packages have restrictions

### Future Enhancements
- Additional database support
- More SSIS component mappings
- Advanced optimization features
- GUI interface for easier use

## Examples

See the `tests/samples/` directory for example DTSX files and their generated outputs.