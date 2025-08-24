# SSIS to Airflow/dbt Migration Tool

A comprehensive solution for migrating SQL Server Integration Services (SSIS) packages to modern data engineering platforms using Apache Airflow and dbt (data build tool).

## 🚀 Quick Start

```bash
# Install the tool
pip install -e .

# Migrate an SSIS package
ssis-migrate --in MyPackage.dtsx --out ./output

# Analyze package first
ssis-migrate analyze MyPackage.dtsx
```

## 📋 Features

- **Complete DTSX Parsing**: Reads and parses SSIS .dtsx XML files with full component support
- **Intelligent Migration**: Automatically determines the best migration approach (Airflow, dbt, or both)
- **SQL Conversion**: Converts T-SQL to Snowflake-compatible SQL with detailed mapping
- **AI-Assisted Generation**: Optional Claude AI integration for enhanced code generation
- **Security Handling**: Properly manages encrypted packages and sensitive data
- **Comprehensive Reports**: Detailed analysis, risk assessment, and migration recommendations

## 🏗️ Architecture

The tool follows a three-stage architecture designed for reliability and extensibility:

```
SSIS (.dtsx XML) → Parser → IR (JSON) → Generators → Airflow DAGs + dbt Projects
```

1. **Parser**: Converts DTSX XML to Intermediate Representation (IR)
2. **Generators**: Transform IR to target platforms
3. **Reporting**: Provides comprehensive migration analysis

## 📦 Installation

### Requirements
- Python 3.8+
- Optional: Claude API key for AI-assisted generation

### Install
```bash
git clone <repository-url>
cd SSISMig
pip install -e .
```

## 🎯 Migration Modes

### Auto Mode (Recommended)
Intelligently determines the best approach:
- **dbt only**: For transformation-only packages
- **Airflow only**: For ingestion-focused packages  
- **Mixed**: Airflow orchestration + dbt transformations

### Specific Modes
```bash
ssis-migrate --in package.dtsx --mode airflow --out ./airflow
ssis-migrate --in package.dtsx --mode dbt --out ./dbt
```

## 🔄 Component Mappings

| SSIS Component | Airflow | dbt |
|----------------|---------|-----|
| Execute SQL Task | `SnowflakeOperator` | SQL Model |
| Data Flow Task | `TaskGroup` + Operators | Multiple Models |
| Script Task | `PythonOperator` | Custom Macro |
| Sequence Container | `TaskGroup` | Model Dependencies |
| ForEach Loop | Dynamic Task Mapping | Incremental Models |
| OLE DB Source | Stage + COPY INTO | `source()` Reference |
| Derived Column | Column Transformation | SELECT Expression |
| Lookup | JOIN Operation | `ref()` JOIN |

## 📁 Output Structure

### Airflow Output
```
output/airflow/
├── package_name_dag.py       # Main DAG
├── package_name_connections.py  # Connection setup
├── requirements.txt          # Dependencies
└── README.md                # Instructions
```

### dbt Output
```
output/dbt/
├── dbt_project.yml          # Project config
├── models/
│   ├── staging/            # Source models
│   ├── intermediate/       # Transforms
│   └── marts/             # Final models
├── models/schema.yml       # Tests & docs
└── profiles.yml.template  # Connection setup
```

## 🛡️ Security Features

- Handles encrypted SSIS packages with proper password support
- Redacts sensitive connection information
- Generates secure connection setup scripts
- Provides security compliance warnings and recommendations

## 🔍 SQL Dialect Conversion

Automatically converts T-SQL to Snowflake:
- `GETDATE()` → `CURRENT_TIMESTAMP()`
- `ISNULL(a,b)` → `COALESCE(a,b)` 
- `TOP n` → `LIMIT n`
- `[identifier]` → `"identifier"`
- And many more...

## 📊 Migration Reports

Comprehensive analysis including:
- Package complexity assessment
- Component mapping confidence levels
- Risk assessment and mitigation strategies
- Manual review items with priorities
- Next steps and recommendations
- Export to HTML, CSV, or JSON formats

## 🧪 Testing

```bash
# Run test suite
pytest tests/

# Test specific components
pytest tests/test_parser.py
pytest tests/test_generators.py

# Generate sample files
python tests/samples/sample_dtsx.py
```

## 📖 Usage Examples

### Basic Migration
```bash
ssis-migrate --in MyETLPackage.dtsx --out ./migration_output
```

### With AI Enhancement
```bash
export ANTHROPIC_API_KEY="your_key_here"
ssis-migrate --in ComplexPackage.dtsx --anthropic-key $ANTHROPIC_API_KEY --out ./output
```

### Encrypted Package
```bash
ssis-migrate --in EncryptedPackage.dtsx --password mypassword --out ./output
```

### Analysis Only
```bash
ssis-migrate analyze MyPackage.dtsx --verbose
```

## ⚙️ Configuration

### Environment Variables
```bash
export ANTHROPIC_API_KEY="your_claude_api_key"
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
```

### Airflow Setup
```python
# Run generated connection script
python output/airflow/package_connections.py
```

### dbt Setup
```bash
cd output/dbt
cp profiles.yml.template ~/.dbt/profiles.yml
# Edit with your Snowflake credentials
dbt compile && dbt test
```

## 🚦 Migration Process

1. **Analysis Phase**
   ```bash
   ssis-migrate analyze package.dtsx
   ```

2. **Migration Phase**
   ```bash
   ssis-migrate --in package.dtsx --out ./output --save-ir
   ```

3. **Validation Phase**
   ```bash
   # Test Airflow DAG
   airflow dags test package_dag 2024-01-01
   
   # Test dbt models
   cd output/dbt && dbt compile && dbt test
   ```

4. **Deployment Phase**
   - Deploy Airflow DAG to your environment
   - Set up dbt in production
   - Configure monitoring and alerting

## 🎨 AI-Assisted Features

When Claude API key is provided:
- Enhanced DAG generation with better error handling
- Optimized dbt model structures
- Intelligent handling of complex transformations
- Advanced SQL optimization suggestions

## 🔧 Development

### Project Structure
```
SSISMig/
├── parser/           # DTSX parsing components
├── generators/       # Airflow & dbt generators
├── models/           # IR data models
├── adapters/         # Database adapters
├── cli/              # Command line interface
├── tests/            # Test suite
└── docs/             # Documentation
```

### Contributing
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## 📋 Limitations & Known Issues

### Current Limitations
- Script tasks require manual conversion
- Some advanced SSIS features not fully supported
- Limited support for custom SSIS components
- Certain encryption types have restrictions

### Workarounds
- Check migration reports for manual review items
- Use generated TODO comments as guidance
- Consult component mapping documentation
- Test thoroughly in development environments

## 📚 Documentation

- [User Guide](USER_GUIDE.md) - Comprehensive usage guide
- [Migration Plan](ssis_to_airflow_dbt_migration_plan.md) - Technical architecture
- [API Documentation](docs/api/) - Developer reference

## 🆘 Troubleshooting

### Common Issues
1. **XML Parsing Errors**: Check DTSX file integrity and encoding
2. **Encrypted Packages**: Ensure correct password and supported encryption type
3. **Complex Components**: Review manual review items in migration report
4. **SQL Conversion**: Check conversion notes in generated code

### Debug Mode
```bash
ssis-migrate --in package.dtsx --verbose --out ./output
tail -f ssis_migration.log
```

## 📄 License

Apache 2.0 License - See [LICENSE](LICENSE) for details.

## 🤝 Support

- Check migration reports for specific guidance
- Review generated TODO comments  
- Consult Airflow and dbt documentation
- Submit issues for bugs or feature requests

---

**Ready to modernize your SSIS workflows?** Start with a simple package analysis and work your way up to full migration!

```bash
ssis-migrate analyze your_package.dtsx
```