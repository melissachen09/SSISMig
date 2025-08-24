# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Installation and Setup
```bash
# Install in development mode (creates console_scripts entry point)
pip install -e .

# Install with all dependencies
pip install -r requirements.txt

# Verify installation
ssis-migrate --help
```

### Testing
```bash
# Run full test suite
pytest tests/

# Run specific test modules
pytest tests/test_parser.py
pytest tests/test_generators.py
pytest tests/test_sql_converter.py
pytest tests/test_models.py

# Run with verbose output and logging
pytest tests/ -v -s

# Run single test function
pytest tests/test_parser.py::test_parse_simple_dtsx -v

# Generate sample DTSX files for testing
python tests/samples/sample_dtsx.py
```

### CLI Usage
```bash
# Basic migration with auto-detection
ssis-migrate --in MyPackage.dtsx --out ./output

# Analysis only (no code generation)
ssis-migrate analyze MyPackage.dtsx --verbose

# With AI enhancement (requires ANTHROPIC_API_KEY env var or --anthropic-key)
ssis-migrate --in package.dtsx --anthropic-key your_key --out ./output

# Specific migration modes
ssis-migrate --in package.dtsx --mode airflow --out ./output
ssis-migrate --in package.dtsx --mode dbt --out ./output

# Save intermediate representation for debugging
ssis-migrate --in package.dtsx --out ./output --save-ir

# Debug with verbose logging
ssis-migrate --in package.dtsx --out ./output --verbose
```

### Development Testing
```bash
# Run demo with included sample data
python demo.py

# Test with sample files
python tests/samples/sample_dtsx.py
ssis-migrate analyze tests/samples/generated_sample.dtsx

# Test with included Q1.dtsx sample
ssis-migrate analyze data/Q1.dtsx --verbose
ssis-migrate --in data/Q1.dtsx --mode auto --out ./test_output

# Project-level migration (new functionality)
ssis-project-migrate analyze examples/sample_project.ispac --verbose
ssis-project-migrate --project examples/sample_project.ispac --out ./project_output --mode auto
```

### Project-Level Migration
The tool now supports complete SSIS project migration with cross-package dependencies:

```bash
# Analyze entire project structure
ssis-project-migrate analyze path/to/project.ispac --verbose

# Migrate complete project with auto strategy detection
ssis-project-migrate --project path/to/project.ispac --out ./build --mode auto

# Force specific migration mode
ssis-project-migrate --project project.ispac --out ./build --mode mixed
ssis-project-migrate --project project.ispac --out ./build --mode airflow  # Airflow only
ssis-project-migrate --project project.ispac --out ./build --mode dbt      # dbt only

# With AI enhancement and debugging
ssis-project-migrate --project project.ispac --out ./build --anthropic-key $ANTHROPIC_API_KEY --save-ir --verbose
```

### Debugging and Troubleshooting
```bash
# Check migration logs (automatically created in project root)
tail -f ssis_migration.log

# Run with maximum verbosity for debugging
ssis-migrate --in package.dtsx --out ./output --verbose

# Debug parsing issues - save intermediate representation
ssis-migrate --in package.dtsx --out ./output --save-ir
# This creates an IR JSON file for manual inspection

# Test parsing only (no code generation)
ssis-migrate analyze package.dtsx --verbose

# Environment variables for debugging
export ANTHROPIC_API_KEY="your_key"  # Enable AI features
export LOG_LEVEL=DEBUG               # More detailed logging
```

### Package Entry Points
The tool provides two console commands via setuptools entry_points in setup.py:
- **Single Package CLI**: `ssis-migrate` → `cli.main:main` 
  - Commands: `ssis-migrate analyze`, `ssis-migrate --in ... --out ...`
- **Project Migration CLI**: `ssis-project-migrate` → `ssis_project_migrator:cli`
  - Commands: `ssis-project-migrate analyze`, `ssis-project-migrate --project ... --out ...`

### Key Dependencies
- **lxml**: XML parsing for DTSX files with Microsoft namespaces
- **pydantic**: IR schema validation and data modeling
- **click**: Command-line interface framework
- **jinja2**: Template engine for code generation (fallback when AI unavailable)
- **sqlparse**: SQL parsing and formatting
- **anthropic**: Claude AI integration for enhanced code generation
- **python-dotenv**: Environment variable management

## Architecture Overview

This is a 3-stage migration pipeline that now supports both individual packages and complete projects:

**Single Package**: DTSX Parser → Intermediate Representation (IR) → Code Generators  
**Project Migration**: Project Parser → Project IR → Master DAG Generator + Unified dbt Generator

### Project Migration Architecture

The enhanced tool adds project-level capabilities:

1. **Project Parser** (`/parser/project_parser.py`): Parses .ispac files and project directories
2. **Project-to-IR Converter** (`/parser/project_to_ir.py`): Orchestrates conversion of entire projects  
3. **Master DAG Generator** (`/generators/master_dag_gen.py`): Creates orchestration DAGs for package dependencies
4. **Project dbt Generator** (`/generators/project_dbt_gen.py`): Creates unified dbt projects from multiple packages
5. **Project CLI** (`ssis_project_migrator.py`): Command-line interface for project migration

### Migration Strategies

The tool automatically recommends migration strategies based on project analysis:

- **dbt-only**: All packages are transformation-only with no dependencies
- **dbt_with_orchestration**: All transformation packages but with execution dependencies  
- **airflow-only**: Packages contain orchestration, ingestion, or external system interaction
- **mixed**: Combination of transformation and orchestration packages

### Cross-Package Dependencies

The system handles ExecutePackageTask dependencies by:
- Parsing package execution chains from DTSX files
- Creating master DAGs with TriggerDagRunOperator and ExternalTaskSensor
- Ensuring proper execution order through Airflow task dependencies
- Supporting conditional execution based on precedence constraints

### Core Data Flow
1. **DTSX Parser** (`/parser/`): Converts SSIS XML packages to standardized IR JSON
2. **IR Models** (`/models/ir.py`): Pydantic-based data models defining the intermediate representation 
3. **Code Generators** (`/generators/`): Transform IR into Airflow DAGs and dbt projects

### Key Components

#### Parser Pipeline (`/parser/`)
- **to_ir.py**: Main orchestrator that coordinates all parsing steps
- **dtsx_reader.py**: XML parsing with Microsoft DTSX namespaces
- **dataflow_parser.py**: Handles SSIS Data Flow tasks and component mapping
- **precedence_parser.py**: Parses control flow dependencies and precedence constraints  
- **expressions.py**: SSIS expression language parser and converter
- **secure.py**: Encryption and security handler for protected packages

#### IR Schema (`/models/ir.py`)
The Intermediate Representation follows the exact schema from section 2 of the migration plan:
- **IRPackage**: Root model containing all package metadata
- **Executable**: SSIS tasks (ExecuteSQL, DataFlow, ScriptTask, etc.)
- **DataFlowComponent**: Pipeline components (Sources, Destinations, Transformations)
- **PrecedenceEdge**: Control flow dependencies with conditions
- **ConnectionManager**: Database and file connections with security handling

#### Code Generation (`/generators/`)
- **airflow_gen.py**: Generates production-ready Airflow DAGs with TaskGroups and proper operator mappings
- **dbt_gen.py**: Creates complete dbt projects with staging/intermediate/marts structure
- **sql_converter.py**: T-SQL to Snowflake dialect conversion with detailed mapping notes

### Migration Decision Logic
The tool automatically determines output format based on package analysis:
- **dbt-only**: Packages containing only data transformations
- **Airflow-only**: Packages with ingestion, orchestration, or mixed operations
- **Mixed mode**: Airflow orchestration calling dbt transformations via Cosmos or operators

### AI Integration
When Claude API key is provided:
- Uses `/generators/prompts/` templates for enhanced code generation
- Fallback to Jinja2 templates when AI unavailable
- System prompts in `airflow_system.txt` and `dbt_system.txt`
- User prompts from `airflow_user_template.md` and `dbt_user_template.md`

### Security and Encryption
- Handles SSIS `ProtectionLevel` encryption with optional password support
- Redacts sensitive connection properties and generates placeholders
- Creates secure connection setup scripts for target platforms

### Component Mapping Confidence
The migration system tracks mapping confidence levels:
- **High**: Direct 1:1 mappings (ExecuteSQL → SnowflakeOperator)
- **Medium**: Structural changes required (ForEach → Dynamic Task Mapping)
- **Low**: Manual intervention needed (Script Tasks, Custom Components)

## Important Implementation Notes

### DTSX XML Structure
SSIS packages use Microsoft-specific XML namespaces:
- **DTS**: Core package structure and executables
- **SQLTask**: Execute SQL task payloads  
- **Pipeline**: Data Flow task components and paths

### SQL Dialect Conversion
The T-SQL to Snowflake converter handles common patterns:
- Function mappings (`GETDATE()` → `CURRENT_TIMESTAMP()`)
- Syntax changes (`ISNULL()` → `COALESCE()`)  
- Identifier quoting (`[column]` → `"column"`)
- Data type conversions with compatibility notes

### Error Handling and Validation
- IR schema validation using Pydantic models
- Component mapping validation with confidence scoring
- Migration report generation identifying manual review items
- Comprehensive logging throughout the pipeline

### Testing Strategy
Tests use fixtures with realistic DTSX XML samples in `/tests/conftest.py`:
- `sample_dtsx_simple`: Basic package with ExecuteSQL task
- `sample_dtsx_with_dataflow`: Complex package with data flow components
- Integration tests validate end-to-end migration workflows

### Test Output Validation
Generated output directories (out_airflow/, out_dbt/, out_auto/) contain examples of:
- Airflow DAGs with proper task dependencies
- dbt projects with complete directory structure
- Migration reports in JSON format
- Connection setup scripts and templates

## File Organization Patterns

### Generated Output Structure
**Airflow**: `package_name_dag.py`, connection scripts, requirements.txt  
**dbt**: Complete project with `models/`, `schema.yml`, `dbt_project.yml`

### Prompt Templates  
Located in `/generators/prompts/` with exact specifications from migration plan section 6

### Migration Reports
Generated in multiple formats (JSON, HTML, CSV) with detailed analysis, risk assessment, and actionable recommendations