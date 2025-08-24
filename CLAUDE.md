# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Installation and Setup
```bash
# Install in development mode
pip install -e .

# Install with all dependencies
pip install -r requirements.txt
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

# Generate sample DTSX files for testing
python tests/samples/sample_dtsx.py
```

### CLI Usage
```bash
# Basic migration
ssis-migrate --in MyPackage.dtsx --out ./output

# Analysis only
ssis-migrate analyze MyPackage.dtsx --verbose

# With AI enhancement
ssis-migrate --in package.dtsx --anthropic-key your_key --out ./output

# Specific migration mode
ssis-migrate --in package.dtsx --mode airflow --out ./output
ssis-migrate --in package.dtsx --mode dbt --out ./output
```

### Development Testing
```bash
# Run demo
python demo.py

# Test with sample files
python tests/samples/sample_dtsx.py
ssis-migrate analyze tests/samples/generated_sample.dtsx
```

## Architecture Overview

This is a 3-stage migration pipeline: **DTSX Parser → Intermediate Representation (IR) → Code Generators**

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

## File Organization Patterns

### Generated Output Structure
**Airflow**: `package_name_dag.py`, connection scripts, requirements.txt  
**dbt**: Complete project with `models/`, `schema.yml`, `dbt_project.yml`

### Prompt Templates  
Located in `/generators/prompts/` with exact specifications from migration plan section 6

### Migration Reports
Generated in multiple formats (JSON, HTML, CSV) with detailed analysis, risk assessment, and actionable recommendations