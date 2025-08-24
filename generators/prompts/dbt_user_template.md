# dbt Project Generation Request

## Objective
Generate a complete dbt project structure from the provided SSIS Intermediate Representation (IR) JSON, filtered to transformation tasks only.

## Input Data
```json
{IR_JSON_TRANSFORM_ONLY}
```

## Project Requirements

### Project Structure
Generate the following files and directories:
- `dbt_project.yml` - Project configuration
- `models/` - SQL model files organized by layer
- `models/staging/` - Staging models (sources)
- `models/intermediate/` - Intermediate transformation models
- `models/marts/` - Final mart models
- `models/schema.yml` - Source and model documentation with tests
- `macros/` - Reusable Jinja macros (if needed)
- `dbt_packages.yml` - Package dependencies (if needed)

### Data Flow Component Mappings
- **OLE DB/ADO.NET Source** → `source()` definition with SELECT statement
- **Derived Column** → Add computed columns in model SELECT
- **Lookup** → `JOIN` to reference table using `ref()` or `source()`
- **Conditional Split** → Split into multiple models with WHERE clauses or CASE statements
- **Union All** → `UNION ALL` in model SQL
- **Aggregate** → GROUP BY with aggregate functions
- **Sort** → ORDER BY clause (mainly for incremental models)
- **Destination** → Model materialization (table, view, or incremental)

### Model Organization
- **Staging Models**: Direct mappings from SSIS sources, basic cleaning
- **Intermediate Models**: Complex transformations, joins, business logic
- **Mart Models**: Final consumption-ready models

### Materialization Strategy
- **Sources/Staging**: Usually `view` for lightweight transformations
- **Heavy Transformations**: Use `table` materialization
- **Large Datasets**: Use `incremental` with appropriate unique_key and merge strategy
- **Lookup Tables**: Use `table` for performance

### Testing Strategy
Include comprehensive tests:
- `unique` tests for primary keys
- `not_null` tests for required fields
- `relationships` tests for foreign keys
- `accepted_values` tests for categorical fields
- Custom data quality tests where appropriate

### Variable Mapping
- SSIS variables `@[User::var]` → `{{ var('var_name') }}`
- SSIS parameters `@[$Package::param]` → `{{ var('param_name') }}`
- Environment-specific values → Use dbt variables with defaults

## Output Files

### 1. dbt_project.yml
```yaml
name: 'ssis_migration'
version: '1.0.0'
config-version: 2

# Model paths and configurations
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"] 
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

# Model configurations
models:
  ssis_migration:
    staging:
      +materialized: view
    intermediate:
      +materialized: table
    marts:
      +materialized: table
```

### 2. Model Files (*.sql)
Generate SQL files for each transformation with:
- Proper Jinja templating
- Source references using `source()`
- Model references using `ref()`
- Business logic preservation from SSIS
- Optimized Snowflake SQL

### 3. schema.yml
Complete schema file with:
- Source definitions with freshness checks
- Model documentation
- Column descriptions
- Comprehensive test definitions

### 4. Macros (if needed)
Generate reusable macros for:
- Complex transformation logic
- Data quality checks
- Common business rules

## Special Considerations

### Incremental Models
For models that should be incremental:
```sql
{{ config(materialized='incremental', unique_key='key_column') }}

select * from {{ source('schema', 'table') }}

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

### SSIS Expression Conversion
Convert SSIS expressions to equivalent dbt/SQL:
- Date functions: `GETDATE()` → `CURRENT_TIMESTAMP()`
- String functions: `LEN()` → `LENGTH()`, `ISNULL()` → `COALESCE()`
- Conditional logic: Use CASE statements

### Documentation Requirements
Each model should include:
- Description of purpose and business logic
- Mapping from original SSIS component(s)
- Column descriptions and business definitions
- Data lineage and transformation notes

### Migration Notes
Include comments in each model indicating:
```sql
-- Migrated from SSIS: [Package Name] -> [Data Flow Task] -> [Component Name]
-- Original transformation: [Brief description]
-- Migration notes: [Any special considerations]
```

## Quality Assurance
- Ensure all models compile successfully
- Verify proper dependency resolution
- Include appropriate data validation
- Generate models that are production-ready
- Follow dbt and SQL best practices

The generated dbt project should be deployable with minimal configuration, requiring only `profiles.yml` setup for the target Snowflake environment.