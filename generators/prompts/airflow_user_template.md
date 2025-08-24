# Airflow DAG Generation Request

## Objective
Generate a complete Airflow DAG from the provided SSIS Intermediate Representation (IR) JSON.

## Input Data
```json
{IR_JSON_HERE}
```

## Mapping Instructions

### Task Type Mappings
- **ExecuteSQL** → `SnowflakeOperator` with SQL execution
- **ScriptTask** → `PythonOperator` with inline script or function call
- **DataFlow** (file ingestion) → `PythonOperator` (stage upload) + `SnowflakeOperator` (COPY INTO)
- **DataFlow** (transformation) → Call to dbt via `BashOperator` or dbt operators
- **SequenceContainer** → `TaskGroup` with nested tasks
- **ForEachLoop** → Dynamic task mapping using `@task` decorator and `expand()`
- **ForLoop** → Dynamic task mapping with range iteration

### Precedence Constraint Mappings
- **Success** → Standard dependency with `>>` operator
- **Failure** → Dependency with `trigger_rule=TriggerRule.ONE_FAILED`
- **Completion** → Dependency with `trigger_rule=TriggerRule.ALL_DONE` 
- **Expression** → `BranchPythonOperator` or `ShortCircuitOperator` with condition logic

### Expression Mappings
- SSIS variables `@[User::var]` → `{{ dag_run.conf.get('var', default) }}`
- SSIS parameters `@[$Package::param]` → `{{ dag_run.conf.get('param', default) }}`
- System variables → `{{ var.value.variable_name }}`

## Output Requirements

Generate a single Python file containing:

1. **Complete DAG Definition**
   - Proper imports for all required operators
   - DAG configuration with parameters
   - All task definitions with proper dependencies

2. **Task Implementation**
   - Map each IR executable to appropriate Airflow operator
   - Include SQL dialect conversion hints where needed
   - Preserve precedence constraint logic exactly

3. **Configuration**
   - Use `SNOWFLAKE_CONN_ID = "snowflake_default"` constant
   - Parameterize database/schema/warehouse via `dag_run.conf`
   - Include proper retry logic and timeouts

4. **Documentation**
   - Docstring for the DAG explaining SSIS source package
   - Comments explaining each task mapping
   - TODO comments for manual review items

5. **Error Handling**
   - Include proper exception handling
   - Add data quality checks where appropriate
   - Implement cleanup tasks for failed runs

## File Structure
```python
# Header with imports and constants
# Helper functions (if needed)
# DAG definition with parameters
# Task definitions in logical order
# Dependency setup matching SSIS precedence
```

## Special Considerations

- **Security**: Do not include actual credentials - use connection IDs and variables
- **Idempotency**: Ensure tasks can be run multiple times safely
- **Monitoring**: Include logging and alerting hooks
- **Performance**: Use appropriate parallelism and resource allocation
- **Maintenance**: Generate clean, readable code with proper structure

## TODO Generation Rules

For unsupported or complex components, generate clear TODO comments with:
1. Description of the original SSIS component
2. Suggested Airflow implementation approach
3. Manual steps required for completion
4. Links to relevant documentation

## Sample Output Structure
The generated DAG should be ready to deploy to an Airflow environment with minimal manual intervention, requiring only connection setup and parameter configuration.