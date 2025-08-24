# SSIS → Airflow / dbt Migration Blueprint (LLM-Assisted)

> Snowflake + Python stack • Open-source friendly • Designed for Claude (or similar LLMs) to implement

---

## 0) Goal & Decision Rule

- **Goal:** Automatically migrate existing **SSIS** packages (`.dtsx`) into:
  - **Airflow** DAGs when the package includes **ingestion** (files/APIs/DB loads/orchestration).
  - **dbt** projects when the package is **transformations-only** (ELT in Snowflake).
- **Mixed packages:** Use **Airflow** to orchestrate ingestion and trigger **dbt** for transforms (via Astronomer Cosmos or a dbt operator pattern).

---

## 1) High-Level Architecture

```
SSIS (.dtsx XML)
        │
        ▼
[Deterministic Parser]
  - Extract control flow, data flow components, SQL, variables,
    connection managers, precedence constraints, protection level
        │
        ▼
           Intermediate Representation (IR, JSON Graph)
        │
        ├───────────────► [Generator: Airflow]
        │                    - Map SSIS tasks/precedence→Operators/TaskGroups
        │                    - Snowflake operators for SQL/loads
        │                    - Dynamic task mapping for loops
        │                    - Branching for expressions
        │
        └───────────────► [Generator: dbt]
                             - Map data-flow transforms→SQL models
                             - Use sources, refs, tests, incremental merges
                             - Jinja variables/macros for params
```

**Inputs:** `.dtsx` files (XML), optional password (if `ProtectionLevel` encrypts sensitive properties).  
**Outputs:** 
- Airflow: `dags/<package>_dag.py` (+ helpers).  
- dbt: `models/*.sql`, `schema.yml`, `dbt_project.yml`.  
- A **migration report** per package (coverage, TODOs).

---

## 2) Intermediate Representation (IR)

Define a stable JSON schema produced by the parser and consumed by generators.

```json
{
  "package_name": "MyPackage",
  "protection_level": "DontSaveSensitive | EncryptSensitiveWithPassword | ...",
  "parameters": [{ "name": "p_cutoff_dt", "type": "DateTime", "value": null }],
  "variables":  [{ "name": "v_batch_id", "type": "Int32",   "value": 0 }],
  "connection_managers": [
    {
      "id": "CM1",
      "type": "OLEDB | ADO.NET | FLATFILE | SNOWFLAKE | FILE | HTTP",
      "properties": { "server": "...", "database": "...", "auth": "redacted" },
      "sensitive": true
    }
  ],
  "executables": [
    {
      "id": "Package\\ExecSQL1",
      "type": "ExecuteSQL",
      "object_name": "ExecSQL1",
      "dialect": "tsql | ansi | snowflake",
      "sql": "DELETE FROM STG_ORDERS WHERE LOAD_DT < @p_cutoff_dt;",
      "parameter_refs": ["p_cutoff_dt"]
    },
    {
      "id": "Package\\DFT_Orders",
      "type": "DataFlow",
      "object_name": "DFT_Orders",
      "components": [
        { "id": "src1",  "component_type": "OLEDBSource",    "sql": "SELECT ...", "outputs": ["p1"] },
        { "id": "dc1",   "component_type": "DerivedColumn",  "expression": "...", "inputs": ["p1"], "outputs": ["p2"] },
        { "id": "lkp1",  "component_type": "Lookup",         "join_on": ["key"],  "ref": "DIM_X",   "inputs": ["p2"], "outputs": ["p3"] },
        { "id": "dest1", "component_type": "SnowflakeDest",  "table": "STG_ORDERS", "inputs": ["p3"], "mode": "append|merge" }
      ]
    }
  ],
  "edges": [
    {
      "from": "Package\\ExecSQL1",
      "to":   "Package\\DFT_Orders",
      "condition": "Success | Failure | Completion | Expression",
      "expression": "@[User::v_flag] == 1"
    }
  ],
  "expressions": [
    { "scope": "Package\\ExecSQL1", "property": "SqlStatementSource", "expr": "\"DELETE FROM ...\" + (DT_STR, 30, 1252) @[User::v_suffix]" }
  ]
}
```

**Notes**
- `executables[*].type` mirrors SSIS task types (e.g., `ExecuteSQL`, `SSIS.Pipeline (DataFlow)`, `ScriptTask`, `SequenceContainer`, `ForEachLoop`, etc.).
- Data Flow components carry minimal, **vendor-neutral** semantics needed to generate dbt SQL or ingestion tasks.
- `edges` encode **precedence constraints** and expressions.
- Carry both literal property values **and** property expressions (SSIS may override via expressions).

---

## 3) Parser Design (DTSX → IR)

**Core steps**
1. **Load XML** with namespace map for `DTS`, `SQLTask`, `Pipeline` etc.
2. **Read package metadata**: name, `ProtectionLevel`, parameters, variables.
3. **Connection Managers**: capture type and non-sensitive properties; mark sensitive fields as redacted.
4. **Executables**:
   - Identify each `DTS:Executable` and its `DTS:ExecutableType`.
   - For **Execute SQL Task**: pull inline SQL from `SQLTask:SqlStatementSource` or resolve variable/expression references.
   - For **Data Flow**: walk `pipeline/components/component` tree, record each component (Source/Derived/Lookup/UnionAll/Destination), including `SqlCommand` for sources and table/merge mode for destinations; capture component linkages via `paths`.
   - For **Containers** (Sequence, ForEach): record structure; nest children or flatten with generated TaskGroup label.
   - For **Script Task / Script Component**: extract embedded code or file path if present.
5. **Precedence Constraints**: materialize to `edges` with success/failure/completion and optional boolean/expression rule.
6. **Expressions**: collect property expressions at package/task level.
7. **Normalize SQL dialect** (rough heuristic; T-SQL vs Snowflake): used by the generator for translation prompts.

**Security / ProtectionLevel**
- If sensitive values are encrypted (`EncryptSensitiveWithPassword`), do **not** attempt to decrypt unless the user provides the password explicitly; otherwise mark sensitive fields as redacted and emit connection placeholders for Airflow/dbt.

---

## 4) Mapping: IR → Airflow (ingestion or mixed)

### 4.1 Operator mapping

| SSIS Task / Concept | Airflow Equivalent |
|---|---|
| Execute SQL Task | `SnowflakeOperator` (or `SQLExecuteQueryOperator` w/ Snowflake conn) |
| Script Task | `PythonOperator` (inline or import a module) |
| File ingest (Flat File Source → table) | Python stage upload + `COPY INTO` task |
| Sequence Container | `TaskGroup` |
| ForEach Loop | Dynamic task mapping (`@task` or `expand()` API) |
| Event Handlers (OnError/OnPostExecute) | Callbacks or explicit branches |
| Precedence: Success/Failure/Completion | Dependency edges + `trigger_rule` (`ALL_SUCCESS` / `ONE_FAILED` / `ALL_DONE`) |
| Precedence: Expression | `BranchPythonOperator` or `ShortCircuitOperator` with templated params |

**Snowflake specifics**
- Prefer staging files (external/internal) + `COPY INTO <db>.<schema>.<table>`.
- Ensure idempotency (file naming with content hash or load history check; use `FORCE=FALSE` if appropriate).
- Use a single `SNOWFLAKE_CONN_ID` and parametrize database/schema/warehouse via `dagrun.conf` or Variables.

### 4.2 DAG shape

- Use **`TaskGroup`s** for containers and data-flow groupings.
- Map edges 1:1 from IR to `>>` dependencies.
- Convert SSIS expressions (`@[User::var]`) into **Airflow params** (`{{ dag_run.conf.get("var") }}`) or Variables (`{{ var.value.var }}`).
- Where SSIS uses **Failure** or **Completion** paths, set `trigger_rule="ONE_FAILED"` or `ALL_DONE` on the downstream tasks.

### 4.3 Example DAG skeleton (generated)

```python
from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.models.param import Param
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

SNOWFLAKE_CONN_ID = "snowflake_default"

def should_run(**ctx):
    # Example for SSIS expression @[User::v_flag] == 1
    return "dft_orders" if int(ctx["dag_run"].conf.get("v_flag", 0)) == 1 else "skip_task"

with DAG(
    dag_id="pkg_mypackage",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={"v_flag": Param(0, type="integer")},
    default_args={"retries": 1},
    render_template_as_native_obj=True,
) as dag:

    execsql1 = SnowflakeOperator(
        task_id="execsql1",
        sql="DELETE FROM STG_ORDERS WHERE LOAD_DT < TO_DATE({{ dag_run.conf['p_cutoff_dt'] }})",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    branch = BranchPythonOperator(task_id="branch_on_flag", python_callable=should_run, provide_context=True)

    with TaskGroup(group_id="dft_orders") as dft_orders:
        # Example: stage + COPY INTO for ingestion, or call to dbt for transforms
        def upload_to_stage(**_):
            # implement stage upload (S3/GCS/Azure Blob) as needed
            pass

        stage_upload = PythonOperator(task_id="stage_upload", python_callable=upload_to_stage)

        copy_into = SnowflakeOperator(
            task_id="copy_into_dest",
            sql="COPY INTO STG_ORDERS FROM @mystage/path/ FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1) ON_ERROR=CONTINUE;",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
        )

        stage_upload >> copy_into

    execsql1 >> branch
    branch >> dft_orders
```

---

## 5) Mapping: IR → dbt (transformations-only)

### 5.1 Component mapping

| Data Flow Component | dbt Model Translation |
|---|---|
| OLE DB / ADO.NET Source | `source()` or `ref()` with a `SELECT` |
| Derived Column | Add computed columns in `SELECT` |
| Lookup | `JOIN` to the lookup table (dim) |
| Conditional Split | Split into multiple models with `WHERE`, or use `CASE WHEN` |
| Union All | `UNION ALL` |
| Destination (table) | Use **materializations**: `table`, `view`, or **incremental** (`merge`) |

### 5.2 dbt incremental pattern (Snowflake)

```sql
-- models/stg_orders.sql
{{ config(materialized='incremental', unique_key='order_id', on_schema_change='sync_all_columns') }}

select
  o.order_id,
  o.customer_id,
  o.order_dt,
  -- derived columns example
  date_trunc('day', o.order_dt) as order_dt_day
from {{ source('raw', 'orders') }} o

{% if is_incremental() %}
  where o.order_dt >= (select coalesce(max(order_dt), to_date('1900-01-01')) from {{ this }})
{% endif %}
```

**schema.yml** with tests:

```yaml
version: 2
sources:
  - name: raw
    schema: RAW
    tables:
      - name: orders

models:
  - name: stg_orders
    tests:
      - unique:
          column_name: order_id
      - not_null:
          column_name: order_id
```

---

## 6) Claude Prompt Contracts (copy/paste)

### 6.1 Airflow code generator (per package)

**System Prompt**

```
You generate production-grade Apache Airflow 2.8+ DAGs for Snowflake and Python.
Use apache-airflow-providers-snowflake and python stdlib/snowflake-connector-python.
Requirements:
- Map SSIS precedence to Airflow dependencies and trigger rules exactly.
- Use TaskGroups for SSIS containers; use dynamic task mapping for ForEach loops.
- Use a constant SNOWFLAKE_CONN_ID and parameterize DB/SCHEMA/WAREHOUSE via params.
- Idempotency: prefer staged files + COPY INTO; avoid destructive SQL without safeguards.
- Emit clear TODOs for unsupported SSIS components; do not silently drop nodes.
- Include docstrings explaining mappings and assumptions.
```

**User Prompt Template**

```
Given the following IR (SSIS→IR JSON), generate a complete Airflow DAG.

IR JSON:
<IR_JSON_HERE>

Mapping Hints:
- ExecuteSQL → SnowflakeOperator
- ScriptTask → PythonOperator
- File ingest → PythonOperator (stage upload) + SnowflakeOperator (COPY INTO)
- SequenceContainer → TaskGroup
- ForEachLoop → dynamic task mapping
- Precedence (Success/Failure/Completion/Expression) → deps + trigger_rule + BranchPythonOperator

Output:
- dags/<safe_package_name>_dag.py (single file), ready to run.
- Do not include secrets. Use SNOWFLAKE_CONN_ID placeholder.
- Insert TODO comments where manual attention is required.
```

### 6.2 dbt code generator (per package)

**System Prompt**

```
You generate a dbt project for Snowflake.
Use Jinja, ref(), source(), tests, and appropriate materializations.
For incremental models, use merge with a clear unique_key.
Emit a migration note per model indicating which SSIS component(s) it maps from.
```

**User Prompt Template**

```
Given the following IR (filtered to transformation tasks only), generate a dbt project.

IR JSON:
<IR_JSON_TRANSFORM_ONLY>

Output:
- dbt_project.yml (with sensible model paths and naming)
- models/* .sql
- models/schema.yml (sources + tests)
- macros/* only if necessary
- No secrets; assume profiles.yml is configured externally.
```

---

## 7) Project Skeleton

```
/ssis-migrator
  /parser
    dtsx_reader.py
    dataflow_parser.py
    precedence_parser.py
    expressions.py
    secure.py
    to_ir.py
  /generators
    airflow_gen.py
    dbt_gen.py
    prompts/
      airflow_system.txt
      airflow_user_template.md
      dbt_system.txt
      dbt_user_template.md
  /adapters
    snowflake.py
  /cli
    main.py
```

**CLI usage**

```
ssis-migrate   --in ./packages/MyPackage.dtsx   --mode auto|airflow|dbt   --out ./build   [--password <dtsx_password_if_needed>]
```

---

## 8) Parser Hints (Python)

### 8.1 Namespaces & skeleton

```python
from lxml import etree

NS = {
    "DTS": "www.microsoft.com/SqlServer/Dts",
    "SQLTask": "www.microsoft.com/sqlserver/dts/tasks/sqltask",
    "Pipeline": "www.microsoft.com/sqlserver/dts/pipeline",
}

tree = etree.parse("MyPackage.dtsx")
root = tree.getroot()

# Executables
for exe in root.xpath(".//DTS:Executable", namespaces=NS):
    etype = exe.get("{%s}ExecutableType" % NS["DTS"])
    oname = exe.get("{%s}ObjectName" % NS["DTS"])
    # ObjectData contains task-specific payload
    obj = exe.find("DTS:ObjectData", namespaces=NS)
    # ... inspect etype, parse SQLTask or Pipeline payloads ...
```

### 8.2 Execute SQL Task (inline SQL or via expressions)

```python
sql_node = exe.xpath(".//SQLTask:SqlTaskData", namespaces=NS)
if sql_node:
    # inline SQL
    stmt = sql_node[0].get("SqlStatementSource") or ""
    # parameters via ResultSet/ParameterBindings if present
```

### 8.3 Data Flow (components & paths)

```python
pl = exe.xpath(".//Pipeline:Pipeline", namespaces=NS)
if pl:
    for comp in pl[0].xpath(".//Pipeline:component", namespaces=NS):
        ctype = comp.get("componentClassID")  # map classID to friendly type
        cname = comp.get("name")
        # Example: SQL command on source
        for prop in comp.xpath(".//Pipeline:properties/Pipeline:property", namespaces=NS):
            if prop.get("name") == "SqlCommand":
                sql_cmd = (prop.text or "").strip()
    # paths define wiring between components
    for path in pl[0].xpath(".//Pipeline:path", namespaces=NS):
        start = path.get("startID"); end = path.get("endID")
```

### 8.4 Precedence constraints

```python
for pc in root.xpath(".//DTS:PrecedenceConstraint", namespaces=NS):
    from_task = pc.get("From")
    to_task = pc.get("To")
    ev = pc.get("Value")  # Success|Failure|Completion
    # expression may be nested; capture text if present
```

> Tip: Preserve both literal property values and **property expressions** (SSIS expression language) for later resolution by generators.

---

## 9) Dialect Conversion (T-SQL → Snowflake)

Common rewrites:
- `GETDATE()` → `CURRENT_TIMESTAMP()`
- `ISNULL(a,b)` → `COALESCE(a,b)`
- Temp tables (`#t`) → CTEs or transient tables; prefer CTEs in dbt models.
- `TOP n` → `FETCH FIRST n ROWS ONLY` (with `ORDER BY`), or `QUALIFY ROW_NUMBER()` patterns.
- `IDENTITY` → sequences or `AUTOINCREMENT` equivalent if needed.
- `LEN()` → `LENGTH()`
- `DATEADD(day, n, x)` → `DATEADD(day, n, x)` (compatible), watch interval semantics.

In Claude prompts, request a **diff note** for each rewritten statement.

---

## 10) Validation & Rollout

1. **Inventory:** run parser across all packages, emit IR JSON + coverage report (tasks by type, encrypted properties, unknown components).
2. **Classify:** if any ingestion present → Airflow; else → dbt.
3. **Generate:** call Claude with the IR + the corresponding prompt contract.
4. **Secrets:** create Airflow Connections and (external) dbt `profiles.yml` for Snowflake.
5. **Static checks:** ensure all IR nodes mapped; all edges present; `trigger_rule`s correct.
6. **Dry-run:** Airflow `rendered` check; dbt `run --select state:modified+` on dev.
7. **Parallel run (optional):** orchestrate legacy SSIS from Airflow during cutover.
8. **Cutover & Decommission.**

---

## 11) Deliverables Checklist

- [ ] Parser producing IR JSON (with tests on sample `.dtsx`).
- [ ] Airflow generator (unit tests for precedence/trigger rules).
- [ ] dbt generator (unit tests for incremental merges, model dependency graph).
- [ ] Claude prompt files (system + user templates).
- [ ] Migration report per package (CSV/Markdown).

---

## 12) Example: Minimal IR → Artifacts (illustrative)

**IR snippet**

```json
{
  "package_name": "OrdersLoad",
  "executables": [
    {"id":"\\ExecSQL1","type":"ExecuteSQL","sql":"DELETE FROM STG_ORDERS WHERE LOAD_DT < :p_cutoff_dt"},
    {"id":"\\DFT_Orders","type":"DataFlow","components":[
      {"id":"src","component_type":"OLEDBSource","sql":"SELECT * FROM RAW_ORDERS"},
      {"id":"dest","component_type":"SnowflakeDest","table":"STG_ORDERS","mode":"append"}
    ]}
  ],
  "edges":[{"from":"\\ExecSQL1","to":"\\DFT_Orders","condition":"Success"}]
}
```

**Expected outputs**
- **Airflow**: `dags/ordersload_dag.py` wiring `ExecSQL1 >> DFT_Orders` and implementing stage+COPY (if ingestion) or calling dbt (if transform).
- **dbt**: `models/stg_orders.sql` selecting from `source('raw','orders')` with schema tests.

---

## 13) Notes on the DTSX Structure (for parser authors)

- SSIS packages are **XML**; control-flow tasks are `DTS:Executable` elements with `DTS:ExecutableType`. The **task payload** is under `DTS:ObjectData` (e.g., `SQLTask:SqlTaskData` for Execute SQL, `Pipeline:Pipeline` for Data Flow).  
- Precedence constraints are declared as `DTS:PrecedenceConstraint` with `From`, `To`, and `Value` (Success/Failure/Completion) plus optional expression nodes.  
- Variables/parameters and property expressions may override task properties; capture both literal and expression forms to avoid losing dynamic behavior.

*(This mirrors Microsoft's DTSX schema guidance from the attached reference.)*

---

## 14) License & Open-Source Stance

- Keep this project **Apache-2.0** to interoperate with Airflow/dbt ecosystems.
- Clearly label any generated code segments and include a header with the origin (package name, timestamp, generator version).

---

## 15) Next Steps

- Implement `/parser` and emit IR on a few representative packages.
- Add generator stubs, then wire Claude prompts to produce first DAG/dbt outputs.
- Iterate with a human-in-the-loop review, then expand coverage for more SSIS components.
