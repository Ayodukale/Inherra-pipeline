## README: Key Principles for Working with dbt & Snowflake Manually

This guide outlines critical "gotchas" and best practices when interacting with dbt-managed objects directly in the Snowflake UI. Following these principles will prevent common and confusing permission errors.

### Principle 1: The dbt Logs are the Ultimate Truth

*   **The Problem:** Your `profiles.yml` might say one thing, but your `dbt_project.yml` (or other configs) can override the target database and schema. You can't trust your local config to know where dbt *actually* built a table.
*   **The Solution:** Always check the dbt run logs. The `create or replace table ...` line in the log is the **undeniable source of truth** for the exact, three-part name (`DATABASE.SCHEMA.TABLE`) of the object dbt created.

    ```log
    -- The "smoking gun" in your dbt logs:
    create or replace transient table RAW_DATA_DB.dbt_aodukale.json_keys_discovered
    ```

### Principle 2: Snowflake Permissions are Role-Based

*   **The Problem:** An error message like `...does not exist or not authorized` is almost never about the object *not existing*. It's about **your current role** not having permission to see or use it.
*   **The Solution:** Find out which role **owns** the object. You can do this by looking at the `owner` column in the output of `SHOW SCHEMAS;`. To interact with that object, your session **must** assume the owner role first.

    ```sql
    -- The command that gives your session the correct "hat"
    USE ROLE DBT_ROLE;
    ```

### Principle 3: Snowflake Worksheets Have "Amnesia"

*   **The Problem:** When you run a command like `USE ROLE DBT_ROLE;` in the UI, it often only applies to the *very next execution*. After that execution finishes, the worksheet session can revert to its default role, making subsequent commands fail with permission errors.
*   **The Solution:** For any multi-step manual process, you must guarantee the session context persists. You have two options:
    1.  **Run As One Block:** Highlight all commands (from `USE ROLE` to the final `UPDATE`) and run them as a single execution.
    2.  **Use a Bulletproof Scripting Block:** For guaranteed success, wrap your entire logic in a `BEGIN...END;` block. This forces all commands to run in a single, reliable transaction where the role is guaranteed to persist.

    ```sql
    -- The bulletproof pattern to defeat session "amnesia"
    DECLARE
      -- variables if needed
    BEGIN
      USE ROLE DBT_ROLE;
      USE WAREHOUSE COMPUTE_WH;
      
      -- ... your CREATE and UPDATE statements here ...

      RETURN 'SUCCESS';
    END;
    ```

## 🔑 Key Principles for Working with dbt & Snowflake Manually (Extended)

### Principle 4: Identifier Casing & Quoting in Snowflake
- **Unquoted names** (e.g. `foo_bar`) are interpreted as **UPPERCASE** by Snowflake (`FOO_BAR`).
- **Quoted names** (e.g. `"foo_bar"`) are case-sensitive and must match exactly.
- ❗ **Error tip:** If you get `invalid identifier`, double-check whether casing or quotes are mismatched.
- ✅ **Best practice:** In dbt, use lowercase and avoid quotes — dbt will quote & uppercase automatically.

---

### Principle 5: Flatten JSON Exactly Once
- Flattening JSON should **only happen once** — in your earliest `stg_*` model.
- Do not re-flatten downstream models; instead, **select from the already-unpacked fields**.
- ✅ Use a macro or staging model like `stg_probate_filings_flattened` to centralize this.

---

### Principle 6: Layered CTE-Style Modeling
Break transformations into small, composable layers:

| Layer         | Purpose                                         |
|---------------|--------------------------------------------------|
| `stg_*`       | Unpack raw records, rename fields, cast types   |
| `cleaned_*`   | Clean up dirty values, default nulls, regex     |
| `int_*`       | Business logic, joins, flags, scoring           |
| `mart_*`      | Final tables used for reporting or syncing      |

Each model should **do one job only**, like a clean function.

---

### Principle 7: dbt CLI Hygiene
- `dbt clean` → clears the `target/` folder. Run before major refactors.
- `dbt deps` → installs packages (like `dbt-utils`) after any changes to `packages.yml`.
- `dbt run -m model_name` → run a specific model.
- `dbt test -m model_name` → test a specific model or layer.

---

### Principle 8: Preserve Schema Test Context
- Keep `schema.yml` test definitions **next to each model** in `models/staging/`, `models/intermediate/`, etc.
- If using automated schema generation (like with `generate_schema_yml.py`), use **merge logic** to:
  - ✅ Add new columns
  - ✅ Preserve existing descriptions
  - ✅ Prevent overwriting custom tests

---

### Principle 9: Role Awareness
- ❗**Use the correct Snowflake role** when running manual SQL scripts or querying new objects.
- After running `USE ROLE DBT_ROLE;`, keep that session open if doing multiple manual updates.
- ✅ Prefer scripts that set context inside a `BEGIN...END` block to avoid role/session issues.

---

### Principle 10: Maintain a Source of Truth Table
- `REFERENCE__JSON_KEYS` should be your single source of truth for:
  - Column names
  - Descriptions
  - Required/optional (nullability)
  - Custom tests
- This allows **automated schema.yml generation** to be accurate and non-destructive.
