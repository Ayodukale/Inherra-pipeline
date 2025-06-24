✅ Schema at .... means

Your Python script successfully activated its virtual environment.
It correctly loaded your credentials from the .env file.
It connected to Snowflake using the DBT_ROLE.
It successfully queried your curated REFERENCE__JSON_KEYS table.
It merged that information with any existing schema.yml.
It wrote the final, perfect schema.yml file to the correct location.


Here is a comprehensive README for the `scripts` directory. You can save this as `scripts/README.md`.

---

# `scripts/` Directory README

This directory contains utility and automation scripts for the dbt project.

## `generate_schema_yml.py`

### Purpose

This script automates the creation and maintenance of the `schema.yml` file for our staging models. It acts as the bridge between our manually curated metadata in Snowflake and our dbt project configuration.

Its core function is to:
1.  Connect to Snowflake using credentials from the project's `.env` file.
2.  Read the list of columns, their descriptions, nullability status, and custom tests from the `REFERENCE__JSON_KEYS` table.
3.  Intelligently **merge** this information with the existing `models/staging/schema.yml` file.
4.  Preserve any manual descriptions or tests for columns that are no longer in the source table (while printing a warning).
5.  Ensure that `not_null` tests are only applied to columns explicitly marked as `is_nullable = FALSE` in our reference table.

This solves two major problems:
*   **Tedious Manual Work:** Eliminates the need to manually type out dozens or hundreds of column names in the YAML file.
*   **Configuration Drift:** Ensures our dbt tests and descriptions are always in sync with our single source of truth (`REFERENCE__JSON_KEYS`), preventing our `schema.yml` from becoming outdated or incorrect.

### How to Use

#### 1. First-Time Setup

Before running the script for the first time, you must have a Python virtual environment set up with the necessary packages.

```bash
# From the root of the dbt project:

# 1. Create the virtual environment (only needs to be done once)
python3 -m venv .venv

# 2. Activate the virtual environment (must be done in every new terminal session)
source .venv/bin/activate

# 3. Install the required packages
pip install -r scripts/requirements.txt
```

#### 2. Running the Script

Ensure your `.env` file in the project root is up-to-date with the correct Snowflake credentials and context (`DBT_ROLE`, `RAW_DATA_DB`, etc.).

To execute the script and overwrite the `schema.yml` file:

```bash
# Make sure your virtual environment is active!
# You should see (.venv) at the start of your terminal prompt.

python scripts/generate_schema_yml.py
```

#### 3. Dry Run Mode

To see what the generated YAML *would* look like without actually writing to the file, use the `--dry-run` flag. This is useful for verification.

```bash
python scripts/generate_schema_yml.py --dry-run
```

### Key Principles & Things to Keep in Mind

1.  **The Source of Truth is Snowflake:** This script assumes that the `RAW_DATA_DB.dbt_aodukale.REFERENCE__JSON_KEYS` table is the **single source of truth**. If you want to change a description or add a `not_null` test, you must `UPDATE` that table in Snowflake first, then re-run this script. Do not manually edit the `schema.yml` with information that can be stored in the reference table, as your changes will be overwritten.

2.  **The Virtual Environment is Mandatory:** The script will fail if you don't activate the virtual environment (`source .venv/bin/activate`) before running it, as it won't be able to find the required packages (like `snowflake-connector-python`).

3.  **Permissions are Key:** The credentials in your `.env` file, specifically the `SNOWFLAKE_ROLE`, must have `USAGE` and `SELECT` privileges on the `REFERENCE__JSON_KEYS` table. We have configured this to be `DBT_ROLE`.

4.  **Non-Destructive for a Reason:** The script is designed to *merge*, not just overwrite. This is a safety feature. If you have manually added a very complex, multi-line description or a custom test to a column in `schema.yml` that this script doesn't manage, it will be preserved.

### Future Improvements

*   Create a `requirements.txt` file to formalize dependencies.
*   Integrate this script into a CI/CD pipeline (e.g., a GitHub Action) to run automatically on pull requests.

---

