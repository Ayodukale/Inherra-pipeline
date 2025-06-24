# 🔁 dbt Metadata Maintenance Guide

This guide outlines the periodic tasks required to keep your dbt project aligned with evolving data — especially when working with dynamic JSON data.

---

## 🔍 1. Detect New JSON Fields

Use your discovery model to find fields not yet in your official schema:

```sql
-- Run this in Snowflake:
SELECT column_name 
FROM RAW_DATA_DB.DBT_AODUKALE.JSON_KEYS_DISCOVERED
EXCEPT
SELECT column_name 
FROM RAW_DATA_DB.DBT_AODUKALE.REFERENCE__JSON_KEYS;
```

---

## ➕ 2. Add New Fields to REFERENCE__JSON_KEYS

For each new field, manually insert metadata into your reference table:

```sql
INSERT INTO RAW_DATA_DB.DBT_AODUKALE.REFERENCE__JSON_KEYS (
    column_name, data_type, description, is_nullable, custom_tests
) VALUES (
    'NEW_FIELD_NAME', NULL, '', TRUE, NULL
);
```

Then update `is_nullable = FALSE` or add `custom_tests` like `['email', 'unique']` as needed.



---

## 🔁 3. Regenerate Schema

Once you've updated the reference table:

```bash
cd scripts
python generate_schema_yml.py
```

This will:
- Add new fields
- Preserve descriptions/tests for existing fields
- Retain old fields that are still valid

---

## ✅ 4. Validate Schema

Make sure dbt accepts the updated schema:

```bash
dbt build -m stg_probate_filings_cleaned
```

Then optionally regenerate docs:

```bash
dbt docs generate
dbt docs serve
```

---

## 💡 Tips

- Run the schema generator often, especially after pipeline changes
- If using GitHub Actions, consider running the Python script as a pre-`dbt build` step
- Backup your schema.yml if editing by hand (though the script handles this too)

---

## 🔐 Reminder

All Snowflake connection settings are pulled from `.env`:

```
SNOWFLAKE_USER=
SNOWFLAKE_PRIVATE_KEY_PATH=
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_ROLE=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
```



## 🛠 Maintenance Task: Update `stg_probate_filings_cleaned.sql` UPDATE, Read scratch that below

### When to do this:
- A new key appears in `reference__json_keys_discovered`
- You manually add a new row to `REFERENCE__JSON_KEYS`
- You change business logic (e.g. new filters, data type coercion, etc.)
- You add tests or descriptions that rely on clean column values

### What to do:
1. Run:
   ```sql
   select column_name from reference__json_keys_discovered
   except
   select column_name from reference__json_keys;

to identify new or missing keys.

2. Manually update `REFERENCE__JSON_KEYS` if needed.

3. Update the model `models/staging/stg_probate_filings_cleaned.sql`:

   * Select all columns from the raw JSON (flattened).
   * Rename/alias columns if needed.
   * Add filtering, parsing, type coercion (e.g. `TRY_TO_DATE`, `TRY_TO_NUMBER`).
   * Add any NULL checks or replacements.

4. Run:

   ```bash
   dbt run -m stg_probate_filings_cleaned
   ```

5. Run:

   ```bash
   dbt test -m stg_probate_filings_cleaned
   ```

6. (Optional) Re-generate docs:

   ```bash
   dbt docs generate
   dbt docs serve
   ```

```

## Scratch that

Good news! Our new design makes this even simpler. You do NOT need to do this.
The SQL file we wrote for stg_probate_filings_cleaned.sql is designed to be "dynamic." It explicitly lists every single column. When a new column is added to the REFERENCE__JSON_KEYS table and you regenerate the schema.yml, the stg_probate_filings_cleaned model will fail its tests because the schema.yml will now list a column that the SQL doesn't produce.
This failure is a good thing. It forces you to go into stg_probate_filings_cleaned.sql and add the one new line for the new key (e.g., raw_record:NEW_FIELD_NAME::string as NEW_FIELD_NAME).


### 🛠 Key Maintenance Scripts

| Script                   | Purpose                                                     |
| ------------------------ | ----------------------------------------------------------- |
| `generate_schema_yml.py` | Auto-generates/upgrades `schema.yml` using curated metadata |
| `server.py`              | FastAPI app for MCP/Claude integration in Cursor            |
| `snowflake_upload.py`    | Loads enriched CSVs into staging Snowflake tables           |
| `notion_sync.py`         | Pushes final leads into Notion CRM                          |

---

### 🧪 Testing Commands

```bash
# Build and test your cleaned model
dbt build -m stg_probate_filings_cleaned

# (Re)generate docs for new schema/test coverage
dbt docs generate
dbt docs serve
````

---

### 🗺 Model Lineage

* `RAW_RECORD` → `stg_probate_filings_cleaned`
* → `int_r_score_features` (adds features like ownership\_match, vacant, etc.)
* → `prd_probate_leads` (final output)
* → Notion sync (external delivery)

---

### 📍Next Up (Post-Onboarding)

* Finalize `int_r_score_features` logic
* Add `ready_to_push = TRUE` flags
* Integrate into GitHub CI/CD
* Begin renovation leads tagging module

---

### 📂 Suggested Folder Tree Update

```bash
/dbt
  /models
    /reference
      json_keys_discovered.sql
    /staging
      stg_probate_filings_cleaned.sql
    /intermediate
      int_r_score_features.sql
    /production
      prd_probate_leads.sql
  /scripts
    generate_schema_yml.py
    snowflake_upload.py
    notion_sync.py
    server.py
  /docs
    claude_integration.md
    mcp_setup.md
  schema.yml
```

---

### ✍️ Update `MAINTENANCE_GUIDE.md` — Additions

---

## 🧠 AI-Assisted Debugging with Claude (via MCP)

Once your MCP server (`server.py`) is running at `http://localhost:8000` and connected to Cursor:

* Open any `.sql` model file in Cursor
* Press `Command + L` to launch Claude
* Ask Claude questions like:

  * “Explain this model”
  * “Why is this test failing?”
  * “What columns feed into `int_r_score_features`?”

Claude uses context from:

* `schema.yml`
* Connected dbt models
* Docs generated via `dbt docs generate`

> ⚠️ Claude integration requires the **MAX** plan.

---

## 🔒 .env Setup (Secrets)

Set these locally before running scripts:

```bash
# Claude
export CLAUDE_API_KEY="sk-..."

# Snowflake
export SNOWFLAKE_USER="..."
export SNOWFLAKE_PASSWORD="..."
export SNOWFLAKE_ACCOUNT="..."
export SNOWFLAKE_DATABASE="..."
export SNOWFLAKE_SCHEMA="..."
```

---

## 🛠 Periodic Script Execution (Manual Runbook)

### Run weekly or after major scrapes:

1. Run dbt to detect new fields:

```bash
dbt run -m reference.json_keys_discovered
```

2. In Snowflake, find new JSON keys:

```sql
SELECT column_name 
FROM reference.json_keys_discovered
EXCEPT
SELECT column_name 
FROM reference.reference__json_keys;
```

3. For each new column, insert it into your key reference table:

```sql
INSERT INTO reference.reference__json_keys (
  column_name, data_type, description, is_nullable, custom_tests
) VALUES (
  'NEW_FIELD', NULL, '', TRUE, NULL
);
```

4. Regenerate your schema.yml file:

```bash
cd scripts
python generate_schema_yml.py
```

5. Build and test the updated model:

```bash
dbt build -m stg_probate_filings_cleaned
```

---

```
```


## 🔐 Analyze models/tests, 🧠 Claude debugging across multiple dbt models, 🧪 Let Claude refine test coverage & tagging		




## 🧭 The Point: Separating "Discovery" from "Curation"

Imagine two distinct roles in your data pipeline:

---

### 🥇 Job 1: The Prospector (Automated Discovery)

- This role is played by your dbt model: `json_keys_discovered`.
- Its job is to **automatically scan raw JSON data** and surface **every key** it finds.
- No human input required — just run:

  ```bash
  dbt run -m reference.json_keys_discovered
````

* Output: A table listing **all observed keys**, including new/unexpected ones.

---

### 🥈 Job 2: The Librarian (Human Curation)

* This is **your job** as the domain expert.

* You review the output from the Prospector and decide:

  * Is `new_weird_field` meaningful or junk?
  * Should `prior_years_taxes_due` be a `number` or `text`?
  * Is `case_id` **always required** (`is_nullable = FALSE`)?

* Your curated decisions go into the official table:
  `REFERENCE__JSON_KEYS`

---

## 🧩 The Bridge Between: `generate_schema_yml.py`

* This Python script translates your **curated REFERENCE\_\_JSON\_KEYS** table into a valid dbt `schema.yml` file.
* It **automates configuration**, but only from vetted, human-approved decisions.

---

## 🚫 Why Not Fully Automate This?

Technically, you **could** chain everything:

* Auto-discovery writes directly into `REFERENCE__JSON_KEYS`
* That triggers the Python script to generate `schema.yml`

But we **intentionally avoid** full automation. Here's why:

### ❌ Example 1: Garbage Field Pollution

If the source system suddenly emits a field like `test_field_123`,
a fully automated system would:

* Accept it
* Add it to your `schema.yml`
* Load it into your warehouse

Result: 🧟 Polluted schema, reduced trust.

### ❌ Example 2: Misinterpreted Fields

A field named `total_value` might look like a number...
But you know it’s actually a **currency string** requiring special logic.

Only a **human** can make that call.

---

## ✅ The Manual Step = Your Quality Gate

The **Librarian step** ensures:

* Only fields that are **understood**
* That are **correctly typed**
* And **deliberately approved**

…get into your production models.

---

## 🔁 How Often Do You Run It?

### 🗓 Weekly or Per Feature:

1. Run discovery:

   ```bash
   dbt run -m reference.json_keys_discovered
   ```

2. In Snowflake, compare discovered keys vs curated ones:

   ```sql
   SELECT column_name 
   FROM reference.json_keys_discovered
   EXCEPT
   SELECT column_name 
   FROM reference.reference__json_keys;
   ```

3. For each new field, insert it into `REFERENCE__JSON_KEYS` with proper metadata.

4. Regenerate schema file:

   ```bash
   python scripts/generate_schema_yml.py
   ```

---

⏱ Time cost: 5–10 minutes
✅ Outcome: Clean, deliberate, reliable dbt models
🧠 Philosophy: Automated discovery + human intent = high data quality

```
```
---end of guide----