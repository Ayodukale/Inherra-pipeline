Dataflow / Architecture Walkthrough

flowchart TD
    A[PROBATE_FILINGS_ENRICHED (raw JSON)] --> B[reference__json_keys_discovered.sql]
    B -->|manual script| C[REFERENCE__JSON_KEYS]

    A --> D[stg_probate_filings_cleaned.sql]
    C --> D
    D --> E[int_r_score_features.sql]
    E --> F[Lead Scoring / R-Code system]

    G[generate_schema_yml.py] --> C
    C --> H[schema.yml]
    H -->|dbt tests| D & E


💡 What Each Component Does
 Component                             | Role                                                                                   |
| ------------------------------------- | -------------------------------------------------------------------------------------- |
| `PROBATE_FILINGS_ENRICHED`            | Raw scraped JSONs from court filings                                                   |
| `reference__json_keys_discovered.sql` | Discovers every JSON key found in raw data (dbt model)                                 |
| `REFERENCE__JSON_KEYS`                | Manually curated Snowflake table (your “source of truth”) with metadata for schema.yml |
| `generate_schema_yml.py`              | Script that reads `REFERENCE__JSON_KEYS` and auto-generates `schema.yml`               |
| `schema.yml`                          | dbt test definitions for `stg_probate_filings_cleaned`                                 |
| `stg_probate_filings_cleaned.sql`     | Flattens JSON → structured rows                                                        |
| `int_r_score_features.sql`            | Adds scoring features for leads                                                        |
| `dbt build`                           | Runs transformations and tests                                                         |
| `mcp-snowflake-service`               | Enables Claude to inspect and debug your models in Cursor                              |

🧠 Why This Architecture Is Smart
Source of truth lives in the warehouse (REFERENCE__JSON_KEYS)

Schema is auto-generated, not manually maintained — less error-prone

Claude debugging is possible via MCP server + Cursor

You have a clean separation:

Discovery (discovered keys)

Curation (manual table)

Transformation (dbt)

Validation (schema.yml)