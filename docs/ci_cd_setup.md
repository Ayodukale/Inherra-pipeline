# CI/CD Setup for Inherra dbt Project

This document outlines the Continuous Integration and Continuous Deployment (CI/CD) setup for the Inherra dbt project.

---

## 🧱 Purpose

Ensure every dbt commit is tested, validated, and documented before being merged to production. This protects data contracts, maintains documentation, and automates deploy pipelines.

---

## 📂 GitHub Actions Workflow: `.github/workflows/ci.yml`

### ✅ Tasks:
1. **Checkout Code** – Pull latest code from the repo.
2. **Install Python + Dependencies** – Setup Python env to run scripts like schema generator.
3. **Run `generate_schema_yml.py`** – Auto-sync `REFERENCE__JSON_KEYS` to `schema.yml`.
4. **Install dbt Dependencies** – Pull in any dbt packages needed.
5. **Run dbt Build** – Build staging + intermediate models (`dbt build -m stg_probate_filings_cleaned int_r_score_features`)
6. **Run dbt Tests** – Ensure data quality via tests in `schema.yml`
7. **Generate dbt Docs** – Create HTML site with full DAG + model metadata.
8. **Optional Publish** – Upload docs to GitHub Pages, S3, or another endpoint.

---

## 📌 Setup Instructions
1. Add your `ci.yml` under `.github/workflows/`
2. Confirm GitHub repo secrets hold Snowflake credentials
3. Push a commit to trigger workflow
4. Monitor CI tab in GitHub for job output

---

## 🔁 Recurring Maintenance
- Sync with changes in `REFERENCE__JSON_KEYS` if schema evolves
- Adjust model/test targets if pipeline grows