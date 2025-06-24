# Inherra dbt Project

This project manages the data transformation pipeline for probate and property data. It includes a custom, semi-automated system for generating and maintaining dbt schemas from a central source of truth in Snowflake.

---

## 🚀 Key Components

-   **/models:** Contains all dbt data models.
    -   `/models/staging`: The first layer of transformation, cleaning and casting raw JSON data.
-   **/scripts:** Contains helper scripts for automation.
    -   `generate_schema_yml.py`: The core script that reads our Snowflake reference table and generates a `schema.yml` file.
    -   `README.md`: Explains how the Python script works.
-   **MAINTENANCE_GUIDE.md:** Your day-to-day operations manual for keeping the project in sync with new data.
-   **.github/workflows/ci.yml:** The GitHub Actions workflow that automates all quality checks for every pull request.
-   **profiles.yml:** (Located in project root) Your **local** connection configuration for dbt. **This file is not committed to Git.**
-   **.env:** (Located in project root) Your **local** connection configuration for the Python script. **This file is not committed to Git.**

---

## 💻 Local Development Setup

To run this project on a new machine:

1.  **Clone the repository:**
    ```bash
    git clone git clone https://github.com/Ayodukale/Inherra-pipeline.git

    cd dbt
    ```

2.  **Set up Python Environment:**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r dbt/scripts/requirements.txt
    pip install dbt-snowflake
    ```

3.  **Configure Local Credentials:**
    -   Create a `profiles.yml` file in the project root with your local dbt connection details.
    -   Create a `.env` file in the project root with your Snowflake connection details for the Python script.

4.  **Run dbt:**
    -   You can now run dbt commands like `dbt build` and `dbt docs generate`.

---

## 🔄 Recurring Maintenance Workflow

This is your primary recurring task to keep the project up-to-date. The full details are in the **[MAINTENANCE_GUIDE.md](MAINTENANCE_GUIDE.md)**.

The short version is:

1.  **Run the discovery model** in dbt to find new JSON keys.
2.  **Compare the results** against your `REFERENCE__JSON_KEYS` table in Snowflake to identify new fields.
3.  **`INSERT` the new fields** into `REFERENCE__JSON_KEYS` and define their metadata (description, nullability).
4.  **Run the Python script** (`python scripts/generate_schema_yml.py`) to update your project's `schema.yml`.
5.  **Commit the changes** to your `schema.yml` file.

---

## 🤖 CI/CD Automation

This project is configured with a GitHub Actions workflow (`.github/workflows/ci.yml`).

### What It Does
This workflow runs automatically on every Pull Request that modifies dbt models or related files. It acts as a robotic quality assurance engineer to:
1.  Set up a clean environment.
2.  Securely connect to Snowflake using GitHub Secrets.
3.  Run the `generate_schema_yml.py` script to ensure schema is up-to-date.
4.  Run `dbt build` to build all modified models and run all associated tests.
5.  **Block the pull request from being merged if any step fails.**

This provides a critical safety net, ensuring that no broken code or failing tests ever make it into the `main` branch.

### What You Need to Do
Nothing. The workflow is fully automated. As long as your changes pass the checks, you will see a green checkmark on your pull request.