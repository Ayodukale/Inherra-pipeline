# Inherra Scraper Pipeline (v1.0)

**Last Updated:** 2025-06-20  
**Primary Maintainer:** Ayo Odukale  
**AI Collaborator(s):** Claude 3 Opus, GPT-4o via Cursor IDE

---

## 🧠 Project Overview

This project is part of Inherra — a real estate signal engine for surfacing probate and transition-state properties. The scraping and enrichment pipeline performs stepwise data extraction, linkage, and scoring across multiple county sources in Harris County, TX.

This README documents the full workflow, including:

- Python-based scraping logic
- Snowflake ingestion and dbt transformations
- Output destinations (e.g. Notion)
- Claude + Cursor integration goals

---

## 🧩 Pipeline Flow (Step-by-Step)

> This is the execution order of each core module. Each script/module is stored under:  
> `/Users/ayoodukale/Documents/Inherra/Python/Inherra scraper`

---

### 🔹 STEP 1: **Scrape Probate Records**

- **Goal:** Collect new probate leads from the Harris County Clerk website.
- **Output:** CSV of probate records (decedent name, filing date, case number, etc.)
- **Known Filename:** `probate_leads_YYYYMMDD.csv`
- **Script:** `harris_probate_scraper.py`  
**Full Process:** [See `probate_scraper.md`](./docs/probate_scraper.md)
- **Status:** ✅ Implemented

---

### 🔹 STEP 2: **Scrape Harris RP (Real Property) Records**

- **Goal:** For each probate lead, identify possible property records associated with the decedent via the Real Property portal.
- **Technique:** Tiered search using last name, first name part, initials.
- **Output:** Flattened CSV of real property matches (`rp_...` columns).
- **Script:** `harris_rp_scraper.py`
- **Full Process:** [See `harris_rp_(real_property)_scrape.md`](./docs/harris_rp_%28real_property%29_scrape.md)
- **Status:** ✅ Implemented

---

### 🔹 STEP 3: **Preliminary Scoring Engine**

- **Goal:** Link RP records to probate records with preliminary confidence scores.
- **Technique:** Fuzzy matching on names and filing dates.
- **Output:** `probate_rp_scored_prelim.csv` with signal strength and match tier.
- **Script:** `probate_rp_scoring_engine_v1.py`
- **Full Process:** [See `probate_rp_prelim_scoring.md`](./docs/probate_rp_prelim_scoring.md)
- **Status:** ✅ Implemented

---

### 🔹 STEP 4: **Enrich with HCAD (Appraisal District Data)**

- **Goal:** Pull property details (square footage, lot size, owner name, mailing address) for each matched property using HCAD scraping.
- **Script:** `hcad_enrichment.py`
- **Output:** `probate_rp_hcad_enriched.csv`
- **Process Summary:** [See `hcad_enrichment.md`](./docs/hcad_enrichment.md)
- **Field Output Understanding:** [See `hcad_enriched_property_data.md`](./docs/hcad_enriched_property_data.md)
- **Status:** ✅ Implemented

---

### 🔹 STEP 5: **Enrich with HCTAX (Tax Records)**

- **Goal:** Pull tax payment history, ownership records, mailing addresses.
- **Script:** `hctax_enrichment.py`
- **Output:** `probate_rp_hcad_hctax_enriched.csv`
- **Process Summary:** [See `hctax_enrichment.md`]
- **Status:** ✅ Implemented

---

### 🔹 STEP 6: **Upload to Snowflake**

- **Goal:** Store cleaned and enriched CSV output to Snowflake staging tables.
- **Script:** `snowflake_upload.py`
- **Folder:** `/scripts/`
- **Status:** ✅ Implemented

---

### 🔹 STEP 7: **Run dbt Transformations**

- **Goal:** Apply scoring logic, tag high-confidence leads, and prep Notion output.
- **Model File:** `int_r_score_features.sql` + `prd_probate_leads.sql`
- **Location:** `/dbt/models/`
- **Status:** ✅ In Progress (see Git repo)

---

### 🔹 STEP 8: **Push to Notion (Output Delivery)**

- **Goal:** Post final scored leads into a Notion database.
- **Script:** `notion_sync.py`
- **Auth:** Uses Notion API key stored in `.env`
- **Status:** ✅ In Progress

---

## 🔐 Local Setup

> See `.env.sample` for environment variable references

```bash
# Install Python dependencies
pip install pandas playwright beautifulsoup4 fuzzywuzzy snowflake-connector-python

# Install Playwright browser
playwright install




## ⚙️ Setup

# 1. Clone the entire pipeline repository
git clone https://github.com/Ayodukale/Inherra-pipeline.git

# 2. Navigate into the project
cd Inherra-pipeline

# 3. Create virtual environment (Best Practice)
python3 -m venv venv
source venv/bin/activate

# 4. Install all Python dependencies
pip install -r dbt/scripts/requirements.txt
pip install dbt-snowflake pandas playwright beautifulsoup4 fuzzywuzzy snowflake-connector-python

# 5. Create your .env file
cp .env.sample .env
# Then edit `.env` with Snowflake credentials


# 6. Test dbt connection (must be inside the dbt folder)
cd dbt
dbt debug


```

---

## 📥 Schema Automation

Instead of manually editing `models/staging/schema.yml`, we use the script:

```bash
cd scripts
python dbt/scripts/generate_schema_yml.py

This script will:
- Connect to Snowflake using the `.env` file
- Pull metadata from `REFERENCE__JSON_KEYS`
- Merge it into the schema.yml (preserving manual overrides)
- Apply `not_null` and custom tests based on metadata

---

🧠 MCP Integration (Model Connected Plugin)
-------------------------------------------

> Our dbt + Snowflake environment is now connected to Cursor via MCP for local AI assistance

### Claude Integration Setup

*   We run a FastAPI app (server.py) locally at http://localhost:8000
    
*   MCP config points to this URL via:
    

{
  "mcpServers": {
    "snowflake": {
      "url": "http://localhost:8000"
    }
  }
}


*   Claude (MAX plan) can be used in Cursor to:
    
    *   Read dbt model files
        
    *   Help debug failing tests
        
    *   Answer lineage and transformation questions in real-time
        



🔄 Model + Schema Lifecycle
---------------------------

Our project now uses a **metadata-driven** approach to define schema tests and documentation via:

*   A REFERENCE\_\_JSON\_KEYS table in Snowflake
    
*   A dbt model json\_keys\_discovered to detect new fields
    
*   A Python script scripts/generate\_schema\_yml.py that:
    
    *   Compares discovered vs official fields
        
    *   Updates models/staging/schema.yml safely
        
    *   Preserves manual descriptions and test overrides
        

🧰 CI/CD (GitHub Actions Setup)
-------------------------------

We are working toward automating this pipeline:

*   Auto-run scraper
    
*   Upload to Snowflake
    
*   Run metadata sync + dbt transformations
    
*   Validate with tests
    
*   Publish docs via GitHub Pages or dbt Cloud
    

See [ci.yml](.github/workflows/ci.yml) for structure once implemented.

### 🛠 Key Maintenance Scripts

| Script                   | Purpose                                                     |
| ------------------------ | ----------------------------------------------------------- |
| `generate_schema_yml.py` | Auto-generates/upgrades `schema.yml` using curated metadata |
| `server.py`              | FastAPI app for MCP/Claude integration in Cursor            |
| `snowflake_upload.py`    | Loads enriched CSVs into staging Snowflake tables           |
| `notion_sync.py`         | Pushes final leads into Notion CRM                          |


### 🧪 Testing Commands

# Build and test your cleaned model
dbt build -m stg_probate_filings_cleaned

# (Re)generate docs for new schema/test coverage
dbt docs generate
dbt docs serve


### 🗺 Model Lineage

*   RAW\_RECORD → stg\_probate\_filings\_cleaned
    
*   → int\_r\_score\_features (adds features like ownership\_match, vacant, etc.)
    
*   → prd\_probate\_leads (final output)
    
*   → Notion sync (external delivery)
    

### 📍Next Up (Post-Onboarding)

*   Finalize int\_r\_score\_features logic
    
*   Add ready\_to\_push = TRUE flags
    
*   Integrate into GitHub CI/CD
    
*   Begin renovation leads tagging module
    








----
## 🔄 Ongoing Maintenance

See [MAINTENANCE_GUIDE.md](MAINTENANCE_GUIDE.md) for how to:
- Detect new fields
- Update the reference table
- Regenerate schema
- Maintain dbt test coverage