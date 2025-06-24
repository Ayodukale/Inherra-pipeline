
# Inherra – Architecture Overview

**Last Updated:** 2025-06-21  
**Owner:** Ayo Odukale  
**Workspace Path:** `/Users/ayoodukale/Documents/Inherra/Python/Inherra scraper/`

---

## 🧠 System Concept

**Inherra** is a precision lead intelligence engine for real estate investors, targeting transition-state properties (probate, inheritance, distress, etc.). It combines:

- Government-sourced data (probate, property, tax records)
- AI-assisted data enrichment (e.g., contact validation, flagging inconsistencies)
- A rules-based + ML hybrid scoring engine
- Automated delivery into CRM/Notion dashboards
- A scalable data pipeline using dbt + Snowflake

---

## 🔄 Data Flow Overview

```

```
      ┌──────────────┐
      │   Probate    │
      │   Scraper    │
      └──────┬───────┘
             │
      ┌──────▼───────┐
      │   RP/HCAD    │
      │   Scraper    │
      └──────┬───────┘
             ▼
      ┌──────────────┐
      │ Python Match │  ← Address parsing + decedent linking
      │ Logic + R-Codes│
      └──────┬───────┘
             ▼
  ┌────────────────────┐
  │  Snowflake Staging │
  └──────┬─────────────┘
         ▼
   ┌────────────┐
   │   dbt SQL  │ ← R-Code features (rules-based)
   └────┬───────┘
        ▼
┌────────────────────┐
│ dbt Python Training│ ← Logistic regression or XGBoost
└────┬───────────────┘
     ▼
```

┌───────────────────────┐
│ dbt Python Inference  │ ← ML scoring on new leads
└────┬──────────────────┘
     ▼
┌───────────────┐   ┌───────────────┐
│ Notion Export │ ← │ Pipedream API │
└───────────────┘   └───────────────┘

```

---

## 🧱 Core Stack Components

| Layer              | Tooling / Format                   | Purpose                                           |
|-------------------|------------------------------------|---------------------------------------------------|
| Data Collection    | Python + Playwright                | Scrape probate, RP, HCAD, HCTAX records            |
| Matching Logic     | Custom Python + Regex              | Link decedent ↔ property with confidence scoring   |
| Staging            | Snowflake External Table           | Upload enriched data via CSV or script            |
| Transformations    | dbt (SQL models)                   | Clean + compute features + create scoring layers   |
| ML Models          | dbt Python models + scikit-learn   | Train + deploy predictive scorers                 |
| Scheduling         | Pipedream                          | Trigger weekly pipeline with warehouse wake       |
| Version Control    | Git + GitHub                       | Code + scoring logic versioning                   |
| Output Delivery    | Notion API or CRM Connector        | Push scored leads to workspace                    |

---

## 🧠 Scoring System Architecture

### v1 – Rules-Based (R-Codes)
- Manual point-based system (e.g. `R1 = 50 pts`, `R10 = 25 pts`)
- Stored in dbt model as `match_score_total`
- Transparent, auditable, good for bootstrapping

### v2 – Predictive Scoring (ML-Driven)
- Leverages dbt Python models to:
  - Train a model (`train_lead_scorer.py`) on past deal data
  - Score new records (`predict_lead_scores.py`) via inference
- Final score becomes `ml_probability_score` (0–1)
- Stored alongside or replaces R-Code total in output

---

## 🔍 Monitoring & Feedback Loops

| Metric                     | Source                                  | Purpose                                     |
|----------------------------|-----------------------------------------|---------------------------------------------|
| Data Health Score          | dbt model `int_data_health_scores.sql`  | Detect schema drift / bad records           |
| System Performance         | `rpt_system_performance.sql`            | Conversion rates from scored leads          |
| User Validation            | Notion fields (validated? accurate?)    | Label training set for ML refinement        |
| Pipeline Status            | Pipedream logs + Slack webhook          | Confirm weekly scrape + transform           |
| Weekly Data-Health Trend   | `rpt_data_health_trend.sql`             | Track average `data_health_score` over time |
| Model Artifact Registry    | `@dbt_models/lead_scorer/v{SEMVER}`     | Auditable model lineage & rollback          |

---

## 🔭 Experimental Modules

| Module              | Status     | Notes                                                                 |
|---------------------|------------|-----------------------------------------------------------------------|
| Graph Linking (Neo4j) | Planned    | Decedent → Executor → Parcel networks for better entity resolution    |
| AI Entity Matching   | Planned    | ML/NLP to refine fuzzy name/address resolution                        |
| Trigger When Rules   | Partial    | Used to notify on match conditions (e.g., Owner State ≠ Property State) |
| Auto Contact Enrich  | Planned    | API enrichment of owner details, skip tracing                         |
| Lead Validation UI   | Planned    | Web form or Notion template for user feedback                         |

---

## 🗂 Suggested Directory Structure (dbt)

```

models/
├── staging/
│   ├── stg\_probate.sql
│   └── stg\_rp.sql
├── intermediate/
│   ├── int\_r\_score\_features.sql
│   ├── int\_data\_health\_scores.sql
│   └── int\_training\_dataset.sql
├── marts/
│   ├── final\_lead\_scores.sql
│   └── rpt\_system\_performance.sql
├── ml/
│   ├── train\_lead\_scorer.py
│   └── predict\_lead\_scores.py

```

---
```
