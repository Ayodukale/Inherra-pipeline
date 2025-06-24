# Inherra Pipeline – Roadmap

**Last Updated:** 2025-06-21  
**Owner:** Ayo Odukale  
**Workspace Path:** `/Users/ayoodukale/Documents/Inherra/Python/Inherra scraper/`

---

## ✅ Completed (v1.0)

- [x] Harris Probate and RP Scrapers (Playwright)
- [x] Prelim scoring engine using R-Code rules
- [x] HCAD and HCTAX enrichment integrated
- [x] CSV upload to Snowflake staging
- [x] dbt models transforming into R-Code features
- [x] Manual sync to Notion dashboard
- [x] Core docs + ReadMe PDFs generated

---

## 🔄 In Progress (v1.1)

- [ ] Finalize and deploy scoring engine v2 (with updated logic + weights)
- [ ] Restructure dbt for staging → intermediate → marts flow
- [ ] Begin ML scoring prototype in dbt Python model
- [ ] Add Notion contact sync logic
- [ ] Normalize address fields and enforce canonical forms
- [ ] dbt health score model for input data quality
- [ ] dbt conversion rate model for output quality
- [ ] Enable auto warehouse suspend in Snowflake
- [ ] Implement `dbt-expectations` tests and freshness checks
- [ ] Move all job orchestration to Pipedream

---

## 🔭 Future Ideas (v2.0+ Evolution)

### 🔁 Predictive Scoring via dbt + ML
- Convert R-Codes into feature table (already in progress)
- Build training set using historical deal outcomes
- Train logistic/XGBoost classifier using dbt Python model
- Save model artifact to Snowflake stage (`joblib`)
- Use 2nd dbt Python model for real-time prediction on new leads
- Replace `match_score_total` with ML probability score

### 🧠 Feedback-Driven System Learning
- Log user feedback (e.g. “Was this a valid owner match?”)
- Incorporate engagement signals (viewed, exported, contacted)
- Use labeled outcome data to update model
- Deming loop architecture: patch ➝ system improvement ➝ patch obsolete

### 📊 Dashboards + Monitoring
- Data health dashboards (missing fields, schema drift, bad XPaths)
- Performance dashboards (conversion rate, false positive rate, time-to-score)
- Quality heatmaps across zip codes or property types

### 🧬 Entity Resolution v2 (Graph-Based)
- Explore Neo4j graph between:
  - Decedent ↔ Executor
  - Executor ↔ Property
  - Owner ↔ Mailing Address
- Use graph centrality and relationship depth to enhance scoring

### 🌎 Geographic Expansion
- Add counties: Travis, Bexar, Dallas
- Modularize scraper scripts by county
- Add metadata-driven XPath templates

### 📤 Delivery + Automation
- Fully automatic pipeline: scrape → enrich → score → push → alert
- CLI tool: `inferra run --county harris --dry-run`
- Auto-suspend Snowflake warehouse after batch
- Auto-archive cold leads after 30 days of no engagement

---

### 📊 Data-Health & Governance Add-ons
- **Model-artifact versioning:** every trained model saved to `@dbt_models/lead_scorer/v{SEMVER}/lead_scorer.joblib` and release tagged in Git.
- **Weekly data-health trend:** create `rpt_data_health_trend.sql` to chart average `data_health_score` by week and surface in Notion.
- **Marketability score:** new feature combining owner-contact completeness + match confidence for lead prioritization.
- **Post-mortem ritual:** run `/docs/post_mortem_template.md` after any critical scrape or pipeline incident.

----


## 🧱 Best Practices – dbt + Snowflake (Current + Planned)

| Area                | Practice                                                                 |
|---------------------|--------------------------------------------------------------------------|
| Compute Efficiency | Use X-Small warehouse + suspend after 1 min inactivity                    |
| Data Volume        | All models incremental + snapshotting                                     |
| Tests              | Use `dbt-expectations` for schema, null, and range checks                 |
| Alerting           | Add freshness + schema drift monitors                                     |
| Modeling           | Maintain clear stg → int → marts flow, with versioned scoring tiers       |
| ML Integration     | Use dbt Python for training + inference models using R-Code features      |
| Git Discipline     | Version all model artifacts and scoring changes in Git                    |

---

