
# Known Issues & Risks – Inherra Pipeline

**Last Updated:** 2025-06-21  
**Owner:** Ayo Odukale  
**Think-Tank Reviewers:** Deming (Systems), Andrew Ng (AI/ML), DJ Patil (Data), Spencer Rascoff (PropTech)

---

## 1. Scraping Fragility

| Issue | Current Impact | Risk Level | Think-Tank Note |
|-------|---------------|-----------|-----------------|
| **XPath / CSS drift** on Harris RP & HCAD sites | Silent data gaps or zero-row outputs | 🔴 High | Deming: “All scrapers are temporary patches; build alerting to detect drift before downstream tasks run.” |
| **Playwright timeouts** on slow network | Partial CSVs, missing pages | 🟠 Medium | Add retry wrapper + exponential back-off. |
| **Captcha / session lockouts** | Scrape fails mid-batch | 🟠 Medium | Explore headless-browser rotation (Andrew Ng). |

---

## 2. Data Quality & Schema Drift

| Issue | Current Impact | Risk Level | Mitigation |
|-------|---------------|-----------|-----------|
| Extra / missing CSV columns after enrichment scripts | dbt models error on `select` | 🔴 High | `dbt test not_null + accepted_values` on critical columns; add `data_health_score`. |
| Mixed delimiters in legacy CSVs (`;` vs `,`) | Incorrect row splits | 🟠 Medium | Normalize delimiter on upload script; assert delimiter in header check. |
| Duplicate rows from RP scraper pagination | Inflated counts, false positives | 🟠 Medium | Add dedup macro keyed on `(rp_file_number, rp_party_full_name)`. |

---

## 3. Snowflake & dbt Operations

| Issue | Symptom | Risk | Mitigation |
|-------|---------|------|-----------|
| Warehouse left running >1 min | Unnecessary cost | 🟠 Medium | Auto-suspend at 60 s; schedule jobs 🕑 off-hours. |
| Non-incremental models reprocessing full history weekly | ↑ COMPUTE credits | 🟠 Medium | Ensure all heavy tables marked `materialized='incremental'`. |
| No freshness alerts | Stale data passes quietly | 🟡 Low | Add `dbt source freshness` check + Slack webhook. |

---

## 4. Scoring Logic & ML Governance

| Issue | Current Impact | Risk | Think-Tank Note |
|-------|---------------|------|-----------------|
| **Rule overlap / double-counting** in R-Codes | Inflation of `match_score_total` | 🔴 High | Spencer R.: “Refactor overlapping rules into single CASE.” |
| Patch rules (e.g., R5.5 common surname penalty) hide core entity-match weakness | Masked false positives | 🔴 High | Deming loop: improve name-matching → retire patch. |
| ML model drift once predictive scoring goes live | Degrading precision | 🟠 Medium | Andrew Ng: implement scheduled retrain on rolling 6-month window. |
| Lack of labeled data for training | Model overfits | 🟠 Medium | DJ Patil: collect user validation feedback fields (`owner_match_correct?`). |

---

## 5. Output Delivery & User Experience

| Issue | Symptom | Risk | Mitigation |
|-------|---------|------|-----------|
| Notion API rate limits | Missed lead pushes | 🟡 Low | Batch writes; exponential back-off. |
| Missing contact enrichment → users can’t act | Lower perceived value | 🟠 Medium | Integrate skip-trace API; mark `contact_confidence` flag. |

---

## 6. Security & Access

| Issue | Risk | Mitigation |
|-------|------|-----------|
| `.env` secrets committed accidentally | Snowflake / Notion keys exposed | Pre-commit hook to block `.env`; enable Snowflake network policies. |
| No row-level security for future multi-tenant product | Data leak between clients | Design `account_id` column + Snowflake RLS policy early. |

---

## 7. Documentation & Bus Factor

| Issue | Risk | Mitigation |
|-------|------|-----------|
| Critical logic in dev notebooks only | Knowledge silo | Migrate to `/docs/` and version in Git. |
| Infrequent update of docs after code change | Drift between docs & code | Add “docs updated?” checklist to PR template. |

---

## 📌 Immediate Action Items

1. Implement **XPath drift alert** (Playwright → Slack) before next scrape.
2. Convert heavy dbt models to **incremental** + snapshot strategy.
3. Add **`dbt-expectations` tests** for all staging columns.
4. Create **feedback capture fields** in Notion for owner-match validation.
5. Install **pre-commit Git hook** to block `.env` or credential files.

---

