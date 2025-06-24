# MCP Script Automation Guide

This document explains the MCP (Master Comparison Pipeline) logic used to validate and compare lead records for the Inherra project.

---

## 🎯 Purpose

Track whether each scraped property lead:
- Has changed ownership
- Has been updated in key fields
- Is ready to be pushed to Notion or CRM tools

---

## 🔧 Core Scripts

### 1. **Change Detection Logic**
Compares scraped data with existing DB entries using:
- `RP File ID`
- `HCAD Owner Name`
- `Mailing Address`, `Legal Description`, etc.

If any key fields differ → `needs_update = TRUE`

---

### 2. **Push Eligibility Flag**
In `acquisition_ready_leads` model:

- Set `ready_to_push = TRUE` when:
  - `r_score_acquisition >= 7`
  - `NOT pushed_to_notion`
  - `NOT stale` (based on last update timestamp)

---

## 🧪 Best Practice
- Use `ROW_HASH()` or a surrogate hash field to simplify change detection
- Validate at each pipeline stage: RP scrape → HCAD join → enriched → scored

---

## 🔁 Recurring Maintenance
- Regularly update scoring thresholds and push logic
- Log mismatches and `needs_update` flags for alerting

---

## 📈 Future Goals
- Auto-trigger push to Notion via Pipedream or dbt + webhook
- Sync updated metadata back into Snowflake