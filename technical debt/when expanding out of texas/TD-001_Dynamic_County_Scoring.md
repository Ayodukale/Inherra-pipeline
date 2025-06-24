# TD-001: Refactor Scoring Model for Dynamic, Multi-County Support

**Date Created:** 2025-06-21  
**Owner:** Ayo Odukale  
**Status:** Open  
**Affected File(s):** `models/intermediate/int_r_score_features.sql`  

---

## 1. Problem Statement

The `int_r_score_features.sql` model currently contains hardcoded logic specific to Harris County, Texas. Specifically, the `constants` CTE defines `home_state` as 'TX' and `nearby_states` as an array of states bordering Texas.

This implementation is not scalable. When we expand to new counties (e.g., Dallas, Travis) or new states (e.g., King County, WA), we will be forced to either duplicate the entire model or perform complex, risky find-and-replace operations within the SQL. This creates a significant maintenance burden and violates the DRY (Don't Repeat Yourself) principle.

**Risk:** Slows down geographic expansion, increases the chance of bugs, and makes the codebase difficult to manage as the business grows.

## 2. Proposed Solution

The hardcoded logic should be abstracted into a metadata-driven configuration table. This will decouple the scoring *logic* from the county-specific *rules*.

**Implementation Plan:**

1.  **Create a `dbt seed`:** Create a new CSV file at `seeds/county_metadata.csv` that contains configuration data for each county we operate in.
    *   **Schema:** `county_name`, `property_home_state`, `nearby_states` (comma-separated string).

2.  **Refactor the dbt Model:** Modify `int_r_score_features.sql` to:
    *   Accept a dbt variable (`run_county`) to determine which county's rules to apply during a `dbt run`.
    *   Replace the `constants` CTE with a new CTE that reads from the `ref('county_metadata')` seed file, filtering for the `run_county`.
    *   All subsequent logic should reference the values from this new configuration CTE, making the model fully dynamic.

## 3. Acceptance Criteria

The ticket will be considered "Done" when:

- [ ] A `seeds/county_metadata.csv` file exists and is populated for at least Harris County.
- [ ] The `int_r_score_features.sql` model no longer contains any hardcoded state or nearby-state values.
- [ ] The model can be run successfully for Harris County via the command: `dbt build --select int_r_score_features --vars 'run_county: Harris'`.
- [ ] The output of the new dynamic model is identical to the output of the previous hardcoded version when run for Harris County.

## 4. Context & Justification

This change aligns with our long-term vision for Inherra as a scalable, multi-market signal engine. By addressing this now (or before expansion), we are "building it right" and preventing future engineering bottlenecks. This is a foundational piece of technical architecture that will pay dividends in development speed and system reliability later.