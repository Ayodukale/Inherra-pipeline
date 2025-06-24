---

# README: Enriched Property Data

### Purpose

This CSV file contains data from Real Property (RP) records that have been linked to Probate court cases. This linked data is then used to search for and scrape detailed property information from the Harris County Appraisal District (HCAD) website. The columns are ordered to tell a "left-to-right story," allowing a user to quickly assess the result of the automated process and then dive deeper into the details.

## 

## Column Descriptions

The columns are grouped into four conceptual "panes" for easier analysis in a spreadsheet.

---

### **Pane A: Quick-Glance Summary (Key Identifiers & Match Overview)**

*(These columns are intended to be frozen for constant visibility)*

1. **`probate_lead_case_number`**: (Text) The unique identifier for the probate case. *Origin: Input Probate Data.*
2. **`rp_file_number`**: (Text) The unique identifier for the Real Property document linked to the probate case. *Origin: Input RP Data.*
3. **`hcad_account`**: (Text) The final HCAD account number for the property found and matched by this script. *Origin: HCAD Scrape.*
4. **`hcad_search_status`**: (Text) The final status of the script's search attempt for this record.
    - `SUCCESS`: A matching HCAD property was found and its details were successfully parsed.
    - `NO_HCAD_MATCH_FOUND_ALL_TIERS`: All search strategies were attempted, but no definitive property was found.
    - `PAGINATION_TOO_LARGE`: A search returned too many results to process. The first page of results may be in `hcad_first_page_summary_data`.
    - `MULTIPLE_HITS_NO_WINNER_TIER_{tier_name}`: Multiple potential matches were found, but the script's logic could not confidently select the single best one.
    - `DETAIL_PARSE_FAILED` or `ERROR_*`: The script encountered a technical error during scraping. The `parsing_error` column will have more information.
5. **`hcad_final_tier_hit`**: (Text) The name of the search strategy (`tier`) that resulted in the final status (e.g., `T0_ExactLotBlockSubdivision`). *Origin: Script Logic.*
6. **`hcad_owner_match_type`**: (Text) A critical field describing the relationship between the owner found on the HCAD record and the people involved in the original documents.
    - `MATCH_PROBATE_DECEDENT_AS_RP_PARTY`: Strongest match. The HCAD owner matches the Probate Decedent, who was also the main party (e.g., Grantor) on the property record.
    - `MATCH_RP_GRANTEE`: The HCAD owner matches a person who was a grantee on the property record.
    - `HCAD_OWNER_IS_UNRELATED_THIRD_PARTY`: The HCAD owner does not appear to match anyone from the original documents.
    - `HCAD_OWNER_NAME_MISSING`: A property record was found, but the owner name was blank on the HCAD website.
7. **`needs_review_flag`**: (1 or 0) A flag to signal that a row requires manual human review. `1` means "review recommended." *Origin: Script Logic.*
8. **`review_reason`**: (Text) The reason why the `needs_review_flag` was set. *Origin: Script Logic.*
9. **`hcad_owner_full_name`**: (Text) The full owner name as listed on the matched HCAD property record. *Origin: HCAD Scrape.*
10. **`probate_lead_decedent_last`**: (Text) Last name of the decedent from the probate case. *Origin: Input Probate Data.*
11. **`probate_lead_decedent_first`**: (Text) First name of the decedent from the probate case. *Origin: Input Probate Data.*
12. **`rp_party_full_name`**: (Text) Full name of the primary party (e.g., Grantor) on the linked property document. *Origin: Calculated from Input RP Data.*

---

### **Pane B: Property Snapshot (Location, Legal & Key Structure Details)**

1. **`hcad_site_address`**: (Text) The physical site address of the matched HCAD property. *Origin: HCAD Scrape.*
2. **`rp_legal_description_text`**: (Text) The primary legal description (often the subdivision name) from the original property record. *Origin: Input RP Data.*
3. **`hcad_base_area_total_sqft`**: (Numeric) The sum of all "Base Area" components from the Building Area table, representing the primary interior living space. *Origin: HCAD Scrape.*
4. **`hcad_impr_total_sqft`**: (Numeric) The total square footage of all primary building structures listed in the main building table. *Origin: HCAD Scrape.*
5. **`hcad_total_structure_sqft`**: (Numeric) The sum of all building area components, including base area, garages, porches, etc. *Origin: HCAD Scrape.*
6. **`hcad_garage_sqft`**: (Numeric) The sum of all "Garage" components from the Building Area table. *Origin: HCAD Scrape.*
7. **`hcad_bedrooms`**: (Numeric) The number of bedrooms listed in the Building Characteristics. *Origin: HCAD Scrape (Dynamically Parsed).*
8. **`hcad_full_bathrooms`**: (Numeric) The number of full bathrooms listed. *Origin: HCAD Scrape (Dynamically Parsed).*

---

### **Pane C: Financials & Name Matching Scores**

1. **`hcad_market_value_detail`**: (Numeric) The **Total Market Value** for the most recent year, from the HCAD detail page. *Origin: HCAD Scrape.*
2. **`hcad_appraised_value_detail`**: (Numeric) The **Appraised Value** for the most recent year, taken from the 5-Year Value History page. *Origin: HCAD Scrape.*
3. **`hcad_improvement_market_value`**: (Numeric) The market value assigned to the improvements (structures) on the property. *Origin: HCAD Scrape.*
4. **`hcad_land_market_value`**: (Numeric) The market value assigned to the land itself. *Origin: HCAD Scrape.*
5. **`hcad_appraised_history_json`**: (JSON Text) A JSON object containing the appraised value for the last 5 years, with the year as the key. *Origin: HCAD Scrape.*
6. **`score_hcad_vs_probate`**: (0-100) The name similarity score between the HCAD Owner and the Probate Decedent. *Origin: Script Logic.*
7. **`score_hcad_vs_rp_party`**: (0-100) The name similarity score between the HCAD Owner and the main Real Property document party. *Origin: Script Logic.*

---

### **Pane D: Full Technical Details & Provenance**

*(This section contains all original columns from the input files for full traceability, as well as detailed JSON blobs for deep analysis.)*

1. **`is_potential_decedent_match`**: (Boolean) Flag from the input file indicating if the RP party was considered a potential match to the decedent. *Origin: Input Data.*
2. **`hcad_mailing_address`**: (Text) The mailing address for the owner on the HCAD record. *Origin: HCAD Scrape.*
3. **`hcad_main_building_data_json`**: (JSON Text) A detailed list of all primary buildings, including their `type`, `style`, `year_built`, `quality`, and `sq_ft`. *Origin: HCAD Scrape.*
4. **`hcad_building_data_json`**: (JSON Text) A detailed list of all building area components (e.g., base area, porch) and their individual `area`. *Origin: HCAD Scrape.*
5. **`hcad_land_data_json`**: (JSON Text) A detailed list of all land type components and their individual `units` and `market_value`. *Origin: HCAD Scrape.*
6. **`hcad_other_characteristics_json`**: (JSON Text) An "everything else" container for any building characteristics found on the page that are not one of the standard, expected columns. This prevents data loss without breaking the main table schema. *Origin: HCAD Scrape (Dynamically Parsed).*
7. **`hcad_foundation_type`**: (Text) The building's foundation type. *Origin: HCAD Scrape (Dynamically Parsed).*
8. **`hcad_exterior_wall`**: (Text) The building's exterior wall material. *Origin: HCAD Scrape (Dynamically Parsed).*
9. **`hcad_heating_ac`**: (Text) The building's HVAC system type. *Origin: HCAD Scrape (Dynamically Parsed).*
10. **`hcad_detail_url_visited`**: (URL) The direct link to the HCAD property page that was scraped. *Origin: HCAD Scrape.*
11. **`parsing_error`**: (Text) Contains the error message if the script failed to parse an HCAD detail page. *Origin: Script Logic.*
...and all other original columns from the input probate and real property files for complete traceability.

---

## Suggested Analyst Workflow

1. **Filter for Review:** Start by filtering `needs_review_flag` to show only rows with a value of `1`. These are the records the script has identified as needing a human eye.
2. **Triage by Status:** Within the records needing review, use `hcad_search_status` and `review_reason` to triage. A `MULTIPLE_HITS_NO_WINNER` may be a higher priority than `NO_HCAD_MATCH_FOUND_ALL_TIERS`.
3. **Validate Name Matches:** For successful matches, quickly compare the key name columns in Pane A to sanity-check the script's `hcad_owner_match_type` conclusion.
4. **Dive into Details:** Use the rich data in Panes B, C, and D for deeper property analysis. For example, an analyst could filter for properties with a high `hcad_land_market_value` but a low `hcad_impr_total_sqft` to find potential teardowns, or analyze the `year_built` from within the `hcad_main_building_data_json` column.
5. **Discover New Data:** Periodically check the `hcad_other_characteristics_json` column to see if HCAD has started providing new, interesting data points that might be worth promoting to their own standard column in a future script update.