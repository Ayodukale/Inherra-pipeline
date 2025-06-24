# Harris RP (Real Property) Scrape

**Version:** 12.1
**Date:** 2025-05-26
**Primary Developer(s):** Ayo Odukale
**AI Collaborator(s):** The Masterclass Room (facilitated by Claude-3 Opus)

## 1. Project Overview

This script (Script 1 of Project Phoenix) is designed to identify real property records potentially associated with deceased individuals. It takes a list of probate leads as input, searches the Harris County Clerk Real Property online portal, extracts detailed information about property transactions, and outputs a structured, flattened CSV file. This output serves as the primary input for Script 2, which performs advanced record linkage and relevance scoring.

The core challenge addressed by this script is the automated and robust extraction of data from a web portal that presents information in multiple, sometimes inconsistent, formats, particularly concerning legal descriptions and party details.

## 2. Core Functionality

*   **Input:** Reads probate leads from a specified CSV file (default: `harris_sample.csv`). Expects columns like `decedent_last`, `decedent_first`, `filing_date`, `case_type_desc`, etc.
*   **Tiered Web Scraping:**
    *   Navigates to the Harris County Clerk Real Property portal.
    *   Employs a multi-tier search strategy for each decedent to maximize relevant hits while managing search scope:
        *   **Name Standardization:** Cleans input names (uppercase, removes suffixes, standardizes first name part for searching).
        *   **Tier 1:** Searches `LAST_NAME STANDARDIZED_FIRST_NAME_PART` (e.g., "SMITH JOHN").
        *   **Tier 2:** Searches `LAST_NAME FIRST_INITIAL` (e.g., "SMITH J").
        *   **Tier 3:** Searches `LAST_NAME` only (configurable, typically for less common surnames).
        *   **Grantor-Focused:** Searches primarily target the decedent as a "Grantor."
        *   **Date Range:** Searches within a configurable window (+/- 1 year by default) around the probate filing date.
        *   **Pagination:** Handles multiple pages of search results (configurable maximum pages per tier).
*   **Data Extraction:**
    *   **Real Property (RP) Document Info:** File Number, Document Filing Date, Instrument Type.
    *   **Party Information:** Parses Grantors, Grantees, and Trustees. Prioritizes structured sub-row data if available, with a robust fallback to parse concatenated party strings from the main "Names" column of a record.
    *   **Legal Description:** Uses a hybrid approach:
        1.  Attempts to parse structured HTML sub-tables.
        2.  If HTML table parsing fails or is incomplete, scans multiple subsequent table cells for plain-text legal descriptions.
        3.  Extracts detailed fields (Desc, Lot, Block, Sec, Subdivision, Abstract, Survey, Tract) using refined regular expressions with lookaheads.
        4.  Includes post-processing to clean common suffixes (e.g., "Related Docs").
*   **Output:**
    *   Generates a CSV file (e.g., `harris_rp_targeted_matches_YYYYMMDD_HHMMSS.csv`) in the `data/targeted_results/` directory.
    *   **Flattened Structure:** Each row represents a single party's involvement in a single property transaction. Common property details are repeated.
    *   **Contextual Enrichment:** Output rows include key data from the input probate lead and metadata about the search process (e.g., search tier, search term used).
    *   **Column Naming Convention:** Uses `probate_lead_...` for fields from the input CSV and `rp_...` for fields scraped from the Real Property portal for clarity.
*   **Resilience & Debugging:**
    *   Configurable retries for search operations.
    *   Form state resets between search tiers.
    *   Extensive timestamped console logging.
    *   Screenshots on error.
    *   HTML page dumps for debugging specific search results.

## 3. Setup and Installation

### Prerequisites:
*   Python 3.7+
*   Playwright library and its browser drivers.

### Installation:
1.  **Clone the repository (if applicable) or download the script.**
2.  **Install Python dependencies:**
    ```bash
    pip install pandas playwright beautifulsoup4
    ```
3.  **Install Playwright browser drivers** (if running for the first time):
    ```bash
    playwright install
    # or playwright install chromium
    ```

## 4. Configuration

Key configurations are at the top of the script (`.py` file):

*   **`INPUT_PROBATE_LEADS_CSV`**: Path to the input CSV file containing probate leads.
    *   **Required columns (example):** `decedent_last`, `decedent_first`, `filing_date` (formats like MM/DD/YYYY, YYYY-MM-DD accepted), `case_type_desc`, `county`, `case_number`, `subtype`, `status`, `signal_strength` (these latter ones are used for enriching output).
*   **`OUTPUT_DIR`**: Directory where output CSVs and debug files will be saved.
*   **`TIER_SETTINGS`**: Dictionary to control the tiered search:
    *   `enable_tier_3`: Boolean, to enable/disable "Last Name Only" searches.
    *   `max_pages_per_tier`: Integer, max number of result pages to scrape per search tier.
    *   `common_surnames`: Set of strings, surnames for which Tier 3 search will be skipped.
*   **Various Timeout Constants:** (e.g., `DEFAULT_ELEMENT_TIMEOUT`, `PAGE_LOAD_TIMEOUT_INITIAL`) can be adjusted if needed for different network conditions.
*   **`MAX_ROWS_TO_DEBUG_HTML`**: Controls how many initial records per page get detailed row structure logging.
*   **`STOP_AFTER_FIRST_SUCCESSFUL_LEAD`**: Boolean (in `run_targeted_rp_scrape`), useful for testing. Set to `False` for full runs.

## 5. Usage

Run the script from the command line:


