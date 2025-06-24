# script4_hcad_enrichment.py

import os
import glob
import pandas as pd
from playwright.sync_api import sync_playwright
import time
import re
from urllib.parse import urljoin
from rapidfuzz import fuzz, process
import traceback
import datetime
import json

# from rapidfuzz import fuzz # For later stages (fuzzy matching)

# --- Configuration ---
HCAD_BASE_URL = "https://hcad.org"
HCAD_ADVANCED_SEARCH_URL = urljoin(HCAD_BASE_URL, "/property-search/real-property-advanced-records/")

# --- Robust Selectors (Updated based on your HTML snippets) ---
LEGAL_DESC_INPUT_SELECTOR = 'input[name="desc"]'
OWNER_NAME_INPUT_SELECTOR = 'input[name="name"]'
SEARCH_BUTTON_SELECTOR = 'input#Search'
RECORD_COUNT_TEXT_SELECTOR = 'p.justcenter'

# NEW and MUCH BETTER selector for result rows based on your provided table HTML:
RESULTS_TABLE_ROWS_SELECTOR = 'table.bgcolor_1 > tbody > tr[bgcolor="ffffff"]'

CHANGE_CRITERIA_BUTTON_SELECTOR = 'input[type="submit"][value="Change Criteria or Sorted Order"]'
DETAIL_PAGE_INDICATOR_TEXT_1 = "Owner & Mailing Address:"
DETAIL_PAGE_INDICATOR_TEXT_2 = "REAL PROPERTY ACCOUNT INFORMATION"
# A good structural indicator for detail page can be a table with a specific header:
DETAIL_PAGE_STRUCTURAL_INDICATOR = 'table th:has-text("Account Number")' # Checks for a <th> with "Account Number"
# --- Define COMMON_SURNAMES as a global constant or near the top of your script ---
COMMON_SURNAMES = {
    "SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES",
    "GARCIA", "MILLER", "DAVIS", "RODRIGUEZ", "MARTINEZ",
    "HERNANDEZ", "LOPEZ", "GONZALEZ", "WILSON", "ANDERSON",
    "THOMAS", "TAYLOR", "MOORE", "JACKSON", "MARTIN",
    "LEE", "PEREZ", "THOMPSON", "WHITE", "HARRIS"
} # Add more if needed

HCAD_RESULTS_PER_PAGE = 20 # Or whatever you observe HCAD's typical first page limit to be (e.g., 20, 25, 50)
HCAD_DETAIL_CACHE = {}
# Thresholds and limits for choose_best_from_multiple (Task 6)
SUMMARY_SCORE_ABSOLUTE_THRESHOLD = 60 # Min summary score for a candidate to be considered a 'good' pick
SUMMARY_SCORE_DIFFERENCE_THRESHOLD = 20 # Min difference between top 1 and top 2 summary scores for a clear pick
DETAIL_FETCH_LIMIT_AFTER_SUMMARY = 3    # Max items to fetch details for if summary round is inconclusive
# Min score for a detailed candidate to be chosen if it's the best of the detailed fetch batch
DETAILED_SCORE_MIN_ACCEPTABLE_THRESHOLD_HIGH_CONF = 70 
DETAILED_SCORE_MIN_ACCEPTABLE_THRESHOLD_LOW_CONF = 50
# Min difference between top 1 and 2 detailed scores (if more than 1 detailed item fetched)
DETAILED_SCORE_DIFFERENCE_THRESHOLD_HIGH_CONF = 15 
DETAILED_SCORE_DIFFERENCE_THRESHOLD_LOW_CONF = 10
# If a single detailed candidate scores above this, it's an auto-winner (after detail fetch)
AUTO_WINNER_DETAIL_SCORE_THRESHOLD = 90 

# --- Helper Functions ---





def _clean_numeric_value(value_str):
    """Helper to clean and convert currency/numeric strings."""
    if value_str is None:
        return None
    cleaned = str(value_str).strip().replace('$', '').replace(',', '')
    if cleaned.lower() == 'pending' or not cleaned:
        return None # Or 0, depending on how you want to treat "Pending"
    try:
        return float(cleaned) # Or int() if appropriate
    except ValueError:
        print(f"WARN: Could not convert '{value_str}' to number, returning None.")
        return None


def _extract_potential_last_name(full_name_str):
    """
    Attempts to extract a potential last name from a full name string.
    Considers common suffixes and takes the last significant token.
    """
    if not full_name_str or not isinstance(full_name_str, str):
        return ""
    
    name_parts = full_name_str.upper().split()
    common_suffixes_titles = {"JR", "SR", "II", "III", "IV", "V", "TRUST", "ESTATE", "EST", "LLC", "INC", "LP", "LTD", "CO", "BANK"}
    
    # Iterate from the end to find the first word not in suffixes
    for i in range(len(name_parts) - 1, -1, -1):
        if name_parts[i] not in common_suffixes_titles:
            # Check if the preceding word is also not a suffix (to handle multi-word last names better, though imperfectly)
            # This is a simple heuristic. True multi-word last name parsing is complex.
            # For now, just return this token.
            return name_parts[i]
    return name_parts[-1] if name_parts else "" # Fallback to last token if all are suffixes (unlikely) or empty


def parse_land_rows_xpath(page):
    """
    Scrapes the Land Data table using a highly specific selector to target only data rows.
    """
    land_rows = []
    total_lot_sqft = 0
    total_land_value = 0
    
    try:
        # This selector now specifically looks for rows that have at least 12 columns,
        # which will filter out most header and spacer rows automatically.
        land_table_rows_selector = '//table[.//th[contains(text(), "Land Use")]]//tr[count(td) >= 12]'
        data_rows = page.locator(land_table_rows_selector).all()
        
        if not data_rows:
             print("INFO: No data rows found in the land table matching the specific criteria.")
        
        for row in data_rows:
            # We can be more confident in these locators now, but the try/except is still good practice.
            try:
                land_use = row.locator('td:nth-child(1)').inner_text().strip()
                unit_type = row.locator('td:nth-child(3)').inner_text().strip()
                units_raw = row.locator('td:nth-child(4)').inner_text().strip()
                market_val_raw = row.locator('td:nth-child(12)').inner_text().strip()

                units = float(units_raw.replace(',', ''))
                market_value = float(market_val_raw.replace('$', '').replace(',', ''))

                land_rows.append({
                    "land_use": land_use,
                    "unit_type": unit_type,
                    "units": units,
                    "market_value": market_value
                })
                
                if unit_type == "SF":
                    total_lot_sqft += units
                
                total_land_value += market_value
            except Exception as e_row:
                print(f"WARN: Could not parse a specific land data row, skipping it. Error: {e_row}")
                continue

    except Exception as e:
        print(f"WARN: An unexpected error occurred in parse_land_rows_xpath. Error: {e}")

    return {
        "hcad_lot_sqft_total": total_lot_sqft,
        "hcad_land_market_value_total": total_land_value,
        "hcad_land_line_count": len(land_rows),
        "hcad_land_data_json": json.dumps(land_rows) if land_rows else None
    }


def parse_building_main_data(page):
    """
    Scrapes the main Building data table by dynamically finding the 'Imprv Sq Ft' column,
    and calculates the total improvement square footage.
    """
    building_list = []
    total_impr_sqft = 0
    print("INFO: Attempting to parse main building data...")
    
    try:
        building_table_selector = '//table[.//th[contains(text(), "Building #")]]'
        building_table = page.locator(building_table_selector)

        if building_table.count() > 0:
            headers = [th.inner_text().strip() for th in building_table.locator('//thead/tr/th').all()]
            
            # Dynamically find the column index for "Imprv Sq Ft"
            sq_ft_col_index = -1
            for i, header in enumerate(headers):
                if "Imprv Sq Ft" in header:
                    sq_ft_col_index = i + 1 # Add 1 because nth-child is 1-based
                    break
            
            if sq_ft_col_index == -1:
                print("WARN: Could not find 'Imprv Sq Ft' column in main building table.")
                return {"hcad_impr_total_sqft": 0, "hcad_main_building_data_json": None}

            data_rows = building_table.locator('//tbody/tr[./td]').all()
            print(f"DEBUG: Found main building table and {len(data_rows)} data row(s).")

            for row in data_rows:
                sq_ft_raw = row.locator(f'td:nth-child({sq_ft_col_index})').inner_text().strip()
                sq_ft = _clean_numeric_value(sq_ft_raw)
                
                if sq_ft:
                    total_impr_sqft += sq_ft

                building_list.append({
                    "year_built": row.locator('td:nth-child(2)').inner_text().strip(),
                    "type": row.locator('td:nth-child(3)').inner_text().strip(),
                    "style": row.locator('td:nth-child(4)').inner_text().strip(),
                    "quality": row.locator('td:nth-child(5)').inner_text().strip(),
                    "sq_ft": sq_ft
                })
        else:
            print("INFO: Main building data table not found on this page.")

    except Exception as e:
        print(f"WARN: An unexpected error occurred in parse_building_main_data. Error: {e}")

    return {
        "hcad_impr_total_sqft": total_impr_sqft, 
        "hcad_main_building_data_json": json.dumps(building_list) if building_list else None
    }


def parse_building_characteristics(page):
    """
    Dynamically scrapes the Building Characteristics table, standardizes known variations,
    and places any unknown characteristics into a separate JSON field to maintain a stable schema.
    """
    known_labels = {
        "foundation_type", "exterior_wall", "roof_type", "heating_ac",
        "grade_adjustment", "physical_condition", "full_bathrooms",
        "half_bathrooms", "bedrooms", "stories", "carport"
    }
    
    standard_characteristics = {}
    extra_characteristics = {}

    print("INFO: Attempting to parse building characteristics...")
    try:
        char_table_selector = 'xpath=/html/body/table/tbody/tr/td/table[17]/tbody/tr/td[2]/table'
        char_table = page.locator(char_table_selector)

        if char_table.count() > 0:
            rows = char_table.locator('tr').all()
            for row in rows:
                cells = row.locator('td').all()
                if len(cells) == 2:
                    label_raw = cells[0].inner_text().strip()
                    value = cells[1].inner_text().strip()
                    
                    if label_raw:
                        cleaned_label = re.sub(r'[\s/-]+', '_', label_raw.lower().replace(':', '').replace('(', '').replace(')', '')).strip('_')
                        
                        # --- NEW: More forgiving if/elif logic to standardize labels ---
                        final_label = cleaned_label # Default to the cleaned label
                        if "room_bedroom" in cleaned_label:
                            final_label = "bedrooms"
                        elif "room_full_bath" in cleaned_label:
                            final_label = "full_bathrooms"
                        elif "room_half_bath" in cleaned_label:
                            final_label = "half_bathrooms"
                        elif "heating_ac" in cleaned_label:
                            final_label = "heating_ac"
                        elif "stories_story_height" in cleaned_label:
                            final_label = "stories"
                        
                        # --- End of new logic ---
                        
                        if final_label in known_labels:
                            standard_characteristics[f"hcad_{final_label}"] = value
                        else:
                            extra_characteristics[final_label] = value
            
            print(f"SUCCESS: Parsed characteristics. Found {len(standard_characteristics)} known and {len(extra_characteristics)} other properties.")
        else:
            print("INFO: Building characteristics table not found on this page.")
            
    except Exception as e:
        print(f"WARN: An unexpected error occurred in parse_building_characteristics. Error: {e}")
        
    standard_characteristics['hcad_other_characteristics_json'] = json.dumps(extra_characteristics) if extra_characteristics else None
    
    return standard_characteristics



def parse_building_area_data(page):
    """
    Scrapes the Building Area table, which has a variable number of rows.
    Calculates total base, structure, and garage square footage.
    """
    # This selector targets all rows within the specific "Building Area" table.
    # It's based on the XPaths you provided, made more general.
    building_area_rows_selector = 'xpath=/html/body/table/tbody/tr/td/table[17]/tbody/tr/td[4]/table/tbody/tr'
    
    building_rows_data = []
    total_base_sqft = 0
    total_structure_sqft = 0
    garage_sqft = 0
    
    try:
        building_area_table_rows = page.locator(building_area_rows_selector).all()
        
        # We start from the 3rd row to skip the header rows shown in your screenshot
        for row in building_area_table_rows[2:]:
            type_text = row.locator('td:nth-child(1)').inner_text().strip()
            area_text = row.locator('td:nth-child(2)').inner_text().strip()
            
            # Skip empty rows if any
            if not type_text or not area_text:
                continue
                
            area = int(area_text.replace(",", ""))

            building_rows_data.append({
                "type": type_text,
                "area": area
            })
            
            # Add to the total structure square footage
            total_structure_sqft += area
            
            # Check for specific types to sum them separately
            if "BASE AREA" in type_text:
                total_base_sqft += area
            
            if "GARAGE" in type_text:
                garage_sqft += area
                
    except Exception as e:
        print(f"WARN: Could not parse building area data. Error: {e}")

    return {
        "hcad_total_base_sqft": total_base_sqft,
        "hcad_total_structure_sqft": total_structure_sqft,
        "hcad_garage_sqft": garage_sqft,
        "hcad_building_area_count": len(building_rows_data),
        "hcad_building_data_json": json.dumps(building_rows_data) if building_rows_data else None
    }



def parse_hcad_detail_page(p_page, detail_url):
    print(f"INFO: Navigating to and parsing detail page: {detail_url}")
    
    # --- Define all keys, including every new field ---
    all_expected_keys = [
        'hcad_account', 'hcad_owner_full_name', 'hcad_mailing_address',
        'hcad_legal_desc_detail', 'hcad_site_address', 'hcad_pct_ownership',
        'hcad_market_value_detail', 'hcad_appraised_value_detail', 
        'hcad_land_area_sf', 'hcad_total_living_area_sf',
        'hcad_lot_sqft_total', 'hcad_land_market_value_total', 'hcad_land_line_count', 'hcad_land_data_json',
        'hcad_total_base_sqft', 'hcad_total_structure_sqft', 'hcad_garage_sqft',
        'hcad_building_area_count', 'hcad_building_data_json',
        'hcad_main_building_data_json', 'hcad_foundation_type', 'hcad_exterior_wall', 'hcad_heating_ac',
        'hcad_grade_adjustment', 'hcad_physical_condition', 'hcad_full_bathrooms', 'hcad_bedrooms',
        'hcad_land_market_value', 'hcad_improvement_market_value', 'hcad_appraised_history_json',
        'hcad_detail_url_visited', 'parsing_error'
    ]
    
    hcad_data = {"hcad_detail_url_visited": detail_url}
    for key in all_expected_keys:
        if key not in hcad_data: hcad_data[key] = None
        
    try:
        # --- Main Navigation ---
        if p_page.url != detail_url:
            p_page.goto(detail_url, timeout=60000, wait_until="networkidle")
        else:
            p_page.wait_for_load_state("networkidle", timeout=20000)
        
        print(f"DEBUG: On detail page: {p_page.url}")

        # --- Scrape Core Fields with Original Robust Logic ---
        # This is the logic that was missing from the previous version.
        # HCAD Account Number
        account_xpath = "/html/body/table/tbody/tr/td/table[1]/tbody/tr/td[2]/b"
        account_elem_b = p_page.locator(f"xpath={account_xpath}")
        if account_elem_b.count() > 0:
            hcad_data['hcad_account'] = ' '.join(account_elem_b.first.inner_text().split()).strip()

        # Owner Name & Mailing Address
        owner_mail_label_semantic_xpath = '//td[starts-with(normalize-space(.), "Owner Name & Mailing Address:")]/following-sibling::*[1]'
        owner_mail_user_xpath = "/html/body/table/tbody/tr/td/table[5]/tbody/tr[2]/td[1]/table/tbody/tr/th"
        owner_mail_value_cell = p_page.locator(f"xpath={owner_mail_label_semantic_xpath}")
        if not owner_mail_value_cell.count(): owner_mail_value_cell = p_page.locator(f"xpath={owner_mail_user_xpath}")
        if owner_mail_value_cell.count() > 0:
            lines = [line.strip() for line in owner_mail_value_cell.first.inner_text().split('\n') if line.strip()]
            if lines:
                hcad_data['hcad_owner_full_name'] = lines[0]
                hcad_data['hcad_mailing_address'] = " ".join(lines[1:]) if len(lines) > 1 else None

        # Legal Description
        legal_desc_label_semantic_xpath = '//td[normalize-space(.)="Legal Description:"]/following-sibling::*[1]'
        legal_desc_user_xpath = "/html/body/table/tbody/tr/td/table[5]/tbody/tr[2]/td[2]/table/tbody/tr[1]/th"
        legal_desc_value_cell = p_page.locator(f"xpath={legal_desc_label_semantic_xpath}")
        if not legal_desc_value_cell.count(): legal_desc_value_cell = p_page.locator(f"xpath={legal_desc_user_xpath}")
        if legal_desc_value_cell.count() > 0:
            hcad_data['hcad_legal_desc_detail'] = ' '.join(legal_desc_value_cell.first.inner_text().splitlines()).strip()

        # Property Address
        prop_addr_semantic_xpath = '//td[normalize-space(.)="Property Address:"]/following-sibling::*[1]'
        prop_addr_user_xpath = "/html/body/table/tbody/tr/td/table[5]/tbody/tr[2]/td[2]/table/tbody/tr[2]/th"
        prop_addr_value_cell = p_page.locator(f"xpath={prop_addr_semantic_xpath}")
        if not prop_addr_value_cell.count(): prop_addr_value_cell = p_page.locator(f"xpath={prop_addr_user_xpath}")
        if prop_addr_value_cell.count() > 0:
            hcad_data['hcad_site_address'] = ' '.join(prop_addr_value_cell.first.inner_text().splitlines()).strip()

        # --- Scrape Additional Static Fields ---
        def get_text(page_context, xpath, name):
            try:
                element = page_context.locator(f"xpath={xpath}").first
                if element.is_visible(timeout=1000):
                    return element.inner_text().strip()
            except Exception:
                print(f"WARN: Could not find '{name}' with XPath: {xpath}")
            return None
        
        hcad_data['hcad_land_market_value'] = _clean_numeric_value(get_text(p_page, "/html/body/table/tbody/tr/td/table[12]/tbody/tr[4]/td[5]", "Land Market Value"))
        hcad_data['hcad_improvement_market_value'] = _clean_numeric_value(get_text(p_page, "/html/body/table/tbody/tr/td/table[12]/tbody/tr[5]/td[5]", "Improvement Market Value"))
        

       # --- Call Dynamic Table Parsers ---
        land_data = parse_land_rows_xpath(p_page)
        hcad_data.update(land_data)
        print(f"INFO: Land data parsed. Lines found: {land_data.get('hcad_land_line_count', 0)}. Total SQFT: {land_data.get('hcad_lot_sqft_total', 0)}.")

        building_area_data = parse_building_area_data(p_page)
        hcad_data.update(building_area_data)
        print(f"INFO: Building Area data parsed. Lines found: {building_area_data.get('hcad_building_area_count', 0)}. Total Base SQFT: {building_area_data.get('hcad_total_base_sqft', 0)}.")
        
        main_building_data = parse_building_main_data(p_page)
        hcad_data.update(main_building_data)

        # --- NEW: Call the building characteristics parser ---
        characteristics_data = parse_building_characteristics(p_page)
        hcad_data.update(characteristics_data)
        print(f"INFO: Building characteristics parsed. Found {len(characteristics_data)} properties.")
        
        # --- CORRECTED: Safely check for None before processing JSON ---
        main_building_json = main_building_data.get('hcad_main_building_data_json')
        building_list_len = len(json.loads(main_building_json)) if main_building_json else 0
        print(f"INFO: Main Building data parsed. Buildings found: {building_list_len}.")

        # --- Navigate and Scrape 5-Year History (with New Tab Logic) ---
        history_link_xpath = "/html/body/table/tbody/tr/td/table[12]/tbody/tr[7]/td/a"
        history_link = p_page.locator(f"xpath={history_link_xpath}")
        if history_link.count() > 0:
            with p_page.context.expect_page() as new_page_info:
                history_link.click()
            
            history_page = new_page_info.value
            history_page.wait_for_load_state("networkidle")
            print(f"DEBUG: Switched to 5-Year History page: {history_page.url}")

            hcad_data['hcad_appraised_value_detail'] = _clean_numeric_value(get_text(history_page, "/html/body/table[2]/tbody/tr[2]/th[1]", "Most Recent Appraised Value"))
            hcad_data['hcad_market_value_detail'] = _clean_numeric_value(get_text(p_page, "/html/body/table/tbody/tr/td/table[12]/tbody/tr[6]/td[5]", "Total Market Value"))
            
            history = {}
            for i in range(1, 6):
                year = get_text(history_page, f"/html/body/table[2]/tbody/tr[1]/td[{i+1}]/b", f"Year {i}")
                value = _clean_numeric_value(get_text(history_page, f"/html/body/table[2]/tbody/tr[2]/th[{i}]", f"Value {i}"))
                if year: history[year] = value
            
            hcad_data['hcad_appraised_history_json'] = json.dumps(history) if history else None
            
            history_page.close()
            print("DEBUG: Closed history tab and returned to main detail page.")
        else:
            print("WARN: Could not find link to 5-Year Value History page.")
        
        print(f"SUCCESS: Parsed detail page data.")
        return hcad_data

    except Exception as e:
        print(f"ERROR: Exception during detail page parsing for {detail_url}: {e}")
        traceback.print_exc()

        # --- NEW: Save screenshots to a dedicated folder ---
        try:
            screenshot_folder = "hcad_error_screenshots"
            os.makedirs(screenshot_folder, exist_ok=True) # Create folder if it doesn't exist
            
            # Create a unique filename based on the case number and time
            case_num_for_file = hcad_data.get('probate_lead_case_number', 'UNKNOWN_CASE')
            timestamp = f"{time.time():.0f}"
            filename = f"parse_error_{case_num_for_file}_{timestamp}.png"

            screenshot_path = os.path.join(screenshot_folder, filename)
            p_page.screenshot(path=screenshot_path)
            print(f"INFO: Saved error screenshot to: {screenshot_path}")
        except Exception as se:
            print(f"ERROR: Could not save screenshot: {se}")
        # --- End of new logic ---

        hcad_data["parsing_error"] = str(e) 
        return hcad_data

#  Only parse_hcad_detail_page is modified here.

def _score_summary_candidate(candidate_summary, rp_data_row, tier_context):
    """
    Scores a candidate based ONLY on summary data from HCAD results list.
    Args:
        candidate_summary (dict): A dict from hcad_results_list (e.g., owner_summary, address_summary).
        rp_data_row (pd.Series): The input row data.
        tier_context (str): The name of the current search tier.
    Returns:
        float: The calculated summary score.
    """
    score = 0
    hcad_owner_summary = str(candidate_summary.get('hcad_owner_summary', '')).upper().strip()
    hcad_address_summary = str(candidate_summary.get('hcad_address_summary', '')).upper().strip() # This often contains legal desc/subdivision
    hcad_account_summary = str(candidate_summary.get('hcad_account_summary', '')).upper().strip()

    # --- Owner Name Component (if tier involves owner) ---
    owner_query_name_for_tier = ""
    # Determine the target name from rp_data_row based on tier_context
    if "GranteeLastName" in tier_context:
        # For T1_GranteeLastName_Subdivision, we'd use the first grantee's last name
        grantee_names = rp_data_row.get('rp_grantee_full_names_list', [])
        if grantee_names and isinstance(grantee_names, list) and grantee_names[0]:
            first_grantee_full_name = str(grantee_names[0]).strip()
            owner_query_name_for_tier = first_grantee_full_name.split()[-1].upper() # Just last name
    elif "DecedentLastName" in tier_context or "Fallback_Owner" in tier_context:
        owner_query_name_for_tier = str(rp_data_row.get('probate_lead_decedent_last', '')).upper().strip()
    
    if owner_query_name_for_tier and hcad_owner_summary:
        # Simple check: is the query name part of the HCAD owner summary?
        # token_set_ratio is good for this as names can be reordered or have initials.
        owner_match_score = fuzz.token_set_ratio(owner_query_name_for_tier, hcad_owner_summary)
        score += owner_match_score * 0.5 # Weight: 50%

    # --- Legal/Address Component (Subdivision mainly) ---
    rp_subdivision_orig = str(rp_data_row.get('rp_legal_description_text', '')).upper().strip()
    if rp_subdivision_orig and hcad_address_summary:
        # hcad_address_summary often IS the subdivision or contains it.
        # token_set_ratio is good for matching phrases with common words.
        subdiv_match_score = fuzz.token_set_ratio(rp_subdivision_orig, hcad_address_summary)
        score += subdiv_match_score * 0.5 # Weight: 50%
    
    # --- Bonus for T1/T2 specific components if address summary is very close to full legal ---
    # This is harder with just summary, but we can try.
    if tier_context in ["T2_ExactLegal", "T3_DropSec"]:
        full_rp_legal = construct_full_rp_legal_for_comparison(rp_data_row)
        if full_rp_legal and hcad_address_summary:
            # If address summary is a good partial match for full legal, boost score
            # This is a rough approximation
            t1_t2_bonus_score = fuzz.partial_ratio(full_rp_legal, hcad_address_summary)
            if t1_t2_bonus_score > 70 : # If pretty good partial match
                score += t1_t2_bonus_score * 0.2 # Add a small bonus (max 20 points)

    # print(f"DEBUG: Summary Score for Acct {hcad_account_summary} (Tier: {tier_context}): {score:.2f} (Owner: '{hcad_owner_summary}', Addr: '{hcad_address_summary}') vs RP Owner Query: '{owner_query_name_for_tier}', RP Subdiv: '{rp_subdivision_orig}'")
    return score


def _score_detailed_candidate(detailed_candidate_data, rp_data_row, tier_context):
    """
    Scores a candidate that has full details fetched, incorporating weighted blending
    of legal and owner scores, and smarter RP target name selection.
    Args:
        detailed_candidate_data (dict): Candidate dict, now including keys from parse_hcad_detail_page.
        rp_data_row (pd.Series): The input row data.
        tier_context (str): The name of the current search tier.
    Returns:
        float: The calculated detailed score (0-100).
    """
    legal_score_raw = 0
    owner_score_normalized = 50 # Default to a neutral owner score

    # HCAD data from detailed page
    hcad_legal_detail = str(detailed_candidate_data.get('hcad_legal_desc_detail', '')).upper().strip()
    hcad_owner_detail = str(detailed_candidate_data.get('hcad_owner_full_name', '')).upper().strip()

    # RP Data for legal comparison
    rp_tract_orig = str(rp_data_row.get('rp_legal_tract', '')).upper().strip()
    rp_block_orig = str(rp_data_row.get('rp_legal_block', '')).upper().strip()
    rp_lot_orig = str(rp_data_row.get('rp_legal_lot', '')).upper().strip()
    rp_subdivision_orig = str(rp_data_row.get('rp_legal_description_text', '')).upper().strip()
    rp_section_orig = str(rp_data_row.get('rp_legal_sec', str(rp_data_row.get('rp_legal_section', '')))).upper().strip()

    # Determine the RP target owner name for comparison against HCAD owner
    # This logic considers the HCAD search tier context
    rp_target_owner_first_name_full = "" 
    rp_target_owner_last = ""

    if "GranteeLastName" in tier_context:
        grantee_names = rp_data_row.get('rp_grantee_full_names_list', [])
        if grantee_names and isinstance(grantee_names, list) and grantee_names[0]:
            first_grantee_full_name_parts = str(grantee_names[0]).upper().strip().split()
            if len(first_grantee_full_name_parts) > 0:
                rp_target_owner_last = first_grantee_full_name_parts[-1]
                rp_target_owner_first_name_full = " ".join(first_grantee_full_name_parts[:-1])
    elif "DecedentLastName" in tier_context or "Fallback_Owner" in tier_context:
        rp_target_owner_last = str(rp_data_row.get('probate_lead_decedent_last', '')).upper().strip()
        rp_target_owner_first_name_full = str(rp_data_row.get('probate_lead_decedent_first', '')).upper().strip()
    else: # For purely legal search tiers (T1, T2, T4)
        if rp_data_row.get('cleaned_rp_party_last_name'): # Prioritize cleaned RP party name
            rp_target_owner_last = str(rp_data_row.get('cleaned_rp_party_last_name', '')).upper().strip()
            rp_target_owner_first_name_full = str(rp_data_row.get('cleaned_rp_party_first_name', '')).upper().strip()
        else: # Fallback to probate decedent if no cleaned_rp_party name
            rp_target_owner_last = str(rp_data_row.get('probate_lead_decedent_last', '')).upper().strip()
            rp_target_owner_first_name_full = str(rp_data_row.get('probate_lead_decedent_first', '')).upper().strip()
    
    rp_target_full_name_for_match = f"{rp_target_owner_first_name_full} {rp_target_owner_last}".strip()
    if not rp_target_full_name_for_match.strip() : rp_target_full_name_for_match = None


    # --- Legal Score Calculation ---
    if tier_context == "T4_Subdivision_Block":
        original_rp_legal_for_t4 = construct_full_rp_legal_for_comparison(rp_data_row)
        if original_rp_legal_for_t4 and hcad_legal_detail:
            legal_score_raw = fuzz.ratio(original_rp_legal_for_t4, hcad_legal_detail)
    else:
        if rp_tract_orig and rp_tract_orig != 'NAN' and f"TR {rp_tract_orig}" in hcad_legal_detail: legal_score_raw += 40
        if rp_block_orig and rp_block_orig != 'NAN' and f"BLK {rp_block_orig}" in hcad_legal_detail: legal_score_raw += 30
        if rp_lot_orig and rp_lot_orig != 'NAN':
            if f"LT {rp_lot_orig}" in hcad_legal_detail or f"LOT {rp_lot_orig}" in hcad_legal_detail: legal_score_raw += 20
        if rp_section_orig and rp_section_orig != 'NAN' and f"SEC {rp_section_orig}" in hcad_legal_detail: legal_score_raw += 10
        
        if rp_subdivision_orig and rp_subdivision_orig != 'NAN' and hcad_legal_detail:
            if rp_subdivision_orig.strip(): 
                subdivision_similarity = fuzz.token_set_ratio(rp_subdivision_orig, hcad_legal_detail)
                legal_score_raw += (subdivision_similarity / 100.0) * 30 
    
    normalized_legal_score = min(100.0, (legal_score_raw / 130.0) * 100.0) if legal_score_raw > 0 else 0.0
    normalized_legal_score = max(0.0, normalized_legal_score) # Ensure non-negative

    # --- Owner Score Calculation (0-100) ---
    if hcad_owner_detail and rp_target_full_name_for_match:
        # Use token_set_ratio for overall name similarity (handles word order, partial matches well)
        owner_score_normalized = fuzz.token_set_ratio(rp_target_full_name_for_match, hcad_owner_detail)
        
        # Penalize if it's an owner-focused tier and the match is poor
        if owner_score_normalized < 60 and ("T1_" in tier_context or "Fallback_Owner" in tier_context):
            owner_score_normalized = max(0, owner_score_normalized - 20) # Further reduce poor scores on owner tiers
    elif not hcad_owner_detail and rp_target_full_name_for_match : # HCAD owner missing, but we had an RP target
        owner_score_normalized = 20 # Low score if HCAD owner is blank
    elif hcad_owner_detail and not rp_target_full_name_for_match: # RP target missing, but HCAD owner present
        owner_score_normalized = 30 # Also low score
    else: # Both missing
        owner_score_normalized = 50 # Neutral if no names to compare

    # --- Score Blending ---
    # Weights: Legal 70%, Owner 30%
    # (Adjust weights if needed: e.g., 0.6 legal, 0.4 owner if owner match is more critical)
    final_blended_score = (normalized_legal_score * 0.70) + (owner_score_normalized * 0.30)
    
    # print(f"DEBUG _score_detailed_candidate: Tier={tier_context}, RP Target='{rp_target_full_name_for_match}', HCAD Owner='{hcad_owner_detail}'")
    # print(f"DEBUG _score_detailed_candidate: LegalRaw={legal_score_raw:.2f} -> NormLegal={normalized_legal_score:.2f}")
    # print(f"DEBUG _score_detailed_candidate: NormOwner={owner_score_normalized:.2f}")
    # print(f"DEBUG _score_detailed_candidate: FinalBlended={final_blended_score:.2f}")

    return max(0.0, min(100.0, final_blended_score))


def construct_search_query(rp_data_row, tier):
    # --- Standard variable extraction at the beginning ---
    tract = str(rp_data_row.get('rp_legal_tract', '')).upper().strip()
    block = str(rp_data_row.get('rp_legal_block', '')).upper().strip()
    subdivision = str(rp_data_row.get('rp_legal_description_text', '')).upper().strip()
    section = str(rp_data_row.get('rp_legal_sec', str(rp_data_row.get('rp_legal_section', '')))).upper().strip() # Handles both keys

    # --- MODIFIED: Replaced the simple extraction for 'lot' with this improved logic ---
    raw_lot = rp_data_row.get('rp_legal_lot', None) # Get it potentially as float/int or string
    lot = "" # Default to empty string
    if pd.notna(raw_lot) and str(raw_lot).strip() != "":
        try:
            # Attempt to convert to float first, then to int, to handle "X.0"
            float_val = float(raw_lot)
            if float_val.is_integer(): # Check if it's a whole number (e.g., 5.0)
                lot = str(int(float_val)) # Convert to "5"
            else:
                lot = str(raw_lot).upper().strip() # Keep as is if it has actual decimals e.g. "5.5A" (unlikely for lot)
        except ValueError:
            # If it's not a number (e.g., "A", "10A", or already "5"), just use it as is
            lot = str(raw_lot).upper().strip()
    # --- END OF MODIFICATION ---

    grantee_full_names = rp_data_row.get('rp_grantee_full_names_list', [])
    first_grantee_last_name = None
    if isinstance(grantee_full_names, list) and grantee_full_names:
        first_grantee_full_name_parts = str(grantee_full_names[0]).split()
        if first_grantee_full_name_parts:
            first_grantee_last_name = first_grantee_full_name_parts[-1].upper().strip()

    decedent_last_for_search = str(rp_data_row.get('probate_lead_decedent_last', '')).upper().strip()

    # Clean 'NAN' values from main variables for query construction
    tract = tract if tract and tract != 'NAN' else "" # Use empty string if NAN for easier logic
    block = block if block and block != 'NAN' else ""
    subdivision = subdivision if subdivision and subdivision != 'NAN' else ""
    section = section if section and section != 'NAN' else ""
    # The existing NAN check for 'lot' is kept, as the new logic runs before it
    lot = lot if lot and lot != 'NAN' else ""
    first_grantee_last_name = first_grantee_last_name if first_grantee_last_name and first_grantee_last_name != 'NAN' else None
    decedent_last_for_search = decedent_last_for_search if decedent_last_for_search and decedent_last_for_search != 'NAN' else None
    # --- End of standard variable extraction ---

    legal_query = None
    owner_query = None

    if tier == "T0_ExactLotBlockSubdivision":
        # This tier requires Lot, Block, and Subdivision to be present
        # Assumes lot is NOT a range because upstream process splits rows.
        if lot and block and subdivision and '-' not in lot and '/' not in lot:
            query_parts = [f"LT {lot}", f"BLK {block}", subdivision]
            legal_query = " ".join(query_parts)
        else:
            print(f"DEBUG (Tier T0): Insufficient data for T0 (Lot: '{lot}', Block: '{block}', Sub: '{subdivision}')")
            return None, None # Explicitly return None if T0 criteria not met

    elif tier == "T2_ExactLegal" or tier == "T3_DropSec": # YOUR RENAMED TIERS (Formerly T1 & T2)
        query_parts = []
        if tract: query_parts.append(f"TR {tract}")
        if block: query_parts.append(f"BLK {block}")
        if subdivision: query_parts.append(subdivision)
        if tier == "T2_ExactLegal" and section: # Check original tier name intent
            query_parts.append(f"SEC {section}")
        if query_parts:
            legal_query = " ".join(query_parts).strip()
    
    elif tier == "T1_GranteeLastName_Subdivision":
        if first_grantee_last_name and subdivision:
            if first_grantee_last_name in COMMON_SURNAMES and not block and not tract:
                return "COMMON_SURNAME_TOO_BROAD", None
            owner_query = first_grantee_last_name
            
            # CORRECTED: Build the list in the desired order
            legal_parts_t3 = []
            if block: legal_parts_t3.append(f"BLK {block}")
            if subdivision: legal_parts_t3.append(subdivision)
            if tract: legal_parts_t3.append(f"TR {tract}")
            legal_query = " ".join(legal_parts_t3).strip()
        else:
            print(f"DEBUG (Tier {tier}): Insufficient data. FirstGranteeLastName: '{first_grantee_last_name}', Sub: '{subdivision}'")
            return None, None

    elif tier == "T1_GrantorLastName_Subdivision":
        if decedent_last_for_search and subdivision:
            if decedent_last_for_search in COMMON_SURNAMES and not block and not tract:
                return "COMMON_SURNAME_TOO_BROAD", None
            owner_query = decedent_last_for_search

            # CORRECTED: Build the list in the desired order
            legal_parts_t3 = []
            if block: legal_parts_t3.append(f"BLK {block}")
            if subdivision: legal_parts_t3.append(subdivision)
            if tract: legal_parts_t3.append(f"TR {tract}")
            legal_query = " ".join(legal_parts_t3).strip()
        else:
            print(f"DEBUG (Tier {tier}): Insufficient data. DecedentLast: '{decedent_last_for_search}', Sub: '{subdivision}'")
            return None, None
    elif tier == "T4_Subdivision_Block": # YOUR T4
        query_parts = []
        if block: query_parts.append(f"BLK {block}")
        if subdivision: query_parts.append(subdivision)
        if query_parts:
            legal_query = " ".join(query_parts).strip()
        else: # Needs at least block or subdivision
            return None, None
            
    elif tier == "Fallback_Owner_SubdivisionContains":
        if decedent_last_for_search and subdivision:
            if decedent_last_for_search in COMMON_SURNAMES and not block and not tract:
                return "COMMON_SURNAME_TOO_BROAD", None
            owner_query = decedent_last_for_search
            legal_parts_fallback = [subdivision] # Subdivision is primary
            # For fallback, maybe only add block OR tract, not both to keep it broader?
            if block: legal_parts_fallback.append(f"BLK {block}")
            elif tract: legal_parts_fallback.append(f"TR {tract}") # Use elif to make it one or the other
            legal_query = " ".join(legal_parts_fallback).strip()
        else:
            print(f"DEBUG (Tier {tier}): Insufficient data. DecedentLast: '{decedent_last_for_search}', Sub: '{subdivision}'")
            return None, None
    
    # Final checks and truncation (ensure this is at the very end)
    if legal_query == "": legal_query = None # Ensure empty string becomes None
    if owner_query == "": owner_query = None

    if not legal_query and not owner_query and tier not in ["T0_ExactLotBlockSubdivision"]: # T0 handles its own None return
        if not (tier == "T0_ExactLotBlockSubdivision" and (not lot or not block or not subdivision)): # Avoid double print for T0
             print(f"DEBUG: Tier {tier} resulted in no usable query components after construction logic.")
        return None, None # Ensure it returns None, None if no query was formed by any tier logic path

    # Truncation
    if legal_query and len(legal_query) > 100:
        print(f"WARN: Truncating legal_query from {len(legal_query)} to 100 chars for tier {tier}.")
        legal_query = legal_query[:100]
    if owner_query and len(owner_query) > 26:
        print(f"WARN: Truncating owner_query from {len(owner_query)} to 26 chars for tier {tier}.")
        owner_query = owner_query[:26]
            
    return legal_query, owner_query


def construct_full_rp_legal_for_comparison(rp_data_row): # Copied from previous, ensure it's in your script
    """Helper to construct a comparable legal string from RP data components."""
    parts = []
    tract = str(rp_data_row.get('rp_legal_tract', '')).upper().strip()
    block = str(rp_data_row.get('rp_legal_block', '')).upper().strip()
    subdivision = str(rp_data_row.get('rp_legal_description_text', '')).upper().strip()
    section = str(rp_data_row.get('rp_legal_section', '')).upper().strip() # You might have 'rp_legal_sec'
    lot = str(rp_data_row.get('rp_legal_lot', '')).upper().strip()

    # Use the exact field names from your CSV for rp_data_row.get()
    if tract and tract != 'NAN': parts.append(f"TR {tract}")
    if block and block != 'NAN': parts.append(f"BLK {block}")
    if subdivision and subdivision != 'NAN': parts.append(subdivision)
    if lot and lot != 'NAN': parts.append(f"LOT {lot}") 
    if section and section != 'NAN': parts.append(f"SEC {section}")
    return " ".join(filter(None, parts)).strip()

# ... (imports and other functions) ...
# Ensure construct_full_rp_legal_for_comparison is defined

def choose_best_from_multiple(hcad_results_list, rp_data_row, tier_context,
                              p_page_for_detail_scrape, confidence_level_of_rp_row):
    global HCAD_DETAIL_CACHE # Access the global cache

    if not hcad_results_list: return None
    print(f"INFO: Choosing best from {len(hcad_results_list)} results for tier '{tier_context}'. RP Sub: '{rp_data_row.get('rp_legal_description_text', '')}', Confidence: {confidence_level_of_rp_row}")

    # --- 1. Summary-Only Scoring Round ---
    scored_summaries = []
    for candidate_summary_item in hcad_results_list:
        # Ensure it's a mutable copy if direct modifications are made later, though not in _score_summary
        current_candidate_summary = candidate_summary_item.copy() 
        summary_score = _score_summary_candidate(current_candidate_summary, rp_data_row, tier_context)
        current_candidate_summary['hcad_list_page_summary_rank_score'] = summary_score
        scored_summaries.append(current_candidate_summary)

    scored_summaries.sort(key=lambda x: x.get('hcad_list_page_summary_rank_score', 0), reverse=True)

    print(f"DEBUG: Top 5 Summary Scored Candidates for tier '{tier_context}':")
    for i, cand in enumerate(scored_summaries[:5]):
        print(f"  #{i+1} Acct: {cand.get('hcad_account_summary')}, Summary Score: {cand.get('hcad_list_page_summary_rank_score', 0):.2f}, Owner: {cand.get('hcad_owner_summary')}, Address: {cand.get('hcad_address_summary')}")

    # --- 2. Check for Clear Winner from Summary Scores ---
    if scored_summaries:
        top_summary_candidate = scored_summaries[0]
        top_summary_score = top_summary_candidate.get('hcad_list_page_summary_rank_score', 0)
        
        second_summary_score = 0
        if len(scored_summaries) > 1:
            second_summary_score = scored_summaries[1].get('hcad_list_page_summary_rank_score', 0)

        if top_summary_score >= SUMMARY_SCORE_ABSOLUTE_THRESHOLD and \
           (len(scored_summaries) == 1 or (top_summary_score - second_summary_score) >= SUMMARY_SCORE_DIFFERENCE_THRESHOLD):
            print(f"INFO: Clear winner identified from summary scoring: Acct {top_summary_candidate.get('hcad_account_summary')} (Summary Score: {top_summary_score:.2f}). Fetching its details for confirmation.")
            # Fetch details for this single summary winner to return a complete record
            # (Cache-aware fetching for this one winner)
            detail_data_for_summary_winner = None
            account_s = top_summary_candidate.get('hcad_account_summary')
            url_s = top_summary_candidate.get('hcad_detail_url')
            
            parsed_acct_s_url = None
            if url_s:
                match_s = re.search(r"acct=(\d+)", url_s, re.IGNORECASE)
                if match_s: parsed_acct_s_url = match_s.group(1)
            
            key_s_check = parsed_acct_s_url if parsed_acct_s_url else account_s

            if key_s_check and key_s_check in HCAD_DETAIL_CACHE:
                detail_data_for_summary_winner = HCAD_DETAIL_CACHE[key_s_check]
            elif p_page_for_detail_scrape and url_s:
                detail_data_for_summary_winner = parse_hcad_detail_page(p_page_for_detail_scrape, url_s)
                if detail_data_for_summary_winner and not detail_data_for_summary_winner.get('parsing_error') and detail_data_for_summary_winner.get('hcad_account'):
                    def_key_s = detail_data_for_summary_winner['hcad_account']
                    HCAD_DETAIL_CACHE[def_key_s] = detail_data_for_summary_winner
                    # Also cache by other potential keys if different
                    if parsed_acct_s_url and parsed_acct_s_url != def_key_s and parsed_acct_s_url not in HCAD_DETAIL_CACHE: HCAD_DETAIL_CACHE[parsed_acct_s_url] = detail_data_for_summary_winner
                    if account_s and account_s != def_key_s and account_s not in HCAD_DETAIL_CACHE: HCAD_DETAIL_CACHE[account_s] = detail_data_for_summary_winner


            if detail_data_for_summary_winner and not detail_data_for_summary_winner.get('parsing_error'):
                top_summary_candidate.update(detail_data_for_summary_winner)
                # Re-score with detailed info to be consistent for the final 'hcad_best_property_fit_score'
                detailed_score_for_summary_winner = _score_detailed_candidate(top_summary_candidate, rp_data_row, tier_context)
                top_summary_candidate['hcad_best_property_fit_score'] = detailed_score_for_summary_winner
                print(f"INFO: Summary winner Acct {top_summary_candidate.get('hcad_account')} re-scored with details: {detailed_score_for_summary_winner:.2f}")
                return top_summary_candidate
            else:
                print(f"WARN: Could not fetch/parse details for summary-picked winner {account_s}. Discarding as winner.")
                # Fall through to detailed fetching for other candidates if any.
    
    # --- 3. Conditional Detail Fetching (if no clear summary winner) ---
    candidates_for_detailed_scoring = []
    # Fetch details for top N candidates from summary scoring (up to DETAIL_FETCH_LIMIT_AFTER_SUMMARY)
    # Only do this if confidence_level_of_rp_row is 'High' OR if it's a very promising tier.
    # For now, let's simplify: always try to detail fetch for the top few if summary was inconclusive.
    
    num_to_fetch_detail = min(len(scored_summaries), DETAIL_FETCH_LIMIT_AFTER_SUMMARY)
    print(f"DEBUG: No clear summary winner. Will fetch details for up to {num_to_fetch_detail} top summary candidates.")

    for i in range(num_to_fetch_detail):
        candidate_to_detail = scored_summaries[i].copy() # Work on a copy
        account_d = candidate_to_detail.get('hcad_account_summary')
        url_d = candidate_to_detail.get('hcad_detail_url')
        
        parsed_acct_d_url = None
        if url_d:
            match_d = re.search(r"acct=(\d+)", url_d, re.IGNORECASE)
            if match_d: parsed_acct_d_url = match_d.group(1)
        key_d_check = parsed_acct_d_url if parsed_acct_d_url else account_d
        
        fetched_detail_data = None
        if key_d_check and key_d_check in HCAD_DETAIL_CACHE:
            print(f"DEBUG: [CACHE HIT] For detail fetch candidate Acct: {key_d_check}")
            fetched_detail_data = HCAD_DETAIL_CACHE[key_d_check]
        elif p_page_for_detail_scrape and url_d:
            print(f"DEBUG: [CACHE MISS] Fetching details for candidate {account_d}, URL: {url_d}")
            fetched_detail_data = parse_hcad_detail_page(p_page_for_detail_scrape, url_d)
            if fetched_detail_data and not fetched_detail_data.get('parsing_error') and fetched_detail_data.get('hcad_account'):
                def_key_d = fetched_detail_data['hcad_account']
                HCAD_DETAIL_CACHE[def_key_d] = fetched_detail_data
                print(f"INFO: Cached details for account {def_key_d}")
                if parsed_acct_d_url and parsed_acct_d_url != def_key_d and parsed_acct_d_url not in HCAD_DETAIL_CACHE: HCAD_DETAIL_CACHE[parsed_acct_d_url] = fetched_detail_data
                if account_d and account_d != def_key_d and account_d not in HCAD_DETAIL_CACHE: HCAD_DETAIL_CACHE[account_d] = fetched_detail_data
        
        if fetched_detail_data and not fetched_detail_data.get('parsing_error'):
            candidate_to_detail.update(fetched_detail_data)
            detailed_score = _score_detailed_candidate(candidate_to_detail, rp_data_row, tier_context)
            candidate_to_detail['hcad_best_property_fit_score'] = detailed_score
            candidates_for_detailed_scoring.append(candidate_to_detail)
            print(f"DEBUG: Detailed Candidate Acct {candidate_to_detail.get('hcad_account')} scored: {detailed_score:.2f}")

            # Early exit if a very high confidence match is found during this limited detail fetch
            if detailed_score >= AUTO_WINNER_DETAIL_SCORE_THRESHOLD:
                print(f"INFO: Auto-winner found after detail fetch: Acct {candidate_to_detail.get('hcad_account')} (Score: {detailed_score:.2f}). Selecting.")
                return candidate_to_detail
        else:
            # If detail fetch failed, use its summary score as its final hcad_best_property_fit_score
            candidate_to_detail['hcad_best_property_fit_score'] = candidate_to_detail.get('hcad_list_page_summary_rank_score', 0)
            candidates_for_detailed_scoring.append(candidate_to_detail) # Still add it for consideration with summary score
            error_msg_d = fetched_detail_data.get('parsing_error') if fetched_detail_data else 'Unknown or no URL'
            print(f"WARN: Could not fetch/parse details for {account_d}. Using its summary score. Error: {error_msg_d}")
        
        if i < (num_to_fetch_detail - 1) : time.sleep(0.75) # Pause between detail fetches

    # --- 4. Final Decision Logic (based on detailed scores or fallbacks) ---
    if not candidates_for_detailed_scoring:
        print(f"WARN: No candidates available for detailed scoring for tier '{tier_context}'. This shouldn't happen if scored_summaries had items.")
        return None # Fallback, though unlikely if scored_summaries was not empty

    # Sort by the final 'hcad_best_property_fit_score' (which is detailed score, or summary score if detail fetch failed)
    candidates_for_detailed_scoring.sort(key=lambda x: x.get('hcad_best_property_fit_score', 0), reverse=True)

    print(f"DEBUG: Top Detailed/Final Scored Results for tier '{tier_context}':")
    for sr_idx, sr_val in enumerate(candidates_for_detailed_scoring[:5]): # Show top 5 from this batch
        print(f"  #{sr_idx+1} Acct: {sr_val.get('hcad_account_summary')}, Final Score: {sr_val.get('hcad_best_property_fit_score',0):.2f}, Legal(detail): {str(sr_val.get('hcad_legal_desc_detail', 'N/A'))[:70]}...")
    
    # Apply decision logic to this potentially small list
    if not candidates_for_detailed_scoring: # Should be redundant due to check above
        print(f"WARN: No scorable results after detail fetch for tier '{tier_context}'.")
        return None

    # Check top score based on detailed scoring
    top_detailed_candidate = candidates_for_detailed_scoring[0]
    top_detailed_score = top_detailed_candidate.get('hcad_best_property_fit_score',0)

    # Determine thresholds based on input confidence
    min_acceptable_score = DETAILED_SCORE_MIN_ACCEPTABLE_THRESHOLD_HIGH_CONF if confidence_level_of_rp_row == 'High' else DETAILED_SCORE_MIN_ACCEPTABLE_THRESHOLD_LOW_CONF
    min_score_difference = DETAILED_SCORE_DIFFERENCE_THRESHOLD_HIGH_CONF if confidence_level_of_rp_row == 'High' else DETAILED_SCORE_DIFFERENCE_THRESHOLD_LOW_CONF

    if top_detailed_score == 0 and confidence_level_of_rp_row == 'High': # Strict check for high conf if top score is 0
         print(f"WARN: Top detailed score is 0 for HIGH confidence lead. Cannot confidently choose for tier '{tier_context}'.")
         return None
    
    if len(candidates_for_detailed_scoring) == 1:
        if top_detailed_score >= min_acceptable_score:
            return top_detailed_candidate
        else:
            print(f"WARN: Tier '{tier_context}' single detailed result score {top_detailed_score:.2f} too low (need >={min_acceptable_score}).")
            return None
    
    # If more than one candidate was detailed and scored
    if len(candidates_for_detailed_scoring) > 1:
        second_detailed_score = candidates_for_detailed_scoring[1].get('hcad_best_property_fit_score',0)
        if top_detailed_score >= min_acceptable_score and (top_detailed_score - second_detailed_score) >= min_score_difference:
            return top_detailed_candidate
        else:
            print(f"WARN: Tier '{tier_context}' (Conf: {confidence_level_of_rp_row}) - Detailed scores too close or top score too low. Top: {top_detailed_score:.2f} (need >={min_acceptable_score}), Diff: {(top_detailed_score - second_detailed_score):.2f} (need >={min_score_difference}).")
            return None
    
# Fallback if scored_summaries was empty to begin with (already handled by initial check)

def search_hcad_and_get_results(p_page, search_tier_name, legal_desc_query=None, owner_name_query=None):
    print(f"INFO: [HCAD Search - {search_tier_name}] Attempting search. Legal: '{legal_desc_query}', Owner: '{owner_name_query}'")
    
    main_page = p_page
    iframe_context = None
    initial_iframe_url_for_debug = "" 

    # --- Navigate to main page and get iframe ---
    current_main_url = main_page.url
    if HCAD_ADVANCED_SEARCH_URL not in current_main_url:
        print(f"INFO: Navigating to main search page: {HCAD_ADVANCED_SEARCH_URL}")
        main_page.goto(HCAD_ADVANCED_SEARCH_URL, timeout=60000, wait_until="networkidle")
        main_page.wait_for_load_state("networkidle", timeout=20000)
    else:
        print(f"DEBUG: Already on main search page URL or reset. Attempting to get/re-get iframe.")

    iframe_selector = 'iframe[src*="public.hcad.org/records/Real/Advanced.asp"]'
    try:
        iframe_element = main_page.wait_for_selector(iframe_selector, state='visible', timeout=20000)
        if not iframe_element: raise Exception(f"iframe_element for '{iframe_selector}' is None.")
        
        iframe_context = iframe_element.content_frame()
        if not iframe_context: raise Exception("content_frame() returned None for iframe.")
        
        initial_iframe_url_for_debug = iframe_context.url 
        print(f"DEBUG: Successfully got iframe context. Initial Frame URL: {initial_iframe_url_for_debug}")
        
        iframe_context.wait_for_load_state("networkidle", timeout=25000)
        print(f"DEBUG: Initial iframe network is idle and form should be ready.")

    except Exception as e_iframe:
        print(f"ERROR: Could not get or initialize iframe context: {e_iframe}")
        try:
            print(f"ERROR: Forcing full reload of main search page due to iframe init error.")
            main_page.goto(HCAD_ADVANCED_SEARCH_URL, timeout=60000, wait_until="networkidle")
            main_page.wait_for_load_state("networkidle", timeout=20000)
            iframe_element = main_page.wait_for_selector(iframe_selector, state='visible', timeout=20000)
            iframe_context = iframe_element.content_frame()
            if not iframe_context: raise Exception("content_frame() still None after reload.")
            initial_iframe_url_for_debug = iframe_context.url
            iframe_context.wait_for_load_state("networkidle", timeout=25000)
        except Exception as e_iframe_retry:
                print(f"ERROR: Still Could not get or initialize iframe context after retry: {e_iframe_retry}")
                return "ERROR", f"Failed to get iframe: {e_iframe_retry}"

    # 2. Locate and fill form elements WITHIN iframe_context
    try:
        legal_desc_input_locator = iframe_context.locator(LEGAL_DESC_INPUT_SELECTOR)
        owner_name_input_locator = iframe_context.locator(OWNER_NAME_INPUT_SELECTOR)
        search_button_locator = iframe_context.locator(SEARCH_BUTTON_SELECTOR)

        legal_desc_input_locator.wait_for(state='visible', timeout=15000)

        if legal_desc_query is not None:
            legal_desc_input_locator.fill("") 
            legal_desc_input_locator.fill(legal_desc_query, timeout=5000)
        if owner_name_query is not None:
            try:
                owner_name_input_locator.wait_for(state='visible', timeout=3000) 
                owner_name_input_locator.fill("") 
                owner_name_input_locator.fill(owner_name_query, timeout=5000)
            except Exception:
                print(f"WARN: Owner name input not processed (perhaps not visible/needed) for query '{owner_name_query}'")
        
        search_button_locator.wait_for(state='visible', timeout=5000)
    except Exception as e_form_fill:
        print(f"ERROR: Error locating or filling form elements in iframe: {e_form_fill}")
        _try_click_change_criteria_IN_IFRAME(iframe_context, main_page, f"{search_tier_name}_FORM_FILL_ERROR_RESET")
        return "ERROR", f"Form element interaction error: {e_form_fill}"

    # 3. Click search button
    print(f"DEBUG: Clicking search button (inside iframe).")
    try:
        with iframe_context.expect_navigation(wait_until="networkidle", timeout=30000):
             search_button_locator.click(timeout=10000)
        print(f"DEBUG: Clicked search and navigation in iframe completed. iFrame URL: {iframe_context.url}")
    except Exception as e_click_nav:
        print(f"DEBUG: Search button click did not result in iframe navigation (or timed out): {e_click_nav}. Assuming JS update, waiting for indicators.")
        iframe_context.wait_for_timeout(1000) 

    # 4. Wait for iframe content to update with result indicators
    print(f"DEBUG: Waiting for iframe content to update with result indicators...")
    no_records_text_selector_iframe = "p:has-text('No records match your search criteria.')"
    results_table_header_indicator_iframe = f"{RESULTS_TABLE_ROWS_SELECTOR.split(' > ')[0]} td.sub_header:has-text('Account Number')"
    change_criteria_button_indicator_iframe = CHANGE_CRITERIA_BUTTON_SELECTOR

    combined_results_indicators_iframe = (
        f"{no_records_text_selector_iframe}, "
        f"{RECORD_COUNT_TEXT_SELECTOR}, " 
        f"{results_table_header_indicator_iframe}, "
        f"{change_criteria_button_indicator_iframe}"
    )
    try:
        iframe_context.wait_for_selector(combined_results_indicators_iframe, timeout=25000)
        print(f"DEBUG: A result indicator appeared WITHIN IFRAME. iFrame URL: {iframe_context.url}")
    except Exception as e_wait_result_iframe:
        print(f"ERROR: Timeout waiting for result indicators within iframe: {e_wait_result_iframe}")
        _try_click_change_criteria_IN_IFRAME(iframe_context, main_page, f"{search_tier_name}_RESULT_TIMEOUT_RESET")
        return "ERROR", "Timeout waiting for result indicators within iframe."

    # 5. Result Analysis - All performed on iframe_context
    if iframe_context.query_selector(no_records_text_selector_iframe):
        print(f"INFO: HCAD Search - {search_tier_name} - resulted in 'No records match your search criteria.'")
        _try_click_change_criteria_IN_IFRAME(iframe_context, main_page, f"{search_tier_name}_NO_HITS_RESET")
        return "NO_HITS", None

    # --- MODIFIED Record Count Parsing Logic ---
    record_count_text = ""
    num_records = 0 # Default to 0
    record_count_para_element = iframe_context.query_selector(RECORD_COUNT_TEXT_SELECTOR) # p.justcenter

    if record_count_para_element:
        full_para_text = record_count_para_element.inner_text().strip()
        print(f"DEBUG: Full paragraph text for record count: '{full_para_text}'")

        match_num_direct = re.search(r'(\d+)', full_para_text) 

        if match_num_direct:
            record_count_text = match_num_direct.group(1)
            print(f"DEBUG: Extracted count directly from paragraph via regex: '{record_count_text}'")
        elif "0 records" in full_para_text.lower() or "no records match" in full_para_text.lower():
            record_count_text = "0"
            print(f"DEBUG: Found zero records text in paragraph: '{full_para_text}'")
        else:
            bold_element = record_count_para_element.query_selector('b')
            if bold_element:
                bold_text_content = bold_element.inner_text().strip()
                print(f"DEBUG: Text from bold tag: '{bold_text_content}'")
                match_num_bold = re.search(r'(\d+)', bold_text_content) 
                if match_num_bold:
                    record_count_text = match_num_bold.group(1)
                else:
                    record_count_text = "" 
            else:
                record_count_text = "" 

        if record_count_text.isdigit():
            num_records = int(record_count_text)
            # This print now happens regardless of num_records value if it's a digit
            print(f"INFO: Parsed num_records = {num_records} from count text in iframe.")
        elif record_count_text: 
            print(f"WARN: Record count text '{record_count_text}' was extracted but is not a digit. Assuming ambiguous (num_records=0).")
            num_records = 0 
        else: 
            print(f"WARN: Could not find or parse any record count text from '{RECORD_COUNT_TEXT_SELECTOR}'. Assuming ambiguous (num_records=0).")
            num_records = 0 
    else: 
        print(f"WARN: Record count paragraph '{RECORD_COUNT_TEXT_SELECTOR}' not found. Assuming ambiguous (num_records=0).")
        num_records = 0
    # --- END OF MODIFIED Record Count Parsing Logic ---

    if num_records == 0 and (record_count_para_element or "0 records" in (iframe_context.content() or "").lower()): # If we explicitly parsed zero OR found "0 records" text
        print(f"INFO: HCAD Search - {search_tier_name} - Confirmed 0 records found.")
        _try_click_change_criteria_IN_IFRAME(iframe_context, main_page, f"{search_tier_name}_ZERO_RECORDS_RESET")
        return "NO_HITS", None

    results_data = []
    rows = [] 
    if num_records > 0 or not record_count_para_element: 
        try:
            rows = iframe_context.query_selector_all(RESULTS_TABLE_ROWS_SELECTOR)
            print(f"DEBUG: Found {len(rows)} rows in iframe using selector: '{RESULTS_TABLE_ROWS_SELECTOR}'")
        except Exception as e_get_rows:
            print(f"DEBUG: Could not get rows using selector (might be direct detail page or error): {e_get_rows}")

        if num_records > HCAD_RESULTS_PER_PAGE and len(rows) >= HCAD_RESULTS_PER_PAGE:
            print(f"WARN: [HCAD Search - {search_tier_name}] HCAD reported {num_records} records, parsed {len(rows)} (a full page). PAGINATION_TOO_LARGE.")
            for i_sum, row_sum_element in enumerate(rows): 
                cols_sum = row_sum_element.query_selector_all('td')
                if len(cols_sum) >= 7:
                    account_link_element_sum = cols_sum[0].query_selector('a')
                    account_number_sum = account_link_element_sum.inner_text().strip() if account_link_element_sum else cols_sum[0].inner_text().strip()
                    detail_url_relative_sum = account_link_element_sum.get_attribute('href') if account_link_element_sum else None
                    detail_url_absolute_sum = urljoin("https://public.hcad.org/", detail_url_relative_sum) if detail_url_relative_sum else None
                    owner_name_raw_sum = cols_sum[1].inner_text()
                    owner_name_sum = ' '.join(owner_name_raw_sum.split()).strip()
                    property_address_sum = cols_sum[2].inner_text().strip()
                    zip_code_sum = cols_sum[3].inner_text().strip()
                    sq_ft_sum = _clean_numeric_value(cols_sum[4].inner_text().strip())
                    market_value_sum = _clean_numeric_value(cols_sum[5].inner_text().strip())
                    appraised_value_sum = _clean_numeric_value(cols_sum[6].inner_text().strip())
                    if detail_url_absolute_sum:
                        results_data.append({
                            'hcad_account_summary': account_number_sum, 'hcad_owner_summary': owner_name_sum,
                            'hcad_address_summary': property_address_sum, 'hcad_zip_summary': zip_code_sum,
                            'hcad_sqft_summary': sq_ft_sum, 'hcad_market_value_summary': market_value_sum,
                            'hcad_appraised_value_summary': appraised_value_sum, 'hcad_detail_url': detail_url_absolute_sum
                        })
            _try_click_change_criteria_IN_IFRAME(iframe_context, main_page, f"{search_tier_name}_PAGINATION_RESET")
            return "PAGINATION_TOO_LARGE", results_data

        if rows: 
            for i, row_element in enumerate(rows):
                cols = row_element.query_selector_all('td')
                if len(cols) >= 7: 
                    account_link_element = cols[0].query_selector('a')
                    account_number = account_link_element.inner_text().strip() if account_link_element else cols[0].inner_text().strip()
                    detail_url_relative = account_link_element.get_attribute('href') if account_link_element else None
                    detail_url_absolute = urljoin("https://public.hcad.org/", detail_url_relative) if detail_url_relative else None
                    owner_name_raw = cols[1].inner_text()
                    owner_name = ' '.join(owner_name_raw.split()).strip() 
                    property_address = cols[2].inner_text().strip()
                    zip_code = cols[3].inner_text().strip()
                    sq_ft = _clean_numeric_value(cols[4].inner_text().strip())
                    market_value = _clean_numeric_value(cols[5].inner_text().strip())
                    appraised_value = _clean_numeric_value(cols[6].inner_text().strip())

                    if detail_url_absolute: 
                        results_data.append({
                            'hcad_account_summary': account_number, 'hcad_owner_summary': owner_name,
                            'hcad_address_summary': property_address, 'hcad_zip_summary': zip_code,
                            'hcad_sqft_summary': sq_ft, 'hcad_market_value_summary': market_value,
                            'hcad_appraised_value_summary': appraised_value, 'hcad_detail_url': detail_url_absolute
                        })
                else:
                    print(f"WARN: Row {i} in iframe results table did not have enough columns ({len(cols)}). Skipping.")
    
    if results_data: # This condition implies rows were found and parsed successfully
        # Crucially, num_records should be correctly parsed now (e.g. 1 for your test case)
        if num_records == 1 and len(results_data) == 1: 
            print(f"INFO: Search returned 1 record, and 1 row parsed. Status: SINGLE_ITEM_IN_LIST.")
            _try_click_change_criteria_IN_IFRAME(iframe_context, main_page, f"{search_tier_name}_SINGLE_ITEM_RESET")
            return "SINGLE_ITEM_IN_LIST", results_data
        
        # If num_records > 1 or (num_records is ambiguous (0) but we found multiple rows)
        print(f"INFO: Search returned {num_records} record(s) (parsed: {len(results_data)}). Status: MULTIPLE_HITS.")
        _try_click_change_criteria_IN_IFRAME(iframe_context, main_page, f"{search_tier_name}_MULTIPLE_HITS_RESET")
        return "MULTIPLE_HITS", results_data
    elif not rows and num_records > 0 : 
        print(f"ERROR: RC >0 in iframe ({num_records}), but no rows found/parsed with selector. Problem with result table or selector.")
        _try_click_change_criteria_IN_IFRAME(iframe_context, main_page, f"{search_tier_name}_IFRAME_NO_ROWS_RESET")
        return "ERROR", "RC >0 in iframe, no rows found."
    
    current_iframe_url_after_search = iframe_context.url
    if "details.asp" in current_iframe_url_after_search.lower() and \
       initial_iframe_url_for_debug.lower() != current_iframe_url_after_search.lower():
        print(f"INFO: HCAD Search - {search_tier_name} - likely a direct navigation to detail page: {current_iframe_url_after_search}")
        return "UNIQUE_HIT", current_iframe_url_after_search

    print(f"WARN: [HCAD Search - {search_tier_name}] iFrame content in unrecognized state or truly no hits after all checks. iFrame URL: {iframe_context.url}")
    _try_click_change_criteria_IN_IFRAME(iframe_context, main_page, f"{search_tier_name}_UNKNOWN_STATE_RESET")
    return "NO_HITS_OR_UNKNOWN_PAGE", None

# Ensure _try_click_change_criteria_IN_IFRAME is defined as in the previous response.
# The rest of the script (parse_hcad_detail_page, etc.) remains the same.
def _try_click_change_criteria_IN_IFRAME(iframe_ctx, main_p, context_message):
    """Attempts to click 'Change Criteria' button *within the iframe*."""
    print(f"INFO: [{context_message}] Attempting to click 'Change Criteria' within iframe.")
    try:
        change_button_iframe = iframe_ctx.locator(CHANGE_CRITERIA_BUTTON_SELECTOR)
        if change_button_iframe.is_visible(timeout=5000):
            print(f"INFO: [{context_message}] 'Change Criteria' button found in iframe. Clicking.")
            # This click should reload the iframe back to its search form.
            with iframe_ctx.expect_navigation(wait_until="networkidle", timeout=30000):
                change_button_iframe.click()
            print(f"INFO: [{context_message}] Clicked 'Change Criteria' in iframe. iFrame URL: {iframe_ctx.url}")
            return True
        else:
            print(f"WARN: [{context_message}] 'Change Criteria' button not visible in iframe. Reloading main page to reset.")
            main_p.goto(HCAD_ADVANCED_SEARCH_URL, timeout=60000, wait_until="networkidle")
            return True # Main page reloaded, iframe should reset
    except Exception as ex_iframe_reset:
        print(f"ERROR: [{context_message}] Exception clicking 'Change Criteria' in iframe: {ex_iframe_reset}. Reloading main page.")
        main_p.goto(HCAD_ADVANCED_SEARCH_URL, timeout=60000, wait_until="networkidle")
        return True # Main page reloaded

# _try_click_change_criteria (for main page) should be kept if needed for other scenarios,
# but the main reset path after iframe operations will likely be reloading the main_page
# or using _try_click_change_criteria_IN_IFRAME.
# The rest of the script (_try_click_change_criteria, parse_hcad_detail_page, etc.) remains the same.
# _try_click_change_criteria function remains the same as the last version, it should handle most reset cases.
# The rest of the script (parse_hcad_detail_page, etc.) also remains the same.

def _try_click_change_criteria(p_page, context_message, fallback_to_main_search=False, force_navigation=False):
    """Helper function to attempt clicking 'Change Criteria' or navigate to main search."""
    print(f"INFO: [{context_message}] Attempting to reset/navigate.")
    try:
        if force_navigation: # Always force if requested
            print(f"INFO: [{context_message}] Forcing navigation to main search page: {HCAD_ADVANCED_SEARCH_URL}")
            p_page.goto(HCAD_ADVANCED_SEARCH_URL, timeout=60000, wait_until="networkidle")
            print(f"INFO: [{context_message}] Successfully navigated (forced).")
            return True

        # Check if we are on a page that *should* have the "Change Criteria" button
        # The "No records match your search criteria." page (from screenshot) does NOT have it.
        # Other results pages (with 1+ hits, or ambiguous count) typically DO.
        # The main search form itself also doesn't have it (but has input fields).

        if HCAD_ADVANCED_SEARCH_URL in p_page.url:
            # If we are on the main search URL, check if the form input is visible.
            # If so, we are on the search form page, no "Change Criteria" click needed.
            try:
                if p_page.locator(LEGAL_DESC_INPUT_SELECTOR).is_visible(timeout=3000): # Quick check
                    print(f"INFO: [{context_message}] Already on search form page with visible inputs. Reset not via button.")
                    return True
            except Exception: # Input not visible, maybe it's the iframe wrapper page.
                pass # Proceed to check for button or navigate

        change_criteria_button = p_page.query_selector(CHANGE_CRITERIA_BUTTON_SELECTOR)
        if change_criteria_button and change_criteria_button.is_visible():
            print(f"INFO: [{context_message}] Clicking 'Change Criteria or Sorted Order' button.")
            with p_page.expect_navigation(wait_until="networkidle", timeout=60000):
                change_criteria_button.click()
            print(f"INFO: [{context_message}] Successfully clicked 'Change Criteria' and navigated.")
            return True
        # If button not found/visible OR if explicitly told to fallback_to_main_search
        elif fallback_to_main_search or HCAD_ADVANCED_SEARCH_URL not in p_page.url:
            print(f"WARN: [{context_message}] 'Change Criteria' not found/visible or fallback requested. Navigating to main search page: {HCAD_ADVANCED_SEARCH_URL}")
            p_page.goto(HCAD_ADVANCED_SEARCH_URL, timeout=60000, wait_until="networkidle")
            print(f"INFO: [{context_message}] Successfully navigated to main search (fallback/direct).")
            return True
        else:
            print(f"WARN: [{context_message}] 'Change Criteria' button not found/visible. No fallback nav, current URL: {p_page.url}")
            return False # Indicate reset action might not have happened as expected

    except Exception as ex:
        print(f"ERROR: [{context_message}] Exception during reset/navigation: {ex}")
        try:
            print(f"INFO: [{context_message}] Final fallback: Navigating to main search page after reset error.")
            p_page.goto(HCAD_ADVANCED_SEARCH_URL, timeout=60000, wait_until="networkidle")
            return True
        except Exception as ex_nav:
            print(f"ERROR: [{context_message}] Final attempt to navigate to main search page also failed: {ex_nav}")
    return False



# ... (ALL OTHER FUNCTION DEFINITIONS: _clean_numeric_value, 
#      search_hcad_and_get_results (with PAGINATION_TOO_LARGE return), 
#      _try_click_change_criteria_IN_IFRAME, _try_click_change_criteria,
#      parse_hcad_detail_page, 
#      construct_search_query (with COMMON_SURNAME_TOO_BROAD return), 
#      construct_full_rp_legal_for_comparison, 
#      choose_best_from_multiple (with conditional detail fetching & scoring)) ...


def main_hcad_processing_loop(df_processed_input, playwright_page):
    all_enriched_data = []
    search_tiers_config = [
        {"name": "T0_ExactLotBlockSubdivision", "type": "legal_primary_exact"},
        {"name": "T1_GranteeLastName_Subdivision", "type": "owner_legal_combo"}, # Renamed from T3
        {"name": "T1_GrantorLastName_Subdivision", "type": "owner_legal_combo"},# Renamed from T3
        {"name": "T2_ExactLegal", "type": "legal_primary"}, # Renamed from T1
        {"name": "T3_DropSec", "type": "legal_primary"}, # Renamed from T2
        {"name": "Fallback_Owner_SubdivisionContains", "type": "owner_legal_combo"},
        {"name": "T4_Subdivision_Block", "type": "legal_primary"},
    ]

    fuzzy_match_cols_to_init = [
        'hcad_detail_url_visited', 'hcad_account', 'hcad_owner_full_name',
        'hcad_mailing_address', 'hcad_legal_desc_detail', 'hcad_site_address',
        'hcad_pct_ownership', 'hcad_market_value_detail', 'hcad_appraised_value_detail',
        'hcad_land_area_sf', 'hcad_total_living_area_sf', 'parsing_error',
        'hcad_search_status', 'hcad_final_tier_hit', 'is_owner_grantor',
        'is_owner_grantee', 'hcad_owner_match_type', 'needs_review_flag', 'review_reason',
        'hcad_first_page_summary_data', 'score_hcad_vs_probate', 'score_hcad_vs_rp_party',
        'score_hcad_vs_best_rp_grantee', 'score_probate_vs_rp_party',
        # Land Data Columns
        'hcad_lot_sqft_total', 'hcad_land_market_value_total', 'hcad_land_line_count', 'hcad_land_data_json',
        # Building Area Columns
        'hcad_total_base_sqft', 'hcad_total_structure_sqft', 'hcad_garage_sqft',
        'hcad_building_area_count', 'hcad_building_data_json',
        # Main Building Data Columns
        'hcad_main_building_data_json',
        # Building Characteristics
        'hcad_foundation_type', 'hcad_exterior_wall', 'hcad_heating_ac',
        'hcad_grade_adjustment', 'hcad_physical_condition',
        'hcad_full_bathrooms', 'hcad_bedrooms',
        # Main Page Values
        'hcad_land_market_value', 'hcad_improvement_market_value',
        # 5-Year History
        'hcad_appraised_history_json'
        # NEW: Add common building characteristic columns
        'hcad_foundation_type', 'hcad_exterior_wall', 'hcad_roof_type', 'hcad_heating_ac',
        'hcad_grade_adjustment', 'hcad_physical_condition', 'hcad_full_bathrooms',
        'hcad_half_bathrooms', 'hcad_bedrooms', 'hcad_stories', 'hcad_carport'
    ]

    for index, rp_row in df_processed_input.iterrows():
        case_id_for_log = rp_row.get('probate_lead_case_number', f"RowIndex_{index}")
        rp_file_for_log = rp_row.get('rp_file_number', 'N/A')
        print(f"\n--- Processing Input Row {index}: Case# {case_id_for_log} (RP File: {rp_file_for_log}) ---")

        output_row = rp_row.to_dict()
        for col in fuzzy_match_cols_to_init:
            if col not in output_row: output_row[col] = None
        # Defaults for binary flags and scores are set within the logic block later

        output_row['hcad_search_status'] = "PENDING_HCAD_SEARCH" 

        can_form_any_query_overall = False
        for tier_info_check in search_tiers_config:
            temp_legal_q_check, temp_owner_q_check = construct_search_query(rp_row, tier_info_check["name"])
            if temp_legal_q_check == "COMMON_SURNAME_TOO_BROAD": continue
            if temp_legal_q_check or temp_owner_q_check:
                can_form_any_query_overall = True; break
        
        if not can_form_any_query_overall:
            print(f"INFO: Case# {case_id_for_log} has insufficient data for any search query. Skipping HCAD search.")
            output_row['hcad_search_status'] = "SKIPPED_INSUFFICIENT_DATA"
            output_row['hcad_final_tier_hit'] = "N/A"
            output_row['hcad_owner_match_type'] = "SKIPPED_NO_QUERY_DATA"
            all_enriched_data.append(output_row); continue

        hcad_winner_detail_data = None 
        hcad_status_final_for_row = "NO_HCAD_MATCH_FOUND_ALL_TIERS" 
        succeeded_tier_name = "N/A"
        first_page_summary_if_too_many = None 

        # --- NEW: Initialize a set to track subdivisions that are too broad ---
        subdivisions_to_skip = set()

        if not _try_click_change_criteria(playwright_page, f"InitialReset_Case_{case_id_for_log}", True, True):
            print(f"ERROR: Initial page reset failed for Case# {case_id_for_log}. Skipping HCAD search for this row.")
            output_row['hcad_search_status'] = "ERROR_PAGE_RESET_FAILED"
            output_row['review_reason'] = "Initial page reset failed"; output_row['needs_review_flag'] = 1
            all_enriched_data.append(output_row); continue
        time.sleep(0.25) 

        for tier_info in search_tiers_config:
            tier_name = tier_info["name"]

            # --- NEW: Check if this tier should be skipped ---
            if tier_name in ["T4_Subdivision_Block", "T3_DropSec", "T2_ExactLegal"]:
                current_subdivision_for_check = str(rp_row.get('rp_legal_description_text', '')).upper().strip()
                if current_subdivision_for_check and current_subdivision_for_check in subdivisions_to_skip:
                    print(f"INFO: Skipping Tier '{tier_name}' because subdivision '{current_subdivision_for_check}' previously returned too many results.")
                    if hcad_status_final_for_row != "SUCCESS": hcad_status_final_for_row = "SKIPPED_DUE_TO_BROAD_SEARCH"
                    continue # Skip to the next tier

            print(f"Attempting Tier: {tier_name}")
            legal_q, owner_q = construct_search_query(rp_row, tier_name)

            if legal_q == "COMMON_SURNAME_TOO_BROAD":
                print(f"INFO (Tier {tier_name}): Skipped - Common surname with insufficient specifics.")
                if hcad_status_final_for_row != "SUCCESS": hcad_status_final_for_row = "COMMON_SURNAME_TOO_BROAD"
                time.sleep(0.2); continue
            elif legal_q is None and owner_q is None:
                print(f"INFO: Skipping tier {tier_name} - no query formed.")
                if hcad_status_final_for_row != "SUCCESS": hcad_status_final_for_row = "NO_QUERY_FORMED"
                continue

            if not _try_click_change_criteria(playwright_page, f"PreSearchReset_Tier_{tier_name}", True, True):
                 print(f"WARN: Pre-search reset for tier {tier_name} might have failed.")
            time.sleep(0.1) 

            status, data = search_hcad_and_get_results(playwright_page, tier_name, legal_q, owner_q)
            
            if hcad_status_final_for_row != "SUCCESS": hcad_status_final_for_row = status

            if status == "UNIQUE_HIT":
                found_hcad_url = data 
                parsed_acct_from_url = None
                match_acct_url = re.search(r"acct=(\d+)", found_hcad_url, re.IGNORECASE)
                if match_acct_url: parsed_acct_from_url = match_acct_url.group(1)

                if parsed_acct_from_url and parsed_acct_from_url in HCAD_DETAIL_CACHE:
                    hcad_winner_detail_data = HCAD_DETAIL_CACHE[parsed_acct_from_url]
                else:
                    hcad_winner_detail_data = parse_hcad_detail_page(playwright_page, found_hcad_url)
                    if hcad_winner_detail_data and not hcad_winner_detail_data.get('parsing_error') and hcad_winner_detail_data.get('hcad_account'):
                        HCAD_DETAIL_CACHE[hcad_winner_detail_data['hcad_account']] = hcad_winner_detail_data
                
                if hcad_winner_detail_data and hcad_winner_detail_data.get('hcad_account'):
                    succeeded_tier_name = tier_name
                    hcad_status_final_for_row = "SUCCESS"
                    if tier_name == "T0_ExactLotBlockSubdivision":
                        temp_hcad_owner = str(hcad_winner_detail_data.get('hcad_owner_full_name', '')).upper().strip()
                        temp_probate_last = str(rp_row.get('probate_lead_decedent_last', '')).upper().strip()
                        temp_rp_party_last = str(rp_row.get('cleaned_rp_party_last_name', '')).upper().strip()
                        if temp_hcad_owner and ((temp_probate_last and temp_probate_last in temp_hcad_owner) or (temp_rp_party_last and temp_rp_party_last in temp_hcad_owner)):
                            print(f"INFO: T0 success with good name signal. Skipping further tiers.")
                            break # Break ONLY if name signal is good
                        else:
                            print(f"INFO: T0 success, but name signal weak. Continuing for confirmation.")
                            hcad_status_final_for_row = "SUCCESS_T0_NEEDS_NAME_CONFIRM"
                            # DO NOT BREAK - continue to next tier
                    else:
                        # For any other successful tier, we can break immediately.
                        break
                else: 
                    hcad_status_final_for_row = "DETAIL_PARSE_FAILED"
                    break

            elif status == "SINGLE_ITEM_IN_LIST":
                single_item_summary = data[0]; found_hcad_url = single_item_summary['hcad_detail_url']
                account_summary_for_lookup = single_item_summary.get('hcad_account_summary')
                parsed_acct_from_url = None
                match_acct_url = re.search(r"acct=(\d+)", found_hcad_url, re.IGNORECASE)
                if match_acct_url: parsed_acct_from_url = match_acct_url.group(1)
                cache_key_to_check = parsed_acct_from_url if parsed_acct_from_url else account_summary_for_lookup

                if cache_key_to_check and cache_key_to_check in HCAD_DETAIL_CACHE:
                    hcad_winner_detail_data = HCAD_DETAIL_CACHE[cache_key_to_check]
                else:
                    hcad_winner_detail_data = parse_hcad_detail_page(playwright_page, found_hcad_url)
                    if hcad_winner_detail_data and not hcad_winner_detail_data.get('parsing_error') and hcad_winner_detail_data.get('hcad_account'):
                        def_key = hcad_winner_detail_data['hcad_account']
                        HCAD_DETAIL_CACHE[def_key] = hcad_winner_detail_data
                        if parsed_acct_from_url and parsed_acct_from_url != def_key and parsed_acct_from_url not in HCAD_DETAIL_CACHE : HCAD_DETAIL_CACHE[parsed_acct_from_url] = hcad_winner_detail_data
                        if account_summary_for_lookup and account_summary_for_lookup != def_key and account_summary_for_lookup not in HCAD_DETAIL_CACHE: HCAD_DETAIL_CACHE[account_summary_for_lookup] = hcad_winner_detail_data
                
                if hcad_winner_detail_data and hcad_winner_detail_data.get('hcad_account'):
                    succeeded_tier_name = tier_name; hcad_status_final_for_row = "SUCCESS" 
                    if tier_name == "T0_ExactLotBlockSubdivision":
                        temp_hcad_owner = str(hcad_winner_detail_data.get('hcad_owner_full_name', '')).upper().strip()
                        temp_probate_last = str(rp_row.get('probate_lead_decedent_last', '')).upper().strip()
                        temp_rp_party_last = str(rp_row.get('cleaned_rp_party_last_name', '')).upper().strip()
                        if temp_hcad_owner and ((temp_probate_last and temp_probate_last in temp_hcad_owner) or (temp_rp_party_last and temp_rp_party_last in temp_hcad_owner)):
                            print(f"INFO: T0 success with good name signal. Skipping further tiers.")
                            break 
                        else:
                            print(f"INFO: T0 success, but name signal weak. Continuing for confirmation.")
                            hcad_status_final_for_row = "SUCCESS_T0_NEEDS_NAME_CONFIRM"
                else: 
                    hcad_status_final_for_row = "DETAIL_PARSE_FAILED"
                break 

            elif status == "PAGINATION_TOO_LARGE":
                first_page_summary_if_too_many = data
                # --- NEW: Add the subdivision to our skip set ---
                current_subdivision = str(rp_row.get('rp_legal_description_text', '')).upper().strip()
                if current_subdivision and current_subdivision != 'NAN':
                    print(f"INFO: Tier '{tier_name}' for subdivision '{current_subdivision}' was too broad. Flagging it to skip subsequent broader searches.")
                    subdivisions_to_skip.add(current_subdivision)
            
            elif status == "MULTIPLE_HITS":
                current_confidence = str(rp_row.get('match_confidence_level', 'Unknown')).strip()
                winner_from_multiple = choose_best_from_multiple(data, rp_row, tier_name, playwright_page, current_confidence)
                if winner_from_multiple and winner_from_multiple.get('hcad_account'): 
                    hcad_winner_detail_data = winner_from_multiple 
                    succeeded_tier_name = tier_name; hcad_status_final_for_row = "SUCCESS" 
                    print(f"INFO: Winner selected by {tier_name}: Acct {hcad_winner_detail_data.get('hcad_account')}")
                    break 
                else:
                    hcad_status_final_for_row = f"MULTIPLE_HITS_NO_WINNER_TIER_{tier_name}"
            
            elif status == "ERROR":
                hcad_status_final_for_row = f"ERROR_IN_TIER_{tier_name}"; break 
            
            elif status in ("NO_HITS", "AMBIGUOUS_COUNT", "NO_HITS_OR_UNKNOWN_PAGE"):
                pass # Status already updated, continue to next tier
            
            time.sleep(0.75) 

        output_row['hcad_search_status'] = hcad_status_final_for_row
        if hcad_status_final_for_row == "SUCCESS" and hcad_winner_detail_data:
            output_row.update(hcad_winner_detail_data) 
            output_row['hcad_final_tier_hit'] = succeeded_tier_name
        elif hcad_winner_detail_data and hcad_winner_detail_data.get('parsing_error'): 
            output_row['hcad_search_status'] = "DETAIL_PARSE_ERROR" 
            output_row['parsing_error'] = hcad_winner_detail_data.get('parsing_error')
            output_row['hcad_final_tier_hit'] = succeeded_tier_name 
            output_row.update(hcad_winner_detail_data)
        elif hcad_status_final_for_row == "PAGINATION_TOO_LARGE" and first_page_summary_if_too_many:
            output_row['hcad_first_page_summary_data'] = str(first_page_summary_if_too_many[:3]) 
            output_row['hcad_final_tier_hit'] = succeeded_tier_name # Tier that hit pagination
        elif hcad_status_final_for_row == "SUCCESS_T0_NEEDS_NAME_CONFIRM" and hcad_winner_detail_data: # Handle new T0 status
            output_row.update(hcad_winner_detail_data)
            output_row['hcad_final_tier_hit'] = "T0_ExactLotBlockSubdivision" # Explicitly set T0
        else: 
            output_row['hcad_final_tier_hit'] = "N/A" 

        # --- Enhanced Owner Match Typing with Raw Fuzzy Scores ---
        output_row['score_hcad_vs_probate'] = 0; output_row['score_hcad_vs_rp_party'] = 0
        output_row['score_hcad_vs_best_rp_grantee'] = 0
        output_row['score_probate_vs_rp_party'] = 0
        output_row['is_owner_grantor'] = 0; output_row['is_owner_grantee'] = 0

        if output_row.get('hcad_search_status') in ["SUCCESS", "SUCCESS_T0_NEEDS_NAME_CONFIRM"] and output_row.get('hcad_owner_full_name'):
            hcad_owner_full_str = str(output_row.get('hcad_owner_full_name', "")).upper().strip()

            # --- Smarter Last Name Guess for HCAD names ---
            hcad_owner_last_guess = ""
            company_indicators = {"LLC", "INC", "LP", "LTD", "CO", "BANK", "TRUST", "ESTATE", "EST"}
            hcad_name_parts = hcad_owner_full_str.split()
            is_company = any(part in company_indicators for part in hcad_name_parts)

            if not is_company and hcad_name_parts:
                hcad_owner_last_guess = hcad_name_parts[0] # Assume first word is last name
            else:
                hcad_owner_last_guess = _extract_potential_last_name(hcad_owner_full_str)
            # --- End of smarter guess ---

            probate_dec_first = str(rp_row.get('probate_lead_decedent_first', "")).upper().strip()
            probate_dec_last = str(rp_row.get('probate_lead_decedent_last', "")).upper().strip()
            probate_decedent_full_name = f"{probate_dec_first} {probate_dec_last}".strip()
            if not probate_decedent_full_name.strip(): probate_decedent_full_name = None

            rp_party_first = str(rp_row.get('cleaned_rp_party_first_name', "")).upper().strip()
            rp_party_last = str(rp_row.get('cleaned_rp_party_last_name', "")).upper().strip()
            rp_party_full_name = f"{rp_party_first} {rp_party_last}".strip()
            if not rp_party_full_name.strip(): rp_party_full_name = None

            match_threshold = 80 # Using the more forgiving threshold

            if probate_decedent_full_name and hcad_owner_full_str:
                probate_dec_last_comp = _extract_potential_last_name(probate_decedent_full_name)
                last_name_score_probate = fuzz.token_set_ratio(probate_dec_last_comp, hcad_owner_last_guess) if probate_dec_last_comp and hcad_owner_last_guess else 0
                full_name_score_probate = fuzz.token_set_ratio(probate_decedent_full_name, hcad_owner_full_str)
                # Using the 30/70 weights that favor the more robust full name match
                output_row['score_hcad_vs_probate'] = round((last_name_score_probate * 0.3) + (full_name_score_probate * 0.7))

            if rp_party_full_name and hcad_owner_full_str:
                rp_party_last_comp = _extract_potential_last_name(rp_party_full_name)
                last_name_score_rp_party = fuzz.token_set_ratio(rp_party_last_comp, hcad_owner_last_guess) if rp_party_last_comp and hcad_owner_last_guess else 0
                full_name_score_rp_party = fuzz.token_set_ratio(rp_party_full_name, hcad_owner_full_str)
                # Applying the 30/70 weights here as well
                output_row['score_hcad_vs_rp_party'] = round((last_name_score_rp_party * 0.3) + (full_name_score_rp_party * 0.7))

            # (The rest of the owner matching logic continues from here...)

            if probate_decedent_full_name and rp_party_full_name:
                output_row['score_probate_vs_rp_party'] = fuzz.token_set_ratio(probate_decedent_full_name, rp_party_full_name)

            rp_grantee_list = rp_row.get('rp_grantee_full_names_list', [])
            if not isinstance(rp_grantee_list, list): rp_grantee_list = []
            current_best_grantee_score = 0
            for grantee_obj in rp_grantee_list:
                grantee_full_name = str(grantee_obj).upper().strip()
                if grantee_full_name and hcad_owner_full_str:
                    grantee_last_guess = _extract_potential_last_name(grantee_full_name)
                    last_name_score_grantee = fuzz.token_set_ratio(grantee_last_guess, hcad_owner_last_guess) if grantee_last_guess and hcad_owner_last_guess else 0
                    full_name_score_grantee = fuzz.token_set_ratio(grantee_full_name, hcad_owner_full_str)
                    combined_score = round((last_name_score_grantee * 0.6) + (full_name_score_grantee * 0.4))
                    if combined_score > current_best_grantee_score: current_best_grantee_score = combined_score
            output_row['score_hcad_vs_best_rp_grantee'] = current_best_grantee_score
            
            probate_matches_hcad_flag = output_row['score_hcad_vs_probate'] >= match_threshold
            rp_party_matches_hcad_flag = output_row['score_hcad_vs_rp_party'] >= match_threshold
            grantee_matches_hcad_flag = output_row['score_hcad_vs_best_rp_grantee'] >= match_threshold
            probate_is_rp_party_flag = output_row['score_probate_vs_rp_party'] >= match_threshold
            
            if not rp_party_full_name and probate_decedent_full_name:
                rp_party_matches_hcad_flag = probate_matches_hcad_flag 
                probate_is_rp_party_flag = True 

            if probate_matches_hcad_flag:
                if probate_is_rp_party_flag: output_row['hcad_owner_match_type'] = "MATCH_PROBATE_DECEDENT_AS_RP_PARTY"
                else: output_row['hcad_owner_match_type'] = "MATCH_PROBATE_DECEDENT_RP_PARTY_DIFFERED"
            elif rp_party_matches_hcad_flag: output_row['hcad_owner_match_type'] = "MATCH_RP_PARTY_PROBATE_DEVIATED"
            elif grantee_matches_hcad_flag: output_row['hcad_owner_match_type'] = "MATCH_RP_GRANTEE"
            else: output_row['hcad_owner_match_type'] = "HCAD_OWNER_IS_UNRELATED_THIRD_PARTY"

            if output_row['hcad_owner_match_type'] in ["MATCH_PROBATE_DECEDENT_AS_RP_PARTY", "MATCH_PROBATE_DECEDENT_RP_PARTY_DIFFERED","MATCH_RP_PARTY_PROBATE_DEVIATED"]:
                output_row['is_owner_grantor'] = 1
            if output_row['hcad_owner_match_type'] == "MATCH_RP_GRANTEE":
                output_row['is_owner_grantee'] = 1
        
        elif output_row.get('hcad_search_status') in ["SUCCESS", "SUCCESS_T0_NEEDS_NAME_CONFIRM"]: 
            output_row['hcad_owner_match_type'] = "HCAD_OWNER_NAME_MISSING"
        else: 
            output_row['hcad_owner_match_type'] = str(output_row.get('hcad_search_status'))
        
        # --- Refined Needs Review Flag Logic ---
        current_hcad_status_for_review = output_row.get('hcad_search_status', '')
        current_owner_match_type = output_row.get('hcad_owner_match_type', '') 
        output_row['needs_review_flag'] = 0 
        output_row['review_reason'] = None 
        
        if current_hcad_status_for_review not in ["SUCCESS", "SKIPPED_INSUFFICIENT_DATA", "NO_HITS", "COMMON_SURNAME_TOO_BROAD", "NO_QUERY_FORMED"]: # If not success or a known "benign" non-success
            output_row['needs_review_flag'] = 1
            output_row['review_reason'] = f"HCAD Search Status: {current_hcad_status_for_review}" # This includes SUCCESS_T0_NEEDS_NAME_CONFIRM
        elif output_row.get('parsing_error') is not None:
            output_row['needs_review_flag'] = 1
            output_row['review_reason'] = f"HCAD Detail Page Parsing Error: {output_row.get('parsing_error')}"
        elif current_owner_match_type == "MATCH_RP_PARTY_PROBATE_DEVIATED":
            output_row['needs_review_flag'] = 1
            output_row['review_reason'] = "Review: HCAD Owner matches RP Party, which differs from Probate Lead"
        elif current_owner_match_type == "MATCH_PROBATE_DECEDENT_RP_PARTY_DIFFERED":
            output_row['needs_review_flag'] = 1
            output_row['review_reason'] = "Review: HCAD Owner matches Probate Lead, but RP Party was different"
        elif current_owner_match_type == "HCAD_OWNER_IS_UNRELATED_THIRD_PARTY":
            output_row['needs_review_flag'] = 1
            output_row['review_reason'] = "Review: HCAD Owner appears to be an unrelated third party"
        elif current_owner_match_type == "MATCH_RP_GRANTEE": 
            probate_matches_this_grantee = False
            if probate_decedent_full_name and output_row.get('hcad_owner_full_name'): 
                if fuzz.token_set_ratio(probate_decedent_full_name, output_row['hcad_owner_full_name']) >= match_threshold:
                    probate_matches_this_grantee = True
            if not probate_matches_this_grantee: 
                output_row['needs_review_flag'] = 1
                output_row['review_reason'] = "Review: HCAD Owner matches RP Grantee (who is not Probate Lead)"
        elif current_owner_match_type == "HCAD_OWNER_NAME_MISSING": 
            output_row['needs_review_flag'] = 1
            output_row['review_reason'] = "HCAD Owner Name Missing After Successful Scrape"
        
        if output_row['needs_review_flag'] == 1 and (output_row.get('review_reason') is None or output_row.get('review_reason') == ''):
            output_row['review_reason'] = "General Review Needed due to search outcome or complex match type"

        all_enriched_data.append(output_row)
        print(f"--- Completed: Case# {case_id_for_log}. Final Status: {output_row['hcad_search_status']}, MatchType: {output_row['hcad_owner_match_type']}, Review: {output_row['needs_review_flag']}, Reason: {output_row['review_reason']} ---")
        time.sleep(1.0) 

    return pd.DataFrame(all_enriched_data)

# The if __name__ == '__main__': block should be the one that loads your
# QA SAMPLE CSV, preprocesses it, and then filters for 'High' confidence records,
# removing the .head(1) and specific rp_file_number filter for this broader QA run.

# --- Main Execution Block (`if __name__ == '__main__':`) ---
# This should be the version that loads your QA SAMPLE CSV,
# performs the preprocessing we validated,
# and then filters for 'High' confidence records to pass to main_hcad_processing_loop.
# Crucially, it should NOT have the .head(1) or the specific rp_file_number filter anymore
# for this next phase of testing.
if __name__ == '__main__':
    # --- Dynamic Input File Logic (remains the same from last time) ---
    INPUT_DATA_FOLDER = "/Users/ayoodukale/Documents/Inherra/Python/Inherra scraper/Script3_Linked_Results"
    TARGET_INPUT_CSV_PATH = None
    full_df_s3_data = None
    try:
        all_files = glob.glob(os.path.join(INPUT_DATA_FOLDER, '*.csv'))
        candidate_files = [f for f in all_files if not f.endswith('_QA_SAMPLE.csv')]
        if candidate_files:
            TARGET_INPUT_CSV_PATH = max(candidate_files, key=os.path.getmtime)
            print(f"INFO: Found {len(candidate_files)} potential input files. Using the latest one:")
            print(f"INFO: -> {TARGET_INPUT_CSV_PATH}")
        else:
            print(f"ERROR: No suitable input files found in '{INPUT_DATA_FOLDER}'.")
    except Exception as e:
        print(f"ERROR: An error occurred while trying to find the latest input file: {e}")

    if TARGET_INPUT_CSV_PATH and os.path.exists(TARGET_INPUT_CSV_PATH):
        try:
            full_df_s3_data = pd.read_csv(TARGET_INPUT_CSV_PATH, delimiter=';')
            print(f"INFO: Successfully loaded {len(full_df_s3_data)} records from '{TARGET_INPUT_CSV_PATH}'")
        except Exception as e:
            print(f"ERROR: Could not read the identified CSV file '{TARGET_INPUT_CSV_PATH}': {e}")
            import traceback; traceback.print_exc()
    else:
        print(f"ERROR: Input file could not be loaded. Path was '{TARGET_INPUT_CSV_PATH}'.")

    if full_df_s3_data is None or full_df_s3_data.empty:
        print("WARN: Could not load data from file. Using minimal internal sample for structure.")
        sample_data_with_confidence = [
            {'probate_lead_case_number': 'S1_Case', 'rp_file_number':'FILE001',
             'rp_party_type':'Grantor', 'is_potential_decedent_match': True,
             'rp_party_first_name':'JANE_DEC','rp_party_last_name':'DOE_DEC', 'match_confidence_level':'High', 
             'rp_legal_description_text': 'TRINITY GARDENS', 'rp_legal_block': '4', 'rp_legal_tract':'77C', 
             'rp_legal_sec': '5', 'rp_legal_lot': '1', 'probate_lead_decedent_first':'JANE_DEC',
             'probate_lead_decedent_last':'DOE_DEC',
             'cleaned_rp_party_first_name': 'JANE_DEC', 'cleaned_rp_party_last_name': 'DOE_DEC'},
            {'probate_lead_case_number': 'S1_Case', 'rp_file_number':'FILE001',
             'rp_party_type':'Grantee', 'is_potential_decedent_match':False,
             'rp_party_first_name':'PETER','rp_party_last_name': 'SMITH', 'cleaned_rp_party_first_name':'PETER',
             'cleaned_rp_party_last_name': 'SMITH'},
        ]
        full_df_s3_data = pd.DataFrame(sample_data_with_confidence)
        print(f"INFO: Using internal sample data with {len(full_df_s3_data)} records.")

    # --- Preprocessing Logic (remains the same) ---
    df_for_hcad_processing = pd.DataFrame()
    if full_df_s3_data is not None and not full_df_s3_data.empty:
        #... (The entire preprocessing section that creates df_for_hcad_processing is unchanged)
        print("INFO: Starting preprocessing of loaded data...")
        party_first_name_col = 'rp_party_first_name'
        party_last_name_col = 'rp_party_last_name'
        essential_cols = ['rp_file_number', 'rp_party_type', party_first_name_col,
                          party_last_name_col, 'is_potential_decedent_match', 'probate_lead_case_number',
                          'match_confidence_level']
        missing_essential = [col for col in essential_cols if col not in full_df_s3_data.columns]
        if missing_essential:
            print(f"ERROR: Essential columns for processing are missing from input: {missing_essential}.")
            df_for_hcad_processing = full_df_s3_data.copy() 
            if 'rp_grantee_full_names_list' not in df_for_hcad_processing.columns:
                df_for_hcad_processing['rp_grantee_full_names_list'] = pd.Series([[] for _ in range(len(df_for_hcad_processing))], index=df_for_hcad_processing.index)
        else:
            full_df_s3_data['rp_party_full_name'] = (
                full_df_s3_data[party_first_name_col].fillna("").astype(str) + " " +
                full_df_s3_data[party_last_name_col].fillna("").astype(str)
            ).str.strip().replace("", None)
            primary_transaction_rows = full_df_s3_data[
                (full_df_s3_data['is_potential_decedent_match'] == True) &
                (full_df_s3_data['rp_party_type'] == 'Grantor')
            ].copy()
            primary_transaction_rows.drop_duplicates(subset=['probate_lead_case_number', 'rp_file_number'], keep='first', inplace=True)
            if not primary_transaction_rows.empty:
                grantees_df = full_df_s3_data[full_df_s3_data['rp_party_type'] == 'Grantee'].copy()
                if not grantees_df.empty and 'rp_party_full_name' in grantees_df.columns:
                    grantee_groups = grantees_df.groupby('rp_file_number')['rp_party_full_name'].apply(
                        lambda x: [name for name in x.tolist() if pd.notna(name) and name.strip()]
                    ).reset_index(name='rp_grantee_full_names_list')
                    df_for_hcad_processing = pd.merge(primary_transaction_rows, grantee_groups, on='rp_file_number', how='left')
                    df_for_hcad_processing['rp_grantee_full_names_list'] = df_for_hcad_processing['rp_grantee_full_names_list'].apply(lambda x: x if isinstance(x, list) else [])
                else:
                    df_for_hcad_processing = primary_transaction_rows.copy()
                    df_for_hcad_processing['rp_grantee_full_names_list'] = pd.Series([[] for _ in range(len(df_for_hcad_processing))], index=df_for_hcad_processing.index)
                print(f"INFO: Preprocessing complete. {len(df_for_hcad_processing)} primary transaction rows prepared.")
            else: print("WARN: No primary decedent-as-grantor rows found.")
    else: print("CRITICAL ERROR: No input data loaded. Cannot preprocess.")


    if df_for_hcad_processing.empty:
        print("INFO: No data to process after preprocessing. Exiting.")
    else:
        # --- MODIFIED: Filtering logic is changed here ---
        # Instead of filtering by 'match_confidence_level', we now filter by 'is_potential_decedent_match'.
        if 'is_potential_decedent_match' not in df_for_hcad_processing.columns:
            print("ERROR: Critical column 'is_potential_decedent_match' not found in the data. Cannot proceed.")
            df_to_process_final = pd.DataFrame() # Create empty dataframe to prevent error
        else:
            # The preprocessing step already selects for `is_potential_decedent_match == True`,
            # so we can use the dataframe directly. This code confirms the filtering.
            df_to_process_final = df_for_hcad_processing[df_for_hcad_processing['is_potential_decedent_match'] == True].copy()
        # --- END OF MODIFICATION ---

        if not df_to_process_final.empty:
            # --- MODIFIED: Updated print statement for clarity ---
            print(f"INFO: Processing {len(df_to_process_final)} records where 'is_potential_decedent_match' is True.")
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, slow_mo=100)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
                    viewport={'width': 1280, 'height': 1024}, locale='en-US',
                    timezone_id='America/Chicago', java_script_enabled=True,
                )
                page = context.new_page()
                try:
                    enriched_df = main_hcad_processing_loop(df_to_process_final, page)
                    print("\n\n--- ENRICHED DATA (From 'is_potential_decedent_match' Filter) ---")
                    if not enriched_df.empty:
                        # ... (display and output logic) ...
                        output_folder = "HCAD_Enrichment_Extractions"
                        if not os.path.exists(output_folder):
                           os.makedirs(output_folder)
                        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                        output_filename = f"script4_hcad_enriched_{timestamp}.csv"
                        output_filepath = os.path.join(output_folder, output_filename)
                        enriched_df.to_csv(output_filepath, index=False)
                        print(f"\nINFO: Enriched data saved to: {output_filepath}")
                except Exception as main_e:
                    print(f"FATAL ERROR: {main_e}");
                    import traceback; traceback.print_exc()
                finally:
                    print("Processing finished.");
                    if os.name != 'posix': input("Press Enter to close browser...")
                    browser.close()
        else:
            # --- MODIFIED: Updated print statement for clarity ---
            print("INFO: No records found where 'is_potential_decedent_match' is True. Exiting.")

#