# -*- coding: utf-8 -*-
"""
production_enrichment_script_final_corrected_v2.py

This is the final, production-ready script for the end-to-end enrichment pipeline.

CORRECTION: This version incorporates the user's final adjustments to the 6-zone
story arc for column ordering.
"""

import os
import glob
import pandas as pd
from playwright.sync_api import sync_playwright
import time
import datetime
import re
import numpy as np
import requests

# --- Configuration ---

# --- ACTION REQUIRED: Replace with your actual Apify credentials ---
APIFY_API_TOKEN = "YOUR_APIFY_API_TOKEN"  # Replace with your token
APIFY_TASK_ID = "your-task-id"           # Replace with the ID of your Apify task/actor

HCTAX_SEARCH_URL = "https://www.hctax.net/Property/PropertyTax"
INPUT_DATA_FOLDER = "/Users/ayoodukale/Documents/Inherra/Python/Inherra scraper/HCAD_Enrichment_Extractions"
OUTPUT_FOLDER = "HCAD Tax Enrichment"

# --- Helper Functions (unchanged) ---
def clean_value(value_str):
    if value_str is None or not isinstance(value_str, str): return None
    match = re.search(r'(-?[\d,]*\.?\d+)', value_str)
    if match:
        numeric_string = match.group(1).replace(',', '')
        try: return float(numeric_string)
        except ValueError: return None
    return None

def get_latest_input_file(folder_path):
    try:
        all_files = glob.glob(os.path.join(folder_path, '*.csv'))
        if not all_files: return None
        return max(all_files, key=os.path.getmtime)
    except Exception as e:
        print(f"ERROR: An error occurred while finding the latest input file: {e}")
        return None

def is_human_name(name_str):
    if not isinstance(name_str, str) or not name_str.strip(): return False
    entity_keywords = ['LLC', 'INC', 'LP', 'LTD', 'CORP', 'CO ', 'TRUST', 'BANK', 'ESTATE', 'EST ', 'PROPERTIES', 'INVESTMENTS', 'FUND', 'GROUP', 'REALTY', 'HOLDINGS', 'ASSOCIATION', 'ASSN', 'VENTURES', 'LLP']
    upper_name = name_str.upper()
    if any(re.search(r'\b' + keyword + r'\b', upper_name) for keyword in entity_keywords):
        return False
    return True

def determine_owner_contact(row):
    hcad_name = row.get('hcad_owner_full_name', '')
    hcad_match_type = row.get('hcad_owner_match_type', '')
    rp_party_name = row.get('rp_party_full_name', '')
    rp_party_type = row.get('rp_party_type', '')
    is_hcad_human = is_human_name(hcad_name)
    is_rp_human = is_human_name(rp_party_name)
    
    if is_hcad_human and hcad_match_type in ['MATCH_PROBATE_DECEDENT_AS_RP_PARTY', 'MATCH_PROBATE_DECEDENT_RP_PARTY_DIFFERED']:
        return pd.Series([hcad_name, 'MATCHED_HCAD_DECEDENT', 'A'])
    if is_hcad_human and hcad_match_type in ['MATCH_RP_PARTY_PROBATE_DEVIATED', 'MATCH_RP_GRANTEE'] and rp_party_type == 'Grantor' and is_rp_human:
        return pd.Series([rp_party_name, 'FALLBACK_RP_GRANTOR', 'B'])
    if not is_hcad_human and rp_party_type == 'Grantor' and is_rp_human:
        return pd.Series([rp_party_name, 'FALLBACK_RP_GRANTOR_VS_ENTITY', 'B'])
    
    rationale = "ENTITY_SUPPRESS"
    if is_hcad_human:
        rationale = "MANUAL_REVIEW_UNMATCHED_HUMAN"
        return pd.Series([hcad_name, rationale, 'C'])
    return pd.Series([np.nan, rationale, 'DROP'])

def enrich_with_apify(contact_name, site_address):
    print(f"INFO: [Apify] Enriching contact for: {contact_name}")
    if not APIFY_API_TOKEN or "YOUR_APIFY_API_TOKEN" in APIFY_API_TOKEN:
        print("WARN: [Apify] API token is a placeholder. Skipping enrichment.")
        return {'contact_phone': None, 'contact_email': None}
    headers = {"Authorization": f"Bearer {APIFY_API_TOKEN}", "Content-Type": "application/json"}
    payload = {"searches": [{"fullName": contact_name, "address": site_address}]}
    try:
        response = requests.post(f"https://api.apify.com/v2/actor-tasks/{APIFY_TASK_ID}/run-sync-get-dataset-items", headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        data = response.json()
        phone = data[0].get('phoneNumbers', [{}])[0].get('number') if data and data[0].get('phoneNumbers') else None
        email = data[0].get('emails', [{}])[0].get('address') if data and data[0].get('emails') else None
        print(f"INFO: [Apify] Success. Phone: {phone}, Email: {email}")
        return {'contact_phone': phone, 'contact_email': email}
    except requests.exceptions.RequestException as e:
        print(f"ERROR: [Apify] API call failed: {e}")
        return {'contact_phone': None, 'contact_email': None}

def scrape_hctax_for_account(page, hcad_account_number):
    scraped_data = {}
    try:
        print(f"\n--- Scraping for Account: {hcad_account_number} ---")
        search_field_xpath = '//*[@id="txtSearchValue"]'
        search_button_xpath = '//*[@id="btnSubmitTaxSearch"]'

        if "PropertyTax" not in page.url:
            page.goto(HCTAX_SEARCH_URL, timeout=60000, wait_until="networkidle")
        
        if "Statement" in page.url:
             search_button_xpath = '//*[@id="btnSubmitTaxSearchStatement"]'

        page.locator(search_field_xpath).wait_for(state='visible', timeout=15000)
        page.locator(search_field_xpath).fill(str(hcad_account_number))
        page.locator(search_button_xpath).click()
        
        result_link_xpath = f'//a[contains(text(), "{hcad_account_number}")]'
        try:
            page.locator(result_link_xpath).wait_for(state='visible', timeout=20000)
            page.locator(result_link_xpath).click()
            page.wait_for_load_state("networkidle", timeout=30000)
        except Exception:
            if page.locator('//*[@id="CurrentStatement"]').count() == 0:
                print(f"ERROR: Failed to navigate to details page for account {hcad_account_number}. Skipping.")
                return {'error': 'Failed to find or navigate to account details page.'}

        def get_text(xpath):
            try:
                element = page.locator(f"xpath={xpath}")
                if element.count() > 0: return element.first.inner_text().strip()
            except Exception: return None
            return None

        def get_concatenated_text(xpath_list):
            parts = []
            for xpath in xpath_list:
                try:
                    script = f"document.evaluate('{xpath}', document, null, XPathResult.STRING_TYPE, null).stringValue;"
                    text_content = page.evaluate(script)
                    if text_content: parts.append(text_content.strip())
                except Exception: pass
            return " ".join(parts) if parts else None

        scraped_data['hctax_account'] = get_text('//*[@id="CurrentStatement"]/table[1]/tbody/tr[2]/td[1]/b')
        scraped_data['account_status_text'] = get_text('//*[@id="CurrentStatement"]/span/strong')
        scraped_data['statement_date'] = get_text('//*[@id="CurrentStatement"]/table[1]/tbody/tr[2]/td[2]')
        
        owner_full_name = get_concatenated_text(['//*[@id="CurrentStatement"]/table[1]/tbody/tr[2]/td[3]/text()[1]'])
        owner_address_line1 = get_concatenated_text(['//*[@id="CurrentStatement"]/table[1]/tbody/tr[2]/td[3]/text()[2]'])
        owner_city_state_zip = get_concatenated_text(['//*[@id="CurrentStatement"]/table[1]/tbody/tr[2]/td[3]/text()[3]'])
        scraped_data['hctax_owner_full_name'] = owner_full_name
        scraped_data['hctax_owner_mailing_address'] = f"{owner_address_line1} {owner_city_state_zip}".strip() if owner_address_line1 and owner_city_state_zip else owner_address_line1
        
        site_address = get_concatenated_text(['//*[@id="CurrentStatement"]/table[2]/tbody/tr[2]/td[1]/text()[1]'])
        legal_desc_part1 = get_concatenated_text(['//*[@id="CurrentStatement"]/table[2]/tbody/tr[2]/td[1]/text()[2]'])
        legal_desc_part2 = get_concatenated_text(['//*[@id="CurrentStatement"]/table[2]/tbody/tr[2]/td[1]/text()[3]'])
        scraped_data['hctax_site_address'] = site_address
        scraped_data['hctax_legal_desc_full'] = f"{legal_desc_part1} {legal_desc_part2}".strip() if legal_desc_part1 and legal_desc_part2 else legal_desc_part1

        scraped_data['hctax_land_market_value'] = clean_value(get_text('//*[@id="CurrentStatement"]/table[2]/tbody/tr[2]/td[2]/table/tbody/tr[1]/td[2]'))
        scraped_data['hctax_improvements_market_value'] = clean_value(get_text('//*[@id="CurrentStatement"]/table[2]/tbody/tr[2]/td[2]/table/tbody/tr[2]/td[2]'))
        scraped_data['hctax_total_market_value'] = clean_value(get_text('//*[@id="CurrentStatement"]/table[2]/tbody/tr[2]/td[2]/table/tbody/tr[3]/td[2]'))
        scraped_data['hctax_appraised_value'] = clean_value(get_text('//*[@id="CurrentStatement"]/table[2]/tbody/tr[2]/td[2]/table/tbody/tr[5]/td[2]'))
        
        scraped_data['exemption_code'] = get_text('//*[@id="CurrentStatement"]/table[2]/tbody/tr[2]/td[3]')
        scraped_data['total_current_taxes_due'] = clean_value(get_text('//*[@id="CurrentStatement"]/table[4]/tbody/tr[3]/td[2]'))
        scraped_data['prior_years_taxes_due'] = clean_value(get_text('//*[@id="CurrentStatement"]/table[4]/tbody/tr[4]/td[2]'))
        scraped_data['taxes_due_by_jan31'] = clean_value(get_text('//*[@id="CurrentStatement"]/table[4]/tbody/tr[1]/td[2]'))

        print(f"SUCCESS: Finished HCTAX scrape for account {hcad_account_number}.")
    except Exception as e:
        scraped_data['error'] = str(e)
        print(f"ERROR: A failure occurred during HCTAX scrape for {hcad_account_number}: {e}")
    return scraped_data

# --- Main Execution Block ---
if __name__ == "__main__":
    print("--- Starting Production Enrichment Process (Final Version) ---")
    latest_input_csv = get_latest_input_file(INPUT_DATA_FOLDER)
    if not latest_input_csv: exit("CRITICAL: No input file found. Exiting.")
    
    try:
        input_df = pd.read_csv(latest_input_csv, dtype=str, low_memory=False)
        if 'hcad_account' in input_df.columns:
            input_df['account_to_search'] = input_df['hcad_account'].fillna(input_df.get('hcad_account_summary', ''))
        elif 'hcad_account_summary' in input_df.columns:
            input_df['account_to_search'] = input_df['hcad_account_summary']
        else:
            exit("CRITICAL: No HCAD account column found. Exiting.")
        input_df.dropna(subset=['account_to_search'], inplace=True)
        input_df = input_df[input_df['account_to_search'] != ''].copy()
        print(f"INFO: Successfully loaded {len(input_df)} records.")
    except Exception as e:
        exit(f"CRITICAL: Failed to read or process CSV file. Error: {e}")

    all_enriched_records = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=50)
        page = browser.new_page()
        for index, row in input_df.iterrows():
            hctax_data = scrape_hctax_for_account(page, row['account_to_search'])
            enriched_row = {**row.to_dict(), **hctax_data}
            all_enriched_records.append(enriched_row)
        browser.close()

    enriched_df = pd.DataFrame(all_enriched_records)

    if not enriched_df.empty:
        print("\nINFO: Applying contact targeting logic...")
        enriched_df['probate_name'] = enriched_df['probate_lead_decedent_first'].fillna('') + ' ' + enriched_df['probate_lead_decedent_last'].fillna('')
        enriched_df['probate_name'] = enriched_df['probate_name'].str.strip()
        
        contact_cols = enriched_df.apply(determine_owner_contact, axis=1)
        contact_cols.columns = ['owner_contact_name', 'owner_contact_rationale', 'contact_target_tier']
        enriched_df = pd.concat([enriched_df, contact_cols], axis=1)

        print("INFO: Beginning contact enrichment via Apify for Tier A & B leads...")
        contact_details = []
        for index, row in enriched_df.iterrows():
            if row['contact_target_tier'] in ['A', 'B'] and pd.notna(row['owner_contact_name']):
                time.sleep(1) 
                address = row.get('hctax_site_address') or row.get('hcad_site_address', '')
                contact_info = enrich_with_apify(row['owner_contact_name'], address)
                contact_details.append(contact_info)
            else:
                contact_details.append({'contact_phone': None, 'contact_email': None})
        enriched_df = pd.concat([enriched_df, pd.DataFrame(contact_details, index=enriched_df.index)], axis=1)

        print("INFO: Reordering columns into the specified 6-Zone 'story arc'.")
        
        # --- MODIFIED: Final Story Arc zones updated as per your request ---
        story_arc_zones = {
            "Zone 1: Lead Triage": ['contact_target_tier', 'owner_contact_rationale', 'match_confidence_level', 'needs_review_flag', 'review_reason', 'match_score_total'],
            "Zone 2: Core People & Property": ['owner_contact_name', 'probate_name', 'hcad_owner_full_name', 'hctax_owner_full_name', 'rp_party_full_name', 'hctax_site_address', 'hcad_site_address', 'hctax_legal_desc_full', 'hcad_legal_desc_detail'],
            "Zone 3: Enriched Contact": ['contact_phone', 'contact_email', 'hctax_owner_mailing_address', 'hcad_mailing_address'],
            "Zone 4: Financial & Tax Signals": ['total_current_taxes_due', 'prior_years_taxes_due', 'account_status_text', 'hctax_total_market_value', 'hcad_market_value_detail', 'hctax_appraised_value', 'hcad_appraised_value_detail', 'hcad_appraised_history_json', 'account_to_search'],
            "Zone 5: Detailed Property DNA": ['hcad_bedrooms', 'hcad_full_bathrooms', 'hcad_half_bathrooms', 'hcad_total_living_area_sf', 'hcad_total_base_sqft', 'hcad_lot_sqft_total', 'hcad_physical_condition', 'hcad_foundation_type', 'hcad_exterior_wall', 'hcad_roof_type', 'hcad_heating_ac', 'exemption_code', 'hcad_building_data_json', 'hcad_land_data_json', 'hcad_land_market_value_total', 'hcad_land_line_count', 'hcad_total_structure_sqft', 'hcad_garage_sqft', 'hcad_building_area_count', 'hcad_grade_adjustment', 'hcad_land_market_value', 'hcad_improvement_market_value', 'hcad_cond___desir___util', 'hcad_room_total', 'hcad_room_rec', 'hcad_room_half_bath', 'hcad_room_full_bath', 'hcad_room_bedroom', 'hcad_fireplace_masonry_firebrick', 'hcad_fireplace_metal_prefab', 'hcad_cost_and_design'],
            "Zone 6: Provenance & No Man's Land": ['probate_lead_case_number', 'rp_file_number', 'hcad_account', 'hctax_account', 'hcad_owner_match_type', 'hcad_search_status', 'score_hcad_vs_probate', 'score_hcad_vs_rp_party', 'hcad_account_summary', 'hcad_owner_summary', 'hcad_address_summary']
        }
        
        final_column_order = []
        for zone, columns in story_arc_zones.items():
            final_column_order.extend(columns)
        
        existing_columns = enriched_df.columns.tolist()
        ordered_existing_columns = [col for col in final_column_order if col in existing_columns]
        other_columns = [col for col in existing_columns if col not in ordered_existing_columns]
        final_ordered_list = ordered_existing_columns + other_columns
        
        enriched_df = enriched_df[final_ordered_list]
        
        if not os.path.exists(OUTPUT_FOLDER): os.makedirs(OUTPUT_FOLDER)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        csv_path = os.path.join(OUTPUT_FOLDER, f"production_enriched_{timestamp}.csv")
        enriched_df.to_csv(csv_path, index=False)
        print(f"\nSUCCESS: Clean CSV for systems saved to: {csv_path}")
        
        excel_path = os.path.join(OUTPUT_FOLDER, f"production_enriched_{timestamp}.xlsx")
        try:
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                enriched_df.to_excel(writer, index=False, sheet_name='Enriched Leads', startrow=1, header=False)
                workbook = writer.book
                worksheet = writer.sheets['Enriched Leads']
                header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'fg_color': '#F2F2F2', 'border': 1, 'align': 'center'})
                zone_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'fg_color': '#D9E1F2', 'border': 1, 'align': 'center'})
                current_col = 0
                for zone_name, columns in story_arc_zones.items():
                    cols_in_zone = [col for col in columns if col in enriched_df.columns]
                    if len(cols_in_zone) > 0:
                        worksheet.merge_range(0, current_col, 0, current_col + len(cols_in_zone) - 1, zone_name, zone_format)
                        current_col += len(cols_in_zone)
                for col_num, value in enumerate(enriched_df.columns.values):
                    worksheet.write(1, col_num, value, header_format)
                worksheet.freeze_panes(2, 2)
            print(f"SUCCESS: Formatted Excel for human analysis saved to: {excel_path}")
        except ImportError:
            print(f"WARN: 'openpyxl' not found. Cannot create formatted Excel file. Please run 'pip install openpyxl'")
        except Exception as e:
            print(f"ERROR: Could not create formatted Excel file. Error: {e}")
    else:
        print("WARN: No records were enriched. No output file was created.")