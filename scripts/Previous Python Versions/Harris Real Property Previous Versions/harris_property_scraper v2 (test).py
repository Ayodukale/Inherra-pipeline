# --- Full script with Refined Scan Range & HTML Table Detection (v8.1 - "Related Docs" post-processing) ---
import asyncio
from datetime import datetime, timedelta, date
from pathlib import Path
import re
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout, Page, Locator, Error as PlaywrightError
import time
import csv 
import json 
from bs4 import BeautifulSoup

# --- Precompiled Regex & Constants ---
LEGAL_PATTERNS = {
    'desc': re.compile(r'(?:DESC|DESCRIPTION)[:\s#-]*\s*(.*?)(?=\s*(?:LOT:|BLOCK:|SEC:|SECTION:|SUBD:|SUBDIVISION:|ABSTRACT:|SURVEY:|TRACT:|$))', re.IGNORECASE),
    'subdivision': re.compile(r'(?:SUBD|SUBDIVISION)[:\s#-]*\s*(.*?)(?=\s*(?:LOT:|BLOCK:|SEC:|SECTION:|ABSTRACT:|SURVEY:|TRACT:|$))', re.IGNORECASE),
    'lot': re.compile(r'LOT[:\s#-]*\s*([\w\s.-]+?)(?=\s*(?:BLOCK:|SEC:|SECTION:|COMMENT:|$))', re.IGNORECASE), 
    'block': re.compile(r'BLOCK[:\s#-]*\s*([\w\s.-]+?)(?=\s*(?:LOT:|SEC:|SECTION:|COMMENT:|$))', re.IGNORECASE),
    'sec': re.compile(r'(?:SEC|SECTION)[:\s#-]*\s*([\w\s.-]+?)(?=\s*(?:LOT:|BLOCK:|COMMENT:|$))', re.IGNORECASE),
    'abstract': re.compile(r'ABSTRACT[:\s#-]*\s*([\w\s.-]+?)(?=\s*(?:SURVEY:|TRACT:|$))', re.IGNORECASE),
    'survey': re.compile(r'SURVEY[:\s#-]*\s*([\w\s.-]+?)(?=\s*(?:ABSTRACT:|TRACT:|$))', re.IGNORECASE),
    'tract': re.compile(r'TRACT[:\s#-]*\s*([\w\s.-]+?)(?=\s*(?:ABSTRACT:|SURVEY:|$))', re.IGNORECASE),
}

PORTAL_URL = "https://cclerk.hctx.net/Applications/WebSearch/RP.aspx"
TODAY_SCRIPT_RUN = datetime.today()
DATE_FROM_OBJ = TODAY_SCRIPT_RUN.date() - timedelta(days=1) 
DATE_TO_OBJ = TODAY_SCRIPT_RUN.date()
DATE_FROM_STR = DATE_FROM_OBJ.strftime("%m/%d/%Y")
DATE_TO_STR = DATE_TO_OBJ.strftime("%m/%d/%Y")

INPUT_PROBATE_LEADS_CSV = Path("harris_sample.csv") 
OUTPUT_DIR = Path("data/targeted_results"); OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_TARGETED_CSV = OUTPUT_DIR / Path(f"harris_rp_targeted_matches_{TODAY_SCRIPT_RUN.strftime('%Y%m%d_%H%M%S')}.csv")

MAX_PAGES_PER_NAME_SEARCH = 10 
MAX_SEARCH_RETRIES_TARGETED = 1 
POLITE_DELAY_AFTER_PAGINATION_CLICK_S = 1
DEFAULT_TIMEOUT = 45_000
PAGE_LOAD_TIMEOUT = 90_000
SEARCH_RESULTS_TIMEOUT = 30_000 
MAX_ROWS_TO_DEBUG_HTML = 5 
MIN_MAIN_RECORD_CELLS_FLEXIBLE = 5 
MAX_CONSECUTIVE_EMPTY_PAGES_TARGETED = 2 
MAX_MAIN_RECORDS_TO_EXTRACT_PER_LEAD_TEST_MODE = 5

def ts_print(message: str): print(f"[{datetime.now().isoformat()}] {message}")
def clean_cell_text(raw_text: str) -> str:
    if raw_text is None: return ""
    return re.sub(r"\s+", " ", raw_text).strip()
def parse_party_name(name_str: str) -> dict:
    if not name_str: return {"last": "", "first": ""}
    parts = name_str.strip().split()
    if not parts: return {"last": "", "first": ""}
    return {"last": parts[0], "first": " ".join(parts[1:]) if len(parts) > 1 else ""}
def compute_signal_rp_score(property_records: list, probate_filing_date: date) -> tuple[int, int]: return 0,0 
def is_button_disabled(button_locator: Locator) -> bool:
    if not button_locator or button_locator.count() == 0: ts_print("[DEBUG is_btn_disabled] No button found."); return True
    try:
        if not button_locator.is_visible(timeout=3000): ts_print("[DEBUG is_btn_disabled] Button not visible."); return True
        if not button_locator.is_enabled(timeout=3000): ts_print("[DEBUG is_btn_disabled] Button not enabled."); return True
        if button_locator.get_attribute("disabled") is not None: ts_print("[DEBUG is_btn_disabled] Has 'disabled' attr."); return True
        class_attr = button_locator.get_attribute("class")
        if class_attr and ("disabled" in class_attr.lower() or "aspNetDisabled" in class_attr): ts_print("[DEBUG is_btn_disabled] Has 'disabled' class."); return True
    except PlaywrightTimeout: ts_print("[DEBUG is_btn_disabled] Timeout checking state, assuming disabled."); return True
    except Exception as e: ts_print(f"[ERROR is_btn_disabled] Error checking: {e}"); return True
    ts_print("[DEBUG is_btn_disabled] Button active and enabled."); return False

def verify_rp_form_ready(page: Page) -> bool: 
    ts_print("[DEBUG verify_rp_form_ready] Verifying RP form context...")
    try:
        grantor_field = page.locator('input[name="ctl00$ContentPlaceHolder1$txtOR"]')
        grantee_field = page.locator('input[name="ctl00$ContentPlaceHolder1$txtEE"]')
        page.locator('input[name="ctl00$ContentPlaceHolder1$txtFrom"]').wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        page.locator('input[name="ctl00$ContentPlaceHolder1$txtTo"]').wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        grantor_field.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        grantee_field.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        grantor_placeholder = grantor_field.get_attribute("placeholder")
        if grantor_placeholder and "Last Name First" not in grantor_placeholder: ts_print(f"[WARN] Grantor placeholder: '{grantor_placeholder}'.")
        grantee_placeholder = grantee_field.get_attribute("placeholder")
        if grantee_placeholder and "Last Name First" not in grantee_placeholder: ts_print(f"[WARN] Grantee placeholder: '{grantee_placeholder}'.")
        search_btn = page.locator('input[name="ctl00$ContentPlaceHolder1$btnSearch"]')
        search_btn.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        if not search_btn.is_enabled(timeout=DEFAULT_TIMEOUT): ts_print("[WARN] Search button not enabled."); _capture_screenshot(page, "form_search_btn_disabled"); return False
        ts_print("[DEBUG verify_rp_form_ready] RP Form inputs appear usable."); return True
    except PlaywrightTimeout as pte: ts_print(f"[WARN] RP Inputs/Btn not vis/enabled (Timeout: {pte})."); _capture_screenshot(page, "form_verify_timeout"); return False
    except Exception as e: ts_print(f"[ERROR verify_rp_form_ready] Error: {e}"); _capture_screenshot(page, "form_verify_exception"); return False

def locate_results_table_rp(page: Page) -> Locator | None: 
    ts_print("[DEBUG locate_table_rp] Finding results table...")
    selectors = ["table#ctl00_ContentPlaceHolder1_gvSearchResults","table:has(tr:has-text('File Number'))","table:has(tr:has-text('Instrument Type'))","table:has(tr:has-text('RP-'))","table#ItemPlaceholderContainer","table.table-striped.table-condensed","table.table-striped"]
    located_table: Locator | None = None; successful_selector = ""
    for i, selector_str in enumerate(selectors):
        ts_print(f"[DEBUG locate_table_rp] Attempting selector {i+1}: {selector_str}")
        current_locator_set = page.locator(selector_str) 
        if current_locator_set.count() > 0: 
            current_locator_element = current_locator_set.first
            try:
                if current_locator_element.is_visible(timeout=5000): ts_print(f"[INFO] Found visible table using selector: {selector_str}"); located_table=current_locator_element; successful_selector=selector_str; break 
            except: pass
    if not located_table:
        no_records_loc = page.get_by_text("No Records Found", exact=False)
        if no_records_loc.count() > 0:
            try:
                if no_records_loc.first.is_visible(timeout=3000): ts_print("[INFO] 'No Records Found' message detected. No table to return."); return None
            except: ts_print("[DEBUG] 'No Records Found' locator found but not visible.")
        ts_print("[WARN] No suitable results table found, and no clear 'No Records' message."); _capture_screenshot(page, "locate_table_rp_failed"); return None
    try:
        if located_table.locator("tr").count() > 0: ts_print(f"[DEBUG] Chosen table (by '{successful_selector}') has {located_table.locator('tr').count()} trs."); return located_table
        else: ts_print(f"[WARN] Chosen table (by '{successful_selector}') has no trs."); return None
    except Exception as e: ts_print(f"[WARN] Error validating chosen table: {e}"); return None

def parse_probate_filing_date_from_input(date_str: str) -> date | None:
    if not date_str: return None
    try: return datetime.strptime(date_str.strip(), "%m/%d/%Y").date()
    except ValueError: ts_print(f"[WARN] Could not parse input date: {date_str}"); return None

def extract_legal_description_from_html_table(html_content: str, record_file_number_for_log: str, page_num_for_log: int, k_for_log: int) -> dict:
    log_prefix = f"  [BS4_HTML_LEGAL P{page_num_for_log}R{k_for_log+1} File# {record_file_number_for_log}]"
    ts_print(f"{log_prefix} Parsing structured HTML for legal description.")
    legal_data = {"legal_description_text": "","legal_lot": "","legal_block": "","legal_subdivision": "","legal_abstract": "","legal_survey": "","legal_tract": "","legal_sec": ""}
    if not html_content: ts_print(f"{log_prefix} HTML content is empty."); return legal_data
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        rows = soup.find_all("tr") 
        if not rows:
            table_in_soup = soup.find("table")
            if table_in_soup:
                ts_print(f"{log_prefix} No TRs directly, found nested table. Processing its TRs.")
                rows = table_in_soup.find_all("tr")
            else:
                ts_print(f"{log_prefix} No TRs or nested table in HTML snippet. Raw text: {soup.get_text(separator=' ')[:200]}")
                return legal_data 
        ts_print(f"{log_prefix} Found {len(rows)} potential rows in legal description section.")
        temp_desc_lines = []
        for row_idx, row_soup in enumerate(rows):
            tds_in_row = row_soup.find_all('td')
            if len(tds_in_row) == 2:
                label_cell_soup = tds_in_row[0]; value_cell_soup = tds_in_row[1]
                label = ""; label_span = label_cell_soup.find('span')
                if label_span: label = clean_cell_text(label_span.get_text()).upper()
                else: label_b = label_cell_soup.find('b'); label = clean_cell_text(label_b.get_text() if label_b else label_cell_soup.get_text()).upper()
                value = ""; value_span = value_cell_soup.find('span') 
                if value_span: value = clean_cell_text(value_span.get_text())
                else: value = clean_cell_text(value_cell_soup.get_text()) 
                if not value: continue
                matched_by_label = False
                if 'DESC:' in label: legal_data["legal_description_text"] = value; matched_by_label=True
                elif 'BLOCK:' in label: legal_data["legal_block"] = value; matched_by_label=True
                elif 'LOT:' in label: legal_data["legal_lot"] = value; matched_by_label=True
                elif 'SUBDIV' in label: legal_data["legal_subdivision"] = value; matched_by_label=True 
                elif 'ABSTRACT' in label: legal_data["legal_abstract"] = value; matched_by_label=True
                elif 'SURVEY' in label: legal_data["legal_survey"] = value; matched_by_label=True
                elif 'TRACT' in label: legal_data["legal_tract"] = value; matched_by_label=True
                elif 'SEC:' in label or 'SECTION:' in label : legal_data["legal_sec"] = value; matched_by_label=True
                elif 'COMMENT:' in label: 
                    if legal_data["legal_description_text"]: legal_data["legal_description_text"] += f" | COMMENT: {value}"
                    else: legal_data["legal_description_text"] = f"COMMENT: {value}"
                    matched_by_label=True
                if matched_by_label: ts_print(f"        [BS4_MATCH] Label '{label}' -> '{value}'")
                elif label and value: temp_desc_lines.append(f"{label_cell_soup.get_text(strip=True)}: {value}")
            elif len(tds_in_row) > 0 : ts_print(f"      [BS4_LEGAL_ROW {row_idx}] Row has {len(tds_in_row)} TDs, not 2. Content: {str(row_soup)[:150]}")
        if temp_desc_lines:
            joined_temp = " | ".join(temp_desc_lines)
            if legal_data["legal_description_text"]: legal_data["legal_description_text"] += f" | OTHER_LEGAL: {joined_temp}"
            else: legal_data["legal_description_text"] = f"OTHER_LEGAL: {joined_temp}"
            ts_print(f"    {log_prefix} Appended unclassified lines: {joined_temp}")
    except Exception as e: ts_print(f"[ERROR] {log_prefix} extract_legal_description_from_html_table failed: {e}")
    return legal_data

def parse_plain_text_legal_description(text_content: str, record_file_number_for_log: str, page_num_for_log: int, main_row_k_for_log: int) -> dict:
    log_prefix = f"  [REGEX_LEGAL_PARSE P{page_num_for_log}R{main_row_k_for_log+1} File# {record_file_number_for_log}]"
    ts_print(f"{log_prefix} Parsing plain text: '{text_content[:100]}...'")
    legal_data = {"legal_description_text": "","legal_lot": "","legal_block": "","legal_subdivision": "","legal_abstract": "","legal_survey": "","legal_tract": "","legal_sec": ""}
    if not isinstance(text_content, str): ts_print(f"{log_prefix} Received non-string content. Type: {type(text_content)}"); return legal_data
    
    temp_parsed = {}
    ordered_keys = ['lot', 'block', 'sec', 'tract', 'abstract', 'survey', 'subdivision', 'desc']

    for key_p in ordered_keys:
        pattern_p = LEGAL_PATTERNS.get(key_p)
        if not pattern_p: continue

        match = pattern_p.search(text_content)
        if match:
            value = clean_cell_text(match.group(1))
            if value: 
                dict_key = f"legal_{key_p}" if key_p != "desc" else "legal_description_text"
                if dict_key not in temp_parsed or not temp_parsed[dict_key]:
                    temp_parsed[dict_key] = value
                    ts_print(f"    {log_prefix} Regex Matched {dict_key} -> '{value}'")
    
    for key_to_fill, val_to_fill in temp_parsed.items():
        if key_to_fill in legal_data: legal_data[key_to_fill] = val_to_fill
            
    if not legal_data.get("legal_description_text") and text_content:
        other_fields_concatenated_len = sum(len(v) for k,v in legal_data.items() if k != "legal_description_text" and v)
        if len(text_content) > other_fields_concatenated_len + 10: 
            if not any(val for key, val in legal_data.items() if key != "legal_description_text" and val):
                ts_print(f"    {log_prefix} No specific patterns matched. Assigning full text to description.")
                legal_data["legal_description_text"] = text_content

    # --- START: ADDED POST-PROCESSING FOR "Related Docs" ---
    for key_rd_clean in legal_data: 
        if isinstance(legal_data[key_rd_clean], str) and "Related Docs" in legal_data[key_rd_clean]:
            original_value = legal_data[key_rd_clean]
            cleaned_value = original_value.split("Related Docs")[0].strip()
            if original_value != cleaned_value: # Only print if a change was made
                ts_print(f"    {log_prefix} Cleaning 'Related Docs' from {key_rd_clean}: '{original_value}' -> '{cleaned_value}'")
                legal_data[key_rd_clean] = cleaned_value
    # --- END: ADDED POST-PROCESSING FOR "Related Docs" ---

    return legal_data

# --- UPDATED extract_data_from_current_page_rp (Step 1 & 4 and refined legal parsing calls) ---
def extract_data_from_current_page_rp(table_locator: Locator, page_num: int) -> list:
    ts_print(f"[DEBUG extract_data_rp] P{page_num}: Starting HYBRID extraction for grouped records.")
    recs = []
    all_trs = table_locator.locator("tr").all(); num_total_trs = len(all_trs)
    ts_print(f"[DEBUG extract_data_rp] P{page_num}: Found {num_total_trs} total <tr> elements")
    if num_total_trs == 0: ts_print(f"[WARN] P{page_num}: No <tr> elements found"); return []
    
    k = 0
    main_records_on_page_count = 0 
    while k < num_total_trs:
        current_tr = all_trs[k]; td_elements = current_tr.locator("td"); num_tds = td_elements.count()
        
        if k < max(20, MAX_ROWS_TO_DEBUG_HTML * 4): 
            ts_print(f"[TR_DEBUG P{page_num}R{k+1}] Cells: {num_tds}")
            try:
                for j_debug in range(min(num_tds, 7)): ts_print(f"  Cell {j_debug}: '{clean_cell_text(td_elements.nth(j_debug).inner_text(timeout=1000))[:70]}'")
            except Exception as e_dbg_detail: ts_print(f"  Debug error for P{page_num}R{k+1}C{j_debug if 'j_debug' in locals() else 'unknown'}: {e_dbg_detail}")
        
        file_number_text = ""; file_number_cell_idx = -1; is_main_record_row = False
        if num_tds >= MIN_MAIN_RECORD_CELLS_FLEXIBLE: 
            try:
                for cell_idx_check in [0, 1, 2]:  
                    if cell_idx_check < num_tds: 
                        try:
                            text = clean_cell_text(td_elements.nth(cell_idx_check).inner_text(timeout=1000))
                            if any(text.startswith(prefix) for prefix in ["RP-", "RM-", "RT-"]):
                                file_number_text = text; file_number_cell_idx = cell_idx_check; is_main_record_row=True; break
                        except: continue 
            except Exception as e_fn_check: ts_print(f"[WARN] Error checking for file_number P{page_num}R{k+1}: {e_fn_check}")

        if is_main_record_row:
            main_records_on_page_count += 1
            ts_print(f"[MAIN_RECORD P{page_num}R{k+1}] File#: '{file_number_text}' in cell[{file_number_cell_idx}]")
            
            idx_file_number_actual = file_number_cell_idx
            idx_file_date_actual = idx_file_number_actual + 1
            idx_type_vol_page_actual = idx_file_number_actual + 2
            idx_names_col_actual = idx_file_number_actual + 3 
            idx_expected_html_legal_col = idx_file_number_actual + 4 

            ts_print(f"  [MAIN_RECORD_DEBUG P{page_num}R{k+1}] Actual File# Idx: {idx_file_number_actual}, Date Idx: {idx_file_date_actual}, Type Idx: {idx_type_vol_page_actual}, Expected Legal Idx: {idx_expected_html_legal_col}")

            if page_num <=1 and main_records_on_page_count <= MAX_ROWS_TO_DEBUG_HTML : 
                ts_print(f"    [ROW_STRUCTURE_DEBUG P{page_num}R{k+1}] File#: {file_number_text}. Total cells: {num_tds}")
                for i_debug_cell in range(num_tds):
                    try: ts_print(f"      Cell[{i_debug_cell}]: '{clean_cell_text(td_elements.nth(i_debug_cell).inner_text(timeout=500))[:100]}'")
                    except Exception as e_cell_dbg: ts_print(f"      Cell[{i_debug_cell}]: [Error reading: {e_cell_dbg}]")

            try:
                current_record = {"file_number": file_number_text, "grantors": [], "grantees": [], "trustees": [] }
                current_record["file_date"] = clean_cell_text(td_elements.nth(idx_file_date_actual).inner_text(timeout=1000)) if num_tds > idx_file_date_actual else ""
                type_vol_page_raw = clean_cell_text(td_elements.nth(idx_type_vol_page_actual).inner_text(timeout=1000)) if num_tds > idx_type_vol_page_actual else ""
                current_record["instrument_type"] = type_vol_page_raw.split()[0] if type_vol_page_raw else ""
                
                parsed_legal_data = {"legal_description_text": "","legal_lot": "","legal_block": "","legal_subdivision": "","legal_abstract": "","legal_survey": "","legal_tract": "","legal_sec": ""}
                html_content_type_A = ""
                
                ts_print(f"    [LEGAL_ATTEMPT_1 P{page_num}R{k+1}] Checking td[{idx_expected_html_legal_col}] for HTML table.")
                if num_tds > idx_expected_html_legal_col:
                    potential_legal_table_cell_loc = td_elements.nth(idx_expected_html_legal_col)
                    try:
                        html_content_type_A = potential_legal_table_cell_loc.inner_html(timeout=2000)
                        html_lower = html_content_type_A.lower()
                        if "<table" in html_lower and \
                           (any(id_marker.lower() in html_lower for id_marker in ['lblDesc', 'lblBlock', 'lblLot', 'lvLegal', 'lblSubDivAdd']) or \
                            any(text_marker.lower() in html_lower for text_marker in ['<b>desc:', '<b>lot:', '<b>block:'])):
                            ts_print(f"      [LEGAL_ATTEMPT_1 P{page_num}R{k+1}] td[{idx_expected_html_legal_col}] looks like structured HTML table. Parsing with BS4.")
                            parsed_legal_data = extract_legal_description_from_html_table(html_content_type_A, file_number_text, page_num, k)
                        else: ts_print(f"      [LEGAL_ATTEMPT_1 P{page_num}R{k+1}] td[{idx_expected_html_legal_col}] not a structured legal table. Text: '{clean_cell_text(potential_legal_table_cell_loc.inner_text())[:100]}'")
                    except Exception as e_attempt1: ts_print(f"      [LEGAL_ATTEMPT_1 P{page_num}R{k+1}] Error checking td[{idx_expected_html_legal_col}]: {e_attempt1}")
                else: ts_print(f"    [LEGAL_ATTEMPT_1 P{page_num}R{k+1}] Expected legal HTML cell td[{idx_expected_html_legal_col}] out of bounds (num_tds: {num_tds}).")

                key_fields_found_A = parsed_legal_data.get("legal_lot") or parsed_legal_data.get("legal_block") or parsed_legal_data.get("legal_subdivision") or parsed_legal_data.get("legal_sec") or parsed_legal_data.get("legal_description_text")
                
                if not key_fields_found_A:
                    ts_print(f"    [LEGAL_ATTEMPT_2 P{page_num}R{k+1}] Key legal fields not from HTML table. Scanning other cells for plain text.")
                    scan_start_idx = idx_names_col_actual + 1 
                    scan_end_idx = min(idx_file_number_actual + 22, num_tds) 
                    ts_print(f"      [LEGAL_ATTEMPT_2_SCAN_RANGE P{page_num}R{k+1}] Scanning from td[{scan_start_idx}] to td[{scan_end_idx-1}]")
                    for scan_i in range(scan_start_idx, scan_end_idx):
                        if scan_i == idx_expected_html_legal_col and html_content_type_A and "<table" in html_content_type_A.lower(): continue 
                        try:
                            plain_text_content = clean_cell_text(td_elements.nth(scan_i).inner_text(timeout=1000))
                            if not plain_text_content or len(plain_text_content) < 5 : continue
                            if any(keyword.lower() in plain_text_content.lower() for keyword in ["Desc:", "Lot:", "Block:", "Sec:", "Subdivision:", "Abstract:", "Survey:", "Tract:"]):
                                ts_print(f"        [LEGAL_ATTEMPT_2_SCAN P{page_num}R{k+1}] Found potential plain text in td[{scan_i}]: '{plain_text_content[:100]}'")
                                parsed_from_plain = parse_plain_text_legal_description(plain_text_content, file_number_text, page_num, k)
                                for key, value in parsed_from_plain.items():
                                    if value and not parsed_legal_data.get(key): parsed_legal_data[key] = value
                                if parsed_legal_data.get("legal_lot") or parsed_legal_data.get("legal_block") or parsed_legal_data.get("legal_description_text"): ts_print(f"        [LEGAL_ATTEMPT_2_SCAN P{page_num}R{k+1}] Parsed plain text from td[{scan_i}]. Break scan."); break 
                        except Exception as e_scan: ts_print(f"      [LEGAL_ATTEMPT_2_SCAN P{page_num}R{k+1}] Error scanning td[{scan_i}]: {e_scan}")
                
                current_record.update(parsed_legal_data); current_record["signal_strength_rp"] = 0 
                k_sub_loop_start_index = k + 1; next_outer_k = k_sub_loop_start_index 
                for k_sub_idx in range(k_sub_loop_start_index, num_total_trs):
                    sub_tr=all_trs[k_sub_idx]; sub_tds_loc=sub_tr.locator("td"); sub_num_tds=sub_tds_loc.count()
                    next_outer_k = k_sub_idx + 1 
                    is_next_main_record_sub = False
                    if sub_num_tds >= MIN_MAIN_RECORD_CELLS_FLEXIBLE:
                        try:
                            temp_file_num_text_sub = ""
                            for sub_cell_idx_check in [0,1,2]:
                                if sub_cell_idx_check < sub_num_tds:
                                    text_check = clean_cell_text(sub_tds_loc.nth(sub_cell_idx_check).inner_text(timeout=500))
                                    if any(text_check.startswith(prefix) for prefix in ["RP-", "RM-", "RT-"]): is_next_main_record_sub = True; temp_file_num_text_sub = text_check; break
                            if is_next_main_record_sub: ts_print(f"[DEBUG] Next main record ({temp_file_num_text_sub}) at TR {k_sub_idx}. End block."); next_outer_k=k_sub_idx; break 
                        except: pass 
                    if sub_num_tds == 2:
                        try:
                            label_text_raw = sub_tds_loc.nth(0).inner_text(timeout=1000); label = clean_cell_text(label_text_raw).upper()
                            value_cell_loc = sub_tds_loc.nth(1); value = clean_cell_text(value_cell_loc.inner_text(timeout=1000))
                            span_in_td = value_cell_loc.locator("span"); 
                            if span_in_td.count() > 0: value = clean_cell_text(span_in_td.first.inner_text(timeout=1000))
                            if "GRANTOR" in label: current_record["grantors"].append(parse_party_name(value))
                            elif "GRANTEE" in label: current_record["grantees"].append(parse_party_name(value))
                            elif "TRUSTEE" in label: current_record["trustees"].append(parse_party_name(value))
                            else: ts_print(f"[DEBUG] TR {k_sub_idx} (2 cells) label '{label_text_raw.strip()}' not recognized. End block."); next_outer_k=k_sub_idx; break 
                        except Exception as e_sub: ts_print(f"[WARN] Error sub-row TR {k_sub_idx}: {e_sub}")
                    elif sub_num_tds == 0: ts_print(f"[DEBUG] TR {k_sub_idx} is empty. Skipping sub-row."); continue 
                    else: ts_print(f"[DEBUG] TR {k_sub_idx} has {sub_num_tds} cells ({str(sub_tr.inner_text(timeout=500))[:50]}...), not typical. End block."); next_outer_k=k_sub_idx; break
                k = next_outer_k - 1 
                current_record["grantors"]=json.dumps(current_record["grantors"]); current_record["grantees"]=json.dumps(current_record["grantees"]); current_record["trustees"]=json.dumps(current_record["trustees"])
                recs.append(current_record)
            except Exception as e_main_proc:
                ts_print(f"[ERROR] Error processing main record TR {k+1}: {e_main_proc}")
                if k < MAX_ROWS_TO_DEBUG_HTML:
                    try:
                        ts_print(f"[DEBUG] HTML of problematic main row: {current_tr.inner_html(timeout=1000)}")
                    except:
                        pass
        k += 1 
    ts_print(f"[INFO extract_data_rp] P{page_num}: Extracted {len(recs)} records from {num_total_trs} TRs."); return recs

def search_rp_for_decedent_and_extract(page: Page, decedent_last: str, decedent_first: str | None, probate_filing_date_obj: date | None) -> list:
    ts_print(f"--- Starting RP Search for: {decedent_last}, {decedent_first or ''} (Probate File Date: {probate_filing_date_obj}) ---")
    all_properties_for_decedent = []
    if not probate_filing_date_obj: ts_print(f"[WARN] No valid probate filing date for {decedent_last}. Skip."); return []
    search_date_from = probate_filing_date_obj - timedelta(days=365); search_date_to = probate_filing_date_obj + timedelta(days=365)   
    search_date_from_str = search_date_from.strftime("%m/%d/%Y"); search_date_to_str = search_date_to.strftime("%m/%d/%Y")
    ts_print(f"[INFO] Searching RP records from {search_date_from_str} to {search_date_to_str}")
    search_name_input = decedent_last.strip().upper()
    ts_print(f"[INFO] Using search name (LAST NAME ONLY MODE): '{search_name_input}' for Grantor/Grantee")
    for attempt in range(MAX_SEARCH_RETRIES_TARGETED + 1):
        try:
            ts_print(f"[DEBUG] Navigating to {PORTAL_URL} for search (Attempt {attempt + 1})")
            page.goto(PORTAL_URL, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT); page.wait_for_timeout(2000)
            if not verify_rp_form_ready(page): _capture_screenshot(page, f"ts_form_not_ready_att{attempt+1}_{search_name_input.replace(' ','_')}"); raise RuntimeError("RP Form not ready")
            page.locator('input[name="ctl00$ContentPlaceHolder1$txtFrom"]').fill(search_date_from_str)
            page.locator('input[name="ctl00$ContentPlaceHolder1$txtTo"]').fill(search_date_to_str)
            page.locator('input[name="ctl00$ContentPlaceHolder1$txtOR"]').fill(search_name_input); ts_print(f"[DEBUG] Filled Grantor: '{search_name_input}'")
            page.locator('input[name="ctl00$ContentPlaceHolder1$txtEE"]').fill(search_name_input); ts_print(f"[DEBUG] Filled Grantee: '{search_name_input}'")
            search_button = page.locator('input[name="ctl00$ContentPlaceHolder1$btnSearch"]'); ts_print(f"[DEBUG] Clicking search..."); search_button.click()
            try: ts_print(f"[DEBUG] Wait network idle..."); page.wait_for_load_state("networkidle", timeout=SEARCH_RESULTS_TIMEOUT); ts_print(f"[DEBUG] Network idle achieved.")
            except PlaywrightTimeout: ts_print(f"[WARN] Timeout network idle for '{search_name_input}'. Proceeding.")
            _capture_screenshot(page, f"ts_after_click_{search_name_input.replace(' ','_')}")
            try:
                with open(OUTPUT_DIR / f"debug_targetsearch_after_click_{search_name_input.replace(' ','_')}.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
            except Exception as e_html_dump: ts_print(f"[WARN] Could not dump HTML: {e_html_dump}")
            table_l = locate_results_table_rp(page)
            if not table_l: ts_print(f"[INFO] No table by locate_results_table_rp for '{search_name_input}'."); return [] 
            page_scrape_count=0; consecutive_empty_pages=0
            first_rec_sel_rel="tr:not(:has(th)):first-of-type td:first-child"; prev_first_rec_text=f"INITIAL_TARGETED_{search_name_input}"
            if table_l.locator("tbody").count()>0 : first_rec_sel_rel="tbody tr:not(:has(th)):first-of-type td:first-child"
            while page_scrape_count < MAX_PAGES_PER_NAME_SEARCH:
                curr_pg_disp_num=page_scrape_count+1; ts_print(f"[INFO] Scraping P{curr_pg_disp_num} for {search_name_input}...")
                try: table_l.wait_for(state="visible",timeout=DEFAULT_TIMEOUT)
                except: ts_print(f"[ERROR] P{curr_pg_disp_num}: Table not visible."); break
                curr_pg_first_rec_text=""
                if table_l.count()>0:
                    first_rec_loc=table_l.locator(first_rec_sel_rel)
                    if first_rec_loc.count() > 0:
                        try:
                            curr_pg_first_rec_text = clean_cell_text(first_rec_loc.first.inner_text(timeout=5000))
                        except:
                            pass
                if page_scrape_count>0 and curr_pg_first_rec_text and curr_pg_first_rec_text==prev_first_rec_text: ts_print(f"[INFO] P{curr_pg_disp_num}: First record SAME. End."); break
                prev_first_rec_text=curr_pg_first_rec_text
                page_data=extract_data_from_current_page_rp(table_l, curr_pg_disp_num)
                if page_data:
                    consecutive_empty_pages=0
                    for rec in page_data: rec.update({"searched_decedent_last":decedent_last,"searched_decedent_first":decedent_first or "","probate_filing_date_for_search":probate_filing_date_obj.strftime("%Y-%m-%d")})
                    all_properties_for_decedent.extend(page_data)
                    if len(all_properties_for_decedent) >= MAX_MAIN_RECORDS_TO_EXTRACT_PER_LEAD_TEST_MODE: ts_print(f"[INFO][TEST_MODE] Reached {MAX_MAIN_RECORDS_TO_EXTRACT_PER_LEAD_TEST_MODE} recs for {search_name_input}. Stop."); break 
                else:
                    consecutive_empty_pages+=1; ts_print(f"[WARN] P{curr_pg_disp_num}: No main recs for {search_name_input}. Empty: {consecutive_empty_pages}")
                    if consecutive_empty_pages >= MAX_CONSECUTIVE_EMPTY_PAGES_TARGETED: ts_print(f"[INFO] Max empty pages for {search_name_input}. Stop."); break
                ts_print(f"[INFO] P{curr_pg_disp_num} for {search_name_input}: Extracted {len(page_data)}. Total for name: {len(all_properties_for_decedent)}")
                if len(all_properties_for_decedent) >= MAX_MAIN_RECORDS_TO_EXTRACT_PER_LEAD_TEST_MODE: break
                next_btn=page.locator("#ctl00_ContentPlaceHolder1_BtnNext")
                if next_btn.count()==0 or is_button_disabled(next_btn): ts_print(f"[INFO] P{curr_pg_disp_num}: No active Next. Last page for {search_name_input}."); break
                page_scrape_count+=1
                if page_scrape_count>=MAX_PAGES_PER_NAME_SEARCH: ts_print(f"[WARN] Reached MAX_PAGES for {search_name_input}."); break
                ts_print(f"[INFO] Clicking Next for P{page_scrape_count+1} for {search_name_input}...")
                next_btn.click(); page.wait_for_timeout(POLITE_DELAY_AFTER_PAGINATION_CLICK_S*1000+1000)
                page.wait_for_load_state("domcontentloaded",timeout=DEFAULT_TIMEOUT)
                table_l=locate_results_table_rp(page)
                if not table_l: ts_print(f"[ERROR] Failed to re-locate table on P{page_scrape_count+1}."); break
            ts_print(f"--- Finished RP Search for: {decedent_last}, {decedent_first or ''}. Found {len(all_properties_for_decedent)} records. ---")
            return all_properties_for_decedent
        except (RuntimeError,PlaywrightTimeout,PlaywrightError) as e:
            ts_print(f"[WARN] Attempt {attempt+1} for {decedent_last} failed: {e}")
            if attempt<MAX_SEARCH_RETRIES_TARGETED: ts_print(f"[WARN] Retrying for {decedent_last}..."); page.wait_for_timeout(3000*(attempt+1))
            else: ts_print(f"[ERROR] All attempts failed for {decedent_last}."); _capture_screenshot(page,f"ts_ALL_ATTEMPTS_FAILED_{decedent_last}"); return []
    return []

def _capture_screenshot(page, name_suffix): 
    if page and not page.is_closed(): 
        try: timestamp=datetime.now().strftime('%H%M%S'); page.screenshot(path=OUTPUT_DIR/f"debug_rp_targeted_{name_suffix}_{timestamp}.png")
        except Exception as e_ss: ts_print(f"[WARN _capture_screenshot] Failed: {e_ss}")

def run_targeted_rp_scrape() -> pd.DataFrame: 
    ts_print(f"--- Starting Harris County RP TARGETED Scraper ---"); ts_print(f"Reading leads from: {INPUT_PROBATE_LEADS_CSV}"); ts_print(f"Output CSV: {OUT_TARGETED_CSV}")
    all_found_property_records=[]; STOP_AFTER_FIRST_SUCCESSFUL_LEAD=True 
    try:
        probate_leads_df=pd.read_csv(INPUT_PROBATE_LEADS_CSV,sep=';',dtype=str).fillna("")
        leads_to_process=probate_leads_df.to_dict('records'); ts_print(f"Loaded {len(leads_to_process)} leads.")
    except FileNotFoundError: ts_print(f"[FATAL] Input CSV not found: {INPUT_PROBATE_LEADS_CSV}"); return pd.DataFrame()
    except Exception as e_csv: ts_print(f"[FATAL] Error reading CSV: {e_csv}"); return pd.DataFrame()
    with sync_playwright() as p:
        browser=None; page_for_screenshot=None
        try:
            browser=p.chromium.launch(headless=True) 
            context=browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
            context.set_default_timeout(DEFAULT_TIMEOUT); page=context.new_page(); page_for_screenshot=page
            for i,lead in enumerate(leads_to_process):
                ts_print(f"--- Processing lead {i+1} of {len(leads_to_process)} ---")
                decedent_last=str(lead.get("decedent_last","")).strip().upper(); decedent_first=str(lead.get("decedent_first","")).strip().upper()
                probate_filing_date_str=str(lead.get("filing_date","")).strip() 
                if not decedent_last: ts_print(f"[WARN] Lead {i+1} missing last name. Skip."); continue
                probate_filing_date_obj=parse_probate_filing_date_from_input(probate_filing_date_str)
                if not probate_filing_date_obj: ts_print(f"[WARN] Lead {i+1} ('{decedent_last}') invalid filing_date ('{probate_filing_date_str}'). Skip."); continue
                if i>0 and not STOP_AFTER_FIRST_SUCCESSFUL_LEAD : page.wait_for_timeout(2000) 
                property_records=search_rp_for_decedent_and_extract(page, decedent_last, decedent_first, probate_filing_date_obj)
                if property_records: 
                    ts_print(f"[SUCCESS] Found {len(property_records)} props for {decedent_last}, {decedent_first or ''}.")
                    all_found_property_records.extend(property_records)
                    if STOP_AFTER_FIRST_SUCCESSFUL_LEAD: ts_print(f"[INFO][TEST_MODE] Stop after 1st successful lead."); break 
                else: ts_print(f"--- No props for {decedent_last}, {decedent_first or ''}. Next lead. ---")
        except PlaywrightTimeout as e_fto: ts_print(f"[FATAL] Playwright Timeout: {e_fto}"); _capture_screenshot(page_for_screenshot,"fatal_timeout")
        except PlaywrightError as e_fpw: ts_print(f"[FATAL] Playwright Error: {e_fpw}"); _capture_screenshot(page_for_screenshot,"fatal_playwright_error")
        except Exception as e_main: ts_print(f"[FATAL] Main loop failed: {e_main}"); _capture_screenshot(page_for_screenshot,"fatal_unexpected_error")
        finally: 
            ts_print("Closing browser.");
            if browser:
                try:
                    browser.close()
                except Exception as e_bc:
                    ts_print(f"[WARN] Error closing browser: {e_bc}")
    if not all_found_property_records: ts_print("No property records found (or stopped before)."); return pd.DataFrame()
    df=pd.DataFrame(all_found_property_records); ts_print(f"Total property records collected: {len(df)}")
    if not df.empty:
        cols=["searched_decedent_last","searched_decedent_first","probate_filing_date_for_search","file_number","file_date","instrument_type","grantors","grantees","trustees","legal_description_text","legal_lot","legal_block","legal_subdivision","legal_abstract","legal_survey","legal_tract","legal_sec","signal_strength_rp"]
        for c in cols: 
            if c not in df: df[c]="[]" if c in ["grantors","grantees","trustees"] else ""
            else: 
                 if c in ["grantors","grantees","trustees"]: df[c]=df[c].fillna("[]")
                 else: df[c]=df[c].fillna("")
        df=df[cols]; df.drop_duplicates(subset=["file_number","file_date","instrument_type","searched_decedent_last"],keep="first",inplace=True)
        ts_print(f"Records after dedupe: {len(df)}")
        if not df.empty: df.to_csv(OUT_TARGETED_CSV,index=False,sep=';',quoting=csv.QUOTE_ALL); ts_print(f"Saved {len(df)} to {OUT_TARGETED_CSV}"); print(df.head(min(3,len(df))).to_string())
        else: ts_print(f"No data after dedupe. {OUT_TARGETED_CSV} not created.")
    else: ts_print(f"No data extracted, {OUT_TARGETED_CSV} not created.")
    ts_print("--- Harris County RP TARGETED Scraper Finished ---"); return df

if __name__ == "__main__":
    run_targeted_rp_scrape()