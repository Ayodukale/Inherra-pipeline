# --- Full script with Tiered Search, Flattened Output & Enriched Lead Data (v12.1) ---
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

INPUT_PROBATE_LEADS_CSV = Path("harris_sample.csv") 
OUTPUT_DIR_NAME = "Harris RP Data Scrapes"
OUTPUT_DIR = Path(OUTPUT_DIR_NAME) 
OUTPUT_DIR.mkdir(parents=True, exist_ok=True) # Creates the folder if it doesn't exist

# Define the full path for the output CSV file
OUT_TARGETED_CSV = OUTPUT_DIR / Path(f"harris_rp_targeted_matches_{TODAY_SCRIPT_RUN.strftime('%Y%m%d_%H%M%S')}.csv")

MAX_SEARCH_RETRIES_TARGETED = 1 
POLITE_DELAY_AFTER_PAGINATION_CLICK_S = 1
DEFAULT_ELEMENT_TIMEOUT = 10_000 
FORM_FILL_TIMEOUT = 15_000 
SEARCH_RESULTS_TIMEOUT = 45_000 
PAGE_LOAD_TIMEOUT_HARD_RESET = 90_000 
PAGE_LOAD_TIMEOUT_INITIAL = 90_000 

MAX_ROWS_TO_DEBUG_HTML = 3 
MIN_MAIN_RECORD_CELLS_FLEXIBLE = 5 
MAX_CONSECUTIVE_EMPTY_PAGES_TARGETED = 2

TIER_SETTINGS = {
    "enable_tier_3": False, 
    "max_pages_per_tier": 2, 
    "common_surnames": {"SMITH", "TAYLOR", "JOHNSON", "WILLIAMS", "JONES", "BROWN", "DAVIS", "MILLER", "WILSON", "MOORE", "TAYLOR", "ANDERSON", "THOMAS", "JACKSON", "WHITE", "HARRIS", "MARTIN", "THOMPSON", "GARCIA", "MARTINEZ", "ROBINSON"} 
}


# --- Add this near the top of Script 2 ---
NICKNAME_MAP = {
    "JOHN": ["JOHNNY", "JOHNNIE", "JON"],
    "WILLIAM": ["BILL", "BILLY", "WILL", "WILLIE", "LIAM"],
    "ROBERT": ["BOB", "ROBBIE", "ROBBY", "BERT", "ROB"],
    "JAMES": ["JIM", "JIMMY"],
    "RICHARD": ["DICK", "RICH", "RICK", "RICKY"],
    "CHARLES": ["CHUCK", "CHARLIE", "CHAS"],
    "JOSEPH": ["JOE", "JOEY"],
    "THOMAS": ["TOM", "TOMMY"],
    "MICHAEL": ["MIKE", "MICKEY", "MICK"],
    "DAVID": ["DAVE", "DAVY"],
    "ELIZABETH": ["LIZ", "LIZZIE", "BETH", "BETTY", "LISA", "LIBBY", "ELLE"],
    "MARGARET": ["MAGGIE", "PEG", "PEGGY", "RITA", "DAISY"],
    "CATHERINE": ["KATE", "KATHY", "CATHY", "KITTY", "KATIE"],
    "KATHERINE": ["KATE", "KATHY", "CATHY", "KITTY", "KATIE"],
    "SUSAN": ["SUE", "SUZIE", "SUZY"],
    "PATRICIA": ["PAT", "PATTY", "TRISH", "TRICIA"],
    # Add more common ones - this is a starting point
}

def _nickname_variants(first_name: str, nickname_map: dict) -> list[str]:
    if not first_name: # Handle empty or None first_name
        return []
    fn_upper = first_name.upper().strip()
    variants = set() # Use a set to avoid duplicates initially

    # Check if fn_upper is a canonical name in the map
    if fn_upper in nickname_map:
        variants.update(nickname_map[fn_upper])
        variants.add(fn_upper) # Add the canonical name itself

    # Check if fn_upper is a nickname in any of the map's value lists
    for canon, nicks_list in nickname_map.items():
        if fn_upper in nicks_list:
            variants.add(canon) # Add the canonical form
            variants.update(nicks_list) # Add all its associated nicknames

    if not variants and fn_upper: # If no variants found yet, the original name is the only variant
        variants.add(fn_upper)
        
    # Return all variants EXCEPT the original input first name, if present
    # This ensures we only search for actual variations
    final_variants = [v for v in list(variants) if v != fn_upper]
    return final_variants
# --- End of new additions ---


def ts_print(message: str): print(f"[{datetime.now().isoformat()}] {message}")

def clean_cell_text(raw_text: str) -> str:
    if raw_text is None: return ""
    return re.sub(r"\s+", " ", raw_text).strip()

def parse_party_name(name_str: str) -> dict:
    if not name_str: return {"last": "", "first": ""}
    parts = name_str.strip().split()
    if not parts: return {"last": "", "first": ""}
    return {"last": parts[0], "first": " ".join(parts[1:]) if len(parts) > 1 else ""}

def standardize_name_for_search(raw_last: str, raw_first: str = "") -> str:
    _name_clean = lambda name_part: re.sub(r"[^\w\s'-]", "", str(name_part or "")).upper().strip()
    cleaned_last = _name_clean(raw_last)
    cleaned_last = re.sub(r"\s+(JR|SR|I{1,3}|IV|V|VI{0,3}|IX|X)$", "", cleaned_last, flags=re.IGNORECASE).strip()
    cleaned_last = re.sub(r",\s*(JR|SR|I{1,3}|IV|V|VI{0,3}|IX|X)$", "", cleaned_last, flags=re.IGNORECASE).strip()
    full_cleaned_first_name_field = _name_clean(raw_first) 
    first_name_to_use = ""
    if full_cleaned_first_name_field:
        first_name_parts = full_cleaned_first_name_field.split()
        if first_name_parts: first_name_to_use = first_name_parts[0] 
    if first_name_to_use: return f"{cleaned_last} {first_name_to_use}".strip()
    return cleaned_last

def parse_probate_filing_date_from_input(date_str: str) -> date | None:
    if not date_str: return None
    date_str = date_str.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"):
        try: return datetime.strptime(date_str, fmt).date()
        except ValueError: continue
    ts_print(f"[WARN parse_probate_filing_date] Could not parse input date: '{date_str}' using tried formats.")
    return None

def compute_signal_rp_score_for_record(record: dict, probate_filing_date: date) -> int: # Expects rp_prefixed keys in record
    score = 0
    if record.get("rp_legal_description_text","").strip(): score += 1
    if record.get("rp_legal_lot","").strip() or record.get("rp_legal_block","").strip(): score += 1
    record_date_str = record.get("rp_file_date") # Changed to rp_file_date
    if record_date_str and probate_filing_date:
        try:
            record_date = datetime.strptime(record_date_str, "%m/%d/%Y").date()
            days_diff = abs((record_date - probate_filing_date).days)
            if days_diff <= 180: score += 2
            elif days_diff <= 365: score += 1
        except ValueError: ts_print(f"[WARN compute_score] Could not parse rp_file_date '{record_date_str}' for scoring {record.get('rp_file_number')}.")
        except Exception as e: ts_print(f"[WARN compute_score] Error scoring {record.get('rp_file_number')}: {e}")
    return score

def is_button_disabled(button_locator: Locator) -> bool:
    if not button_locator or button_locator.count() == 0: ts_print("    [DEBUG is_btn_disabled] No button locator found."); return True
    try:
        if not button_locator.is_visible(timeout=2000): ts_print("    [DEBUG is_btn_disabled] Button not visible."); return True
        if not button_locator.is_enabled(timeout=2000): ts_print("    [DEBUG is_btn_disabled] Button not enabled (is_enabled=false)."); return True
        if button_locator.get_attribute("disabled") is not None: ts_print("    [DEBUG is_btn_disabled] Button has 'disabled' attribute."); return True
        class_attr = button_locator.get_attribute("class")
        if class_attr and ("disabled" in class_attr.lower() or "aspNetDisabled" in class_attr): ts_print("    [DEBUG is_btn_disabled] Button has 'disabled' in class attribute."); return True
    except PlaywrightTimeout: ts_print("    [DEBUG is_btn_disabled] Timeout checking button state, assuming disabled."); return True
    except Exception as e: ts_print(f"    [ERROR is_btn_disabled] Error checking button: {e}, assuming disabled."); return True
    return False

def verify_rp_form_ready(page: Page, timeout_ms: int = DEFAULT_ELEMENT_TIMEOUT) -> bool: 
    try:
        grantor_field = page.locator('input[name="ctl00$ContentPlaceHolder1$txtOR"]')
        grantee_field = page.locator('input[name="ctl00$ContentPlaceHolder1$txtEE"]')
        page.locator('input[name="ctl00$ContentPlaceHolder1$txtFrom"]').wait_for(state="visible", timeout=timeout_ms)
        page.locator('input[name="ctl00$ContentPlaceHolder1$txtTo"]').wait_for(state="visible", timeout=timeout_ms)
        grantor_field.wait_for(state="visible", timeout=timeout_ms)
        grantee_field.wait_for(state="visible", timeout=timeout_ms)
        search_btn = page.locator('input[name="ctl00$ContentPlaceHolder1$btnSearch"]')
        search_btn.wait_for(state="visible", timeout=timeout_ms)
        if not search_btn.is_enabled(timeout=timeout_ms): ts_print("  [WARN] Search button not enabled."); _capture_screenshot(page, "form_search_btn_disabled"); return False
        return True
    except PlaywrightTimeout as pte: ts_print(f"  [WARN] RP Inputs/Btn not vis/enabled (Timeout: {pte})."); _capture_screenshot(page, "form_verify_timeout"); return False
    except Exception as e: ts_print(f"  [ERROR verify_rp_form_ready] Error: {e}"); _capture_screenshot(page, "form_verify_exception"); return False

def locate_results_table_rp(page: Page) -> Locator | None: 
    selectors = ["table#ctl00_ContentPlaceHolder1_gvSearchResults","table:has(tr:has-text('File Number'))","table:has(tr:has-text('Instrument Type'))","table:has(tr:has-text('RP-'))","table#ItemPlaceholderContainer","table.table-striped.table-condensed","table.table-striped", "table.grid", "table.results", "table:visible"]
    located_table: Locator | None = None
    for i, selector_str in enumerate(selectors):
        current_locator_set = page.locator(selector_str) 
        if current_locator_set.count() > 0: 
            for j in range(current_locator_set.count()):
                current_locator_element = current_locator_set.nth(j)
                try:
                    if current_locator_element.is_visible(timeout=1000):
                        if current_locator_element.locator("tr").count() > 1 or "File Number" in current_locator_element.inner_text(timeout=1000):
                            located_table=current_locator_element; break
                except: continue
            if located_table: break 
    if not located_table:
        no_records_loc = page.get_by_text("No Records Found", exact=False)
        if no_records_loc.count() > 0:
            try:
                if no_records_loc.first.is_visible(timeout=3000): ts_print("    [INFO] 'No Records Found' message detected. No table to return."); return None
            except: pass 
        ts_print("    [WARN] No suitable results table found after trying all selectors, and no clear 'No Records' message.");
        return None
    return located_table

def extract_legal_description_from_html_table(html_content: str, record_file_number_for_log: str, page_num_for_log: int, k_for_log: int) -> dict: # Returns rp_prefixed keys
    log_prefix = f"  [BS4_HTML_LEGAL P{page_num_for_log}R{k_for_log+1} File# {record_file_number_for_log}]"
    legal_data_template = {"rp_legal_description_text": "","rp_legal_lot": "","rp_legal_block": "","rp_legal_subdivision": "","rp_legal_abstract": "","rp_legal_survey": "","rp_legal_tract": "","rp_legal_sec": ""}
    legal_data = legal_data_template.copy()
    if not html_content: ts_print(f"{log_prefix} HTML content is empty."); return legal_data
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        rows = soup.find_all("tr") 
        if not rows:
            table_in_soup = soup.find("table")
            if table_in_soup: rows = table_in_soup.find_all("tr")
            else: ts_print(f"{log_prefix} No TRs or nested table. Raw text: {soup.get_text(separator=' ')[:100]}"); return legal_data 
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
                if 'DESC:' in label: legal_data["rp_legal_description_text"] = value
                elif 'BLOCK:' in label: legal_data["rp_legal_block"] = value
                elif 'LOT:' in label: legal_data["rp_legal_lot"] = value
                elif 'SUBDIV' in label: legal_data["rp_legal_subdivision"] = value
                elif 'ABSTRACT' in label: legal_data["rp_legal_abstract"] = value
                elif 'SURVEY' in label: legal_data["rp_legal_survey"] = value
                elif 'TRACT' in label: legal_data["rp_legal_tract"] = value
                elif 'SEC:' in label or 'SECTION:' in label : legal_data["rp_legal_sec"] = value
                elif 'COMMENT:' in label: 
                    if legal_data["rp_legal_description_text"]: legal_data["rp_legal_description_text"] += f" | COMMENT: {value}"
                    else: legal_data["rp_legal_description_text"] = f"COMMENT: {value}"
                elif label and value: temp_desc_lines.append(f"{label_cell_soup.get_text(strip=True)}: {value}")
        if temp_desc_lines:
            joined_temp = " | ".join(temp_desc_lines)
            if legal_data["rp_legal_description_text"]: legal_data["rp_legal_description_text"] += f" | OTHER_LEGAL: {joined_temp}"
            else: legal_data["rp_legal_description_text"] = f"OTHER_LEGAL: {joined_temp}"
    except Exception as e: 
        ts_print(f"[ERROR] {log_prefix} extract_legal_description_from_html_table failed: {e}")
        error_desc = f"[PARSE ERROR in BS4_HTML_LEGAL] Raw HTML: {html_content[:200]}... Error: {str(e)[:100]}"
        legal_data = legal_data_template.copy(); legal_data["rp_legal_description_text"] = error_desc # Store error in rp_prefixed field
    return legal_data

def parse_plain_text_legal_description(text_content: str, record_file_number_for_log: str, page_num_for_log: int, main_row_k_for_log: int) -> dict: # Returns rp_prefixed keys
    log_prefix = f"  [REGEX_LEGAL_PARSE P{page_num_for_log}R{main_row_k_for_log+1} File# {record_file_number_for_log}]"
    legal_data = {"rp_legal_description_text": "","rp_legal_lot": "","rp_legal_block": "","rp_legal_subdivision": "","rp_legal_abstract": "","rp_legal_survey": "","rp_legal_tract": "","rp_legal_sec": ""}
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
                dict_key = f"rp_legal_{key_p}" if key_p != "desc" else "rp_legal_description_text" # rp_prefix
                if dict_key not in temp_parsed or not temp_parsed[dict_key]: 
                    temp_parsed[dict_key] = value
    for key_to_fill, val_to_fill in temp_parsed.items():
        if key_to_fill in legal_data: legal_data[key_to_fill] = val_to_fill
    if not any(val for key, val in legal_data.items() if key not in ["rp_legal_description_text", "rp_legal_sec"] and val) and text_content and not legal_data.get("rp_legal_description_text"):
        legal_data["rp_legal_description_text"] = text_content
    for key_rd_clean in list(legal_data.keys()): 
        if isinstance(legal_data[key_rd_clean], str) and "Related Docs" in legal_data[key_rd_clean]:
            original_value = legal_data[key_rd_clean]
            cleaned_value = original_value.split("Related Docs")[0].strip()
            if original_value != cleaned_value: 
                ts_print(f"    {log_prefix} Cleaning 'Related Docs' from {key_rd_clean}: '{original_value}' -> '{cleaned_value}'"); legal_data[key_rd_clean] = cleaned_value
    return legal_data

def parse_parties_from_names_column(names_text: str, file_number: str, page_num: int, row_k: int) -> tuple[list, list, list]:
    log_prefix = f"    [FALLBACK_NAMES_PARSE P{page_num}R{row_k+1} File# {file_number}]"
    if not names_text: return [], [], []
    grantors, grantees, trustees = [], [], []
    party_pattern = re.compile(r'(Grantor|Grantee|Trustee|GTR|GTE|TR)[:\s]+\s*(.*?)(?=\s*(?:Grantor|Grantee|Trustee|GTR|GTE|TR|$))', re.IGNORECASE)
    matches_found = False
    for match in party_pattern.finditer(names_text):
        matches_found = True
        party_type, name_value = match.groups()
        name_clean = clean_cell_text(name_value)
        if not name_clean: continue
        party_data = parse_party_name(name_clean)
        party_type_lower = party_type.lower()
        if 'grantor' in party_type_lower or 'gtr' in party_type_lower: grantors.append(party_data)
        elif 'grantee' in party_type_lower or 'gte' in party_type_lower: grantees.append(party_data)
        elif 'trustee' in party_type_lower or 'tr' in party_type_lower: trustees.append(party_data)
    if matches_found: ts_print(f"{log_prefix} Parsed from Names Column: GTRs={len(grantors)}, GTEs={len(grantees)}, TRs={len(trustees)}")
    return grantors, grantees, trustees

def extract_data_from_current_page_rp(table_locator: Locator, page_num_for_log: int) -> list:
    recs = [] 
    all_trs = table_locator.locator("tr").all(); num_total_trs = len(all_trs)
    if num_total_trs == 0: ts_print(f"[WARN] P{page_num_for_log}: No <tr> elements found"); return []
    k = 0; main_records_on_page_count = 0 
    while k < num_total_trs:
        current_tr = all_trs[k]; td_elements = current_tr.locator("td"); num_tds = td_elements.count()
        if main_records_on_page_count < MAX_ROWS_TO_DEBUG_HTML or k < (MAX_ROWS_TO_DEBUG_HTML * 3):
            ts_print(f"  [TR_DEBUG P{page_num_for_log}R{k+1}] Cells: {num_tds}")
            try:
                for j_debug in range(min(num_tds, 7)): ts_print(f"    Cell {j_debug}: '{clean_cell_text(td_elements.nth(j_debug).inner_text(timeout=500))[:70]}'")
            except Exception as e_dbg_detail: ts_print(f"    Debug error for P{page_num_for_log}R{k+1}: {e_dbg_detail}")
        
        file_number_text = ""; file_number_cell_idx = -1; is_main_record_row = False
        if num_tds >= MIN_MAIN_RECORD_CELLS_FLEXIBLE: 
            try:
                for cell_idx_check in [0, 1, 2]:  
                    if cell_idx_check < num_tds: 
                        try:
                            text = clean_cell_text(td_elements.nth(cell_idx_check).inner_text(timeout=500))
                            if any(text.startswith(prefix) for prefix in ["RP-", "RM-", "RT-"]):
                                file_number_text = text; file_number_cell_idx = cell_idx_check; is_main_record_row=True; break
                        except: continue 
            except Exception as e_fn_check: ts_print(f"  [WARN] Error checking for file_number P{page_num_for_log}R{k+1}: {e_fn_check}")

        if is_main_record_row:
            main_records_on_page_count += 1
            ts_print(f"  [MAIN_RECORD P{page_num_for_log}R{k+1}] File#: '{file_number_text}' in cell[{file_number_cell_idx}]")
            
            idx_file_number_actual = file_number_cell_idx; idx_file_date_actual = idx_file_number_actual + 1
            idx_type_vol_page_actual = idx_file_number_actual + 2; idx_names_col_actual = idx_file_number_actual + 3 
            idx_expected_html_legal_col = idx_file_number_actual + 4 
            if main_records_on_page_count <= MAX_ROWS_TO_DEBUG_HTML : 
                ts_print(f"      [ROW_STRUCTURE_DEBUG P{page_num_for_log}R{k+1}] File#: {file_number_text}. Total cells: {num_tds}")
            try:
                temp_rp_record = {"rp_file_number": file_number_text, "grantors": [], "grantees": [], "trustees": [] } # Temporary dict for RP data
                temp_rp_record["rp_file_date"] = clean_cell_text(td_elements.nth(idx_file_date_actual).inner_text(timeout=500)) if num_tds > idx_file_date_actual else ""
                type_vol_page_raw = clean_cell_text(td_elements.nth(idx_type_vol_page_actual).inner_text(timeout=500)) if num_tds > idx_type_vol_page_actual else ""
                temp_rp_record["rp_instrument_type"] = type_vol_page_raw.split()[0] if type_vol_page_raw else ""
                
                parsed_legal_data = extract_legal_description_from_html_table("", file_number_text, page_num_for_log, k) # Init with rp_ prefixed empty fields
                html_content_type_A = ""
                
                if num_tds > idx_expected_html_legal_col:
                    potential_legal_table_cell_loc = td_elements.nth(idx_expected_html_legal_col)
                    try:
                        html_content_type_A = potential_legal_table_cell_loc.inner_html(timeout=1000)
                        html_lower = html_content_type_A.lower()
                        if "<table" in html_lower and (any(id_marker.lower() in html_lower for id_marker in ['lblDesc', 'lblBlock', 'lblLot', 'lvLegal', 'lblSubDivAdd']) or any(text_marker.lower() in html_lower for text_marker in ['<b>desc:', '<b>lot:', '<b>block:'])):
                            ts_print(f"      [LEGAL_ATTEMPT_1 P{page_num_for_log}R{k+1}] td[{idx_expected_html_legal_col}] HAS HTML TABLE. Parsing with BS4.")
                            parsed_legal_data = extract_legal_description_from_html_table(html_content_type_A, file_number_text, page_num_for_log, k)
                    except Exception as e_attempt1: ts_print(f"      [LEGAL_ATTEMPT_1 P{page_num_for_log}R{k+1}] Error checking td[{idx_expected_html_legal_col}]: {e_attempt1}")

                key_fields_found_A = parsed_legal_data.get("rp_legal_lot") or parsed_legal_data.get("rp_legal_block") or parsed_legal_data.get("rp_legal_subdivision") or parsed_legal_data.get("rp_legal_sec") or parsed_legal_data.get("rp_legal_description_text")
                
                if not key_fields_found_A or (isinstance(parsed_legal_data.get("rp_legal_description_text"), str) and "[PARSE ERROR" in parsed_legal_data.get("rp_legal_description_text")):
                    scan_start_idx = idx_names_col_actual + 1 
                    scan_end_idx = min(idx_file_number_actual + 22, num_tds) 
                    for scan_i in range(scan_start_idx, scan_end_idx):
                        if scan_i == idx_expected_html_legal_col and html_content_type_A and "<table" in html_content_type_A.lower(): continue 
                        try:
                            plain_text_content = clean_cell_text(td_elements.nth(scan_i).inner_text(timeout=500))
                            if not plain_text_content or len(plain_text_content) < 5 : continue
                            if any(keyword.lower() in plain_text_content.lower() for keyword in ["Desc:", "Lot:", "Block:", "Sec:", "Subdivision:", "Abstract:", "Survey:", "Tract:"]):
                                ts_print(f"        [LEGAL_ATTEMPT_2_SCAN P{page_num_for_log}R{k+1}] Found plain text in td[{scan_i}]: '{plain_text_content[:70]}'")
                                parsed_from_plain = parse_plain_text_legal_description(plain_text_content, file_number_text, page_num_for_log, k)
                                for key_lp, value_lp in parsed_from_plain.items(): # key_lp will be rp_prefixed
                                    if value_lp and not parsed_legal_data.get(key_lp): parsed_legal_data[key_lp] = value_lp
                                if parsed_legal_data.get("rp_legal_lot") or parsed_legal_data.get("rp_legal_block") or parsed_legal_data.get("rp_legal_description_text"): break 
                        except Exception as e_scan: ts_print(f"      [LEGAL_ATTEMPT_2_SCAN P{page_num_for_log}R{k+1}] Error scanning td[{scan_i}]: {e_scan}")
                
                temp_rp_record.update(parsed_legal_data) # Add legal fields to temp_rp_record
                k_sub_loop_start_index = k + 1; next_outer_k = k_sub_loop_start_index 
                for k_sub_idx in range(k_sub_loop_start_index, num_total_trs):
                    sub_tr=all_trs[k_sub_idx]; sub_tds_loc=sub_tr.locator("td"); sub_num_tds=sub_tds_loc.count()
                    next_outer_k = k_sub_idx + 1 
                    is_next_main_record_sub = False
                    if sub_num_tds >= MIN_MAIN_RECORD_CELLS_FLEXIBLE:
                        try:
                            for sub_cell_idx_check in [0,1,2]:
                                if sub_cell_idx_check < sub_num_tds:
                                    text_check = clean_cell_text(sub_tds_loc.nth(sub_cell_idx_check).inner_text(timeout=200))
                                    if any(text_check.startswith(prefix) for prefix in ["RP-", "RM-", "RT-"]): is_next_main_record_sub = True; break
                            if is_next_main_record_sub: next_outer_k=k_sub_idx; break 
                        except: pass 
                    if sub_num_tds == 2:
                        try:
                            label_text_raw = sub_tds_loc.nth(0).inner_text(timeout=500); label_cleaned = clean_cell_text(label_text_raw).upper()
                            value_cell_loc = sub_tds_loc.nth(1); value_cleaned = clean_cell_text(value_cell_loc.inner_text(timeout=500))
                            span_in_td = value_cell_loc.locator("span"); 
                            if span_in_td.count() > 0: value_cleaned = clean_cell_text(span_in_td.first.inner_text(timeout=500))
                            party_found_in_subrow = False
                            if "GRANTOR" in label_cleaned: temp_rp_record["grantors"].append(parse_party_name(value_cleaned)); party_found_in_subrow = True
                            elif "GRANTEE" in label_cleaned: temp_rp_record["grantees"].append(parse_party_name(value_cleaned)); party_found_in_subrow = True
                            elif "TRUSTEE" in label_cleaned: temp_rp_record["trustees"].append(parse_party_name(value_cleaned)); party_found_in_subrow = True
                            if party_found_in_subrow: continue
                            legal_field_updated_in_subrow = False # Uses rp_prefixed keys
                            if 'DESC:' in label_cleaned and not temp_rp_record.get("rp_legal_description_text"): temp_rp_record["rp_legal_description_text"] = value_cleaned; legal_field_updated_in_subrow = True
                            elif 'LOT:' in label_cleaned and not temp_rp_record.get("rp_legal_lot"): temp_rp_record["rp_legal_lot"] = value_cleaned; legal_field_updated_in_subrow = True
                            elif 'BLOCK:' in label_cleaned and not temp_rp_record.get("rp_legal_block"): temp_rp_record["rp_legal_block"] = value_cleaned; legal_field_updated_in_subrow = True
                            elif ('SUBDIV' in label_cleaned or 'SUBDIVISION' in label_cleaned) and not temp_rp_record.get("rp_legal_subdivision"): temp_rp_record["rp_legal_subdivision"] = value_cleaned; legal_field_updated_in_subrow = True
                            if legal_field_updated_in_subrow: ts_print(f"      [SUBROW_LEGAL P{page_num_for_log}R{k_sub_idx}] Label '{label_cleaned}' -> '{value_cleaned}'"); continue
                            next_outer_k=k_sub_idx; break 
                        except Exception as e_sub: ts_print(f"    [WARN SUBROW_PROC_ERR P{page_num_for_log}R{k_sub_idx}]: {e_sub}"); next_outer_k=k_sub_idx; break
                    elif sub_num_tds == 0: continue 
                    else: next_outer_k=k_sub_idx; break
                k = next_outer_k - 1 
                if not temp_rp_record["grantors"] and not temp_rp_record["grantees"] and not temp_rp_record["trustees"]:
                    names_col_text = ""
                    if num_tds > idx_names_col_actual:
                        try: names_col_text = clean_cell_text(td_elements.nth(idx_names_col_actual).inner_text(timeout=500))
                        except Exception as e_names_col: ts_print(f"      [WARN P{page_num_for_log}R{k+1}] Could not read names column for fallback: {e_names_col}")
                    if names_col_text:
                        gtrs, gtes, trs = parse_parties_from_names_column(names_col_text, file_number_text, page_num_for_log, k) 
                        if gtrs and not temp_rp_record["grantors"]: temp_rp_record["grantors"] = gtrs
                        if gtes and not temp_rp_record["grantees"]: temp_rp_record["grantees"] = gtes
                        if trs and not temp_rp_record["trustees"]: temp_rp_record["trustees"] = trs
                
                                # temp_rp_record is now fully populated with initial data, including potentially "rp_legal_lot": "6-7"
                # AND grantors/grantees/trustees lists are populated in temp_rp_record

                # --- START OF NEW LOT EXPANSION LOGIC ---
                lot_value_str = str(temp_rp_record.get("rp_legal_lot", "")).strip()
                expanded_individual_lots = []

                if not lot_value_str: # If lot is empty, treat as one "empty lot" entry to process parties once
                    expanded_individual_lots.append("")
                elif re.fullmatch(r'^\d+-\d+$', lot_value_str): # Matches "6-7", "10-12"
                    try:
                        start_lot, end_lot = map(int, lot_value_str.split('-'))
                        if start_lot <= end_lot:
                            for i in range(start_lot, end_lot + 1):
                                expanded_individual_lots.append(str(i))
                        else:
                            ts_print(f"      [WARN_LOT_PARSE P{page_num_for_log}R{k+1}] Invalid lot range '{lot_value_str}', keeping as is.")
                            expanded_individual_lots.append(lot_value_str) # Keep original if range is invalid
                    except ValueError:
                        ts_print(f"      [WARN_LOT_PARSE P{page_num_for_log}R{k+1}] Could not parse lot range '{lot_value_str}', keeping as is.")
                        expanded_individual_lots.append(lot_value_str) # Keep original if parsing fails
                elif ',' in lot_value_str: # Matches "5,6,7"
                    expanded_individual_lots = [lot.strip() for lot in lot_value_str.split(',') if lot.strip()]
                elif '&' in lot_value_str: # Matches "5 & 6"
                    expanded_individual_lots = [lot.strip() for lot in lot_value_str.split('&') if lot.strip()]
                # Add other delimiters like "AND" if observed
                # Example: elif ' AND ' in lot_value_str.upper():
                # expanded_lots = [lot.strip() for lot in re.split(r'\s+AND\s+', lot_value_str, flags=re.IGNORECASE) if lot.strip()]
                else: # Single lot or unhandled pattern
                    expanded_individual_lots.append(lot_value_str)
                
                if not expanded_individual_lots: # If after parsing, list is empty (e.g. lot was just ",")
                    expanded_individual_lots.append("") # Ensure at least one iteration for records without specific lots

                # ---
                # Create the base record information, EXCLUDING the original rp_legal_lot
                # as we will add the expanded lots individually.
                # Also exclude party lists as they will be processed per expanded lot.
                # ---
                base_info_for_expansion = {
                    key: val for key, val in temp_rp_record.items() 
                    if key not in ["rp_legal_lot", "grantors", "grantees", "trustees"]
                }

                # ---
                # Now, loop through each expanded_individual_lot.
                # For each individual lot, create records for all associated parties.
                # ---
                processed_any_party_for_main_record = False
                for current_lot in expanded_individual_lots:
                    # Create a record specific to this lot
                    lot_specific_base_data = base_info_for_expansion.copy()
                    lot_specific_base_data["rp_legal_lot"] = current_lot # Assign the individual lot

                    parties_found_for_this_lot = False
                    # Process Grantors for this lot
                    for party_dict in temp_rp_record.get("grantors", []):
                        row_data = lot_specific_base_data.copy()
                        row_data["rp_party_type"] = "Grantor"
                        row_data["rp_party_last_name"] = party_dict.get("last", "")
                        row_data["rp_party_first_name"] = party_dict.get("first", "")
                        recs.append(row_data)
                        parties_found_for_this_lot = True
                        processed_any_party_for_main_record = True
                    
                    # Process Grantees for this lot
                    for party_dict in temp_rp_record.get("grantees", []):
                        row_data = lot_specific_base_data.copy()
                        row_data["rp_party_type"] = "Grantee"
                        row_data["rp_party_last_name"] = party_dict.get("last", "")
                        row_data["rp_party_first_name"] = party_dict.get("first", "")
                        recs.append(row_data)
                        parties_found_for_this_lot = True
                        processed_any_party_for_main_record = True

                    # Process Trustees for this lot
                    for party_dict in temp_rp_record.get("trustees", []):
                        row_data = lot_specific_base_data.copy()
                        row_data["rp_party_type"] = "Trustee"
                        row_data["rp_party_last_name"] = party_dict.get("last", "")
                        row_data["rp_party_first_name"] = party_dict.get("first", "")
                        recs.append(row_data)
                        parties_found_for_this_lot = True
                        processed_any_party_for_main_record = True

                    # If NO parties were listed for this main record (grantors/grantees/trustees lists were empty)
                    # but we do have lot(s), create one entry per lot with N/A party.
                    if not temp_rp_record.get("grantors", []) and \
                       not temp_rp_record.get("grantees", []) and \
                       not temp_rp_record.get("trustees", []):
                        # This case means the original temp_rp_record had no parties.
                        # We still want to create a record for each expanded lot.
                        row_data = lot_specific_base_data.copy()
                        row_data["rp_party_type"] = "N/A"
                        row_data["rp_party_last_name"] = ""
                        row_data["rp_party_first_name"] = ""
                        recs.append(row_data)
                        processed_any_party_for_main_record = True # Mark as processed to avoid fallback below

                # This fallback is if the main record parsing (file#, date etc.) worked,
                # but absolutely NO parties were found in subrows OR names column, AND no lots expanded anything.
                # This situation should be rare if expanded_individual_lots always has at least one item.
                # The logic above handles the "no parties but has lots" case.
                # This is more for "no parties AND no meaningful lot info to expand on".
                if not processed_any_party_for_main_record:
                    ts_print(f"      [INFO P{page_num_for_log}R{k+1}] No parties and no distinct lots processed for {file_number_text}. Appending base RP info once with original lot value.")
                    # Fallback to creating a single record with the original (possibly ranged) lot value
                    # and N/A party if no parties were ever processed.
                    final_fallback_base = base_info_for_expansion.copy() # This already excludes party lists
                    final_fallback_base["rp_legal_lot"] = lot_value_str # Use original lot string
                    final_fallback_base["rp_party_type"] = "N/A"
                    final_fallback_base["rp_party_last_name"] = ""
                    final_fallback_base["rp_party_first_name"] = ""
                    recs.append(final_fallback_base)
                
                # --- END OF NEW LOT EXPANSION LOGIC ---
            except Exception as e_main_proc:
                ts_print(f"  [ERROR P{page_num_for_log}R{k+1}] Error processing main record: {e_main_proc}")
                if main_records_on_page_count <= MAX_ROWS_TO_DEBUG_HTML:
                    try: ts_print(f"    [DEBUG_HTML P{page_num_for_log}R{k+1}] HTML of problematic main row: {current_tr.inner_html(timeout=500)}")
                    except: pass
        k += 1 
    ts_print(f"  [INFO extract_data_rp] P{page_num_for_log}: Extracted {len(recs)} rows (flattened) from {num_total_trs} TRs."); return recs

def _is_rare_surname(surname: str, common_surnames_list: set) -> bool:
    return surname.upper().strip() not in common_surnames_list

def _execute_single_search(
    page: Page, search_name: str, tier_label: str,
    search_date_from_str: str, search_date_to_str: str, 
    max_pages_this_tier: int, overall_attempt_num: int 
    ) -> list:
    ts_print(f"  [{tier_label}] Attempting Grantor-ONLY search with name: '{search_name}'")
    records_for_this_search_term = []
    try:
        ts_print(f"    [{tier_label}] Ensuring clean form state (navigating to portal)...")
        for nav_attempt in range(2): 
            try:
                page.goto(PORTAL_URL, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT_HARD_RESET)
                page.locator('input[name="ctl00$ContentPlaceHolder1$txtOR"]').wait_for(state="visible", timeout=DEFAULT_ELEMENT_TIMEOUT)
                break
            except Exception as e_nav:
                ts_print(f"    [WARN {tier_label}] Navigation/reset attempt {nav_attempt+1} failed: {e_nav}")
                if nav_attempt == 1: 
                    _capture_screenshot(page, f"ts_nav_reset_failed_{tier_label}_att{overall_attempt_num}")
                    raise RuntimeError(f"Failed to reset page state for {tier_label} after multiple attempts.")
                page.wait_for_timeout(2000)

        if not verify_rp_form_ready(page, timeout_ms=FORM_FILL_TIMEOUT):
            _capture_screenshot(page, f"ts_form_not_ready_after_reset_{tier_label}_att{overall_attempt_num}")
            raise RuntimeError(f"RP Form not ready after reset for {tier_label}")
        ts_print(f"    [{tier_label}] Form reset successful.")

        def _safe_fill(locator: Locator, value: str, field_name: str):
            try:
                locator.fill("", timeout=5000) 
                locator.fill(value, timeout=FORM_FILL_TIMEOUT)
            except Exception as e_fill:
                ts_print(f"    [ERROR {tier_label}] Failed to fill '{field_name}': {e_fill}")
                _capture_screenshot(page, f"ts_fill_error_{field_name}_{tier_label}_att{overall_attempt_num}")
                raise 

        _safe_fill(page.locator('input[name="ctl00$ContentPlaceHolder1$txtFrom"]'), search_date_from_str, "DateFrom")
        _safe_fill(page.locator('input[name="ctl00$ContentPlaceHolder1$txtTo"]'), search_date_to_str, "DateTo")
        _safe_fill(page.locator('input[name="ctl00$ContentPlaceHolder1$txtOR"]'), search_name, "Grantor")
        _safe_fill(page.locator('input[name="ctl00$ContentPlaceHolder1$txtEE"]'), "", "Grantee (cleared)") 
        ts_print(f"    [{tier_label}] Filled Grantor: '{search_name}', Grantee: (EMPTY/CLEARED), Dates: {search_date_from_str}-{search_date_to_str}")
        
        if tier_label == "TIER_3": page.wait_for_timeout(1500)
        else: page.wait_for_timeout(500)

        search_button = page.locator('input[name="ctl00$ContentPlaceHolder1$btnSearch"]')
        ts_print(f"    [{tier_label}] Clicking search button...")
        search_button.click()

        try: 
            page.wait_for_load_state("networkidle", timeout=SEARCH_RESULTS_TIMEOUT)
        except PlaywrightTimeout: 
            ts_print(f"    [WARN {tier_label}] Timeout waiting for network idle for '{search_name}'. Proceeding.")

        _capture_screenshot(page, f"ts_after_tier_click_{search_name.replace(' ','_')}_{tier_label}_att{overall_attempt_num}")
        try:
            html_dump_name = OUTPUT_DIR / f"debug_targetsearch_after_tier_click_{search_name.replace(' ','_')}_{tier_label}_att{overall_attempt_num}.html"
            with open(html_dump_name, "w", encoding="utf-8") as f: f.write(page.content())
        except Exception as e_html_dump: 
            ts_print(f"    [WARN {tier_label}] Could not dump HTML: {e_html_dump}")

        table_l = locate_results_table_rp(page)
        if not table_l:
            ts_print(f"    [{tier_label}] No results table found for '{search_name}'.")
            return []

        current_page_in_tier = 0; consecutive_empty_pages_this_tier = 0
        prev_first_rec_text_in_tier = f"INITIAL_FOR_TIER_{tier_label}_{search_name}"
        
        while current_page_in_tier < max_pages_this_tier:
            page_num_for_logging = current_page_in_tier + 1
            ts_print(f"    [{tier_label} P{page_num_for_logging}] Scraping page for '{search_name}'...")
            current_table_l = locate_results_table_rp(page) 
            if not current_table_l:
                ts_print(f"    [ERROR {tier_label} P{page_num_for_logging}] Table disappeared for '{search_name}'. Ending tier.")
                break
            first_rec_sel_rel = "tr:not(:has(th)):first-of-type td:first-child"
            if current_table_l.locator("tbody").count() > 0: first_rec_sel_rel = "tbody tr:not(:has(th)):first-of-type td:first-child"
            curr_pg_first_rec_text_in_tier = ""
            first_rec_loc = current_table_l.locator(first_rec_sel_rel)
            if first_rec_loc.count() > 0:
                try: curr_pg_first_rec_text_in_tier = clean_cell_text(first_rec_loc.first.inner_text(timeout=3000))
                except: pass
            if current_page_in_tier > 0 and curr_pg_first_rec_text_in_tier and curr_pg_first_rec_text_in_tier == prev_first_rec_text_in_tier:
                ts_print(f"    [{tier_label} P{page_num_for_logging}] First record same as previous. End unique results for '{search_name}'.")
                break
            prev_first_rec_text_in_tier = curr_pg_first_rec_text_in_tier
            page_data = extract_data_from_current_page_rp(current_table_l, page_num_for_logging)
            if page_data:
                consecutive_empty_pages_this_tier = 0
                for rec in page_data:
                    rec["rp_found_by_search_term"] = search_name # rp_prefixed
                    rec["rp_search_tier"] = tier_label       # rp_prefixed
                records_for_this_search_term.extend(page_data)
            else:
                consecutive_empty_pages_this_tier += 1
                ts_print(f"    [WARN {tier_label} P{page_num_for_logging}] No main records for '{search_name}'. Empty: {consecutive_empty_pages_this_tier}")
                if consecutive_empty_pages_this_tier >= MAX_CONSECUTIVE_EMPTY_PAGES_TARGETED:
                    ts_print(f"    [{tier_label} P{page_num_for_logging}] Max consecutive empty pages for '{search_name}'. Stop tier.")
                    break
            ts_print(f"    [{tier_label} P{page_num_for_logging}] Extracted {len(page_data)} rows. Total for term: {len(records_for_this_search_term)}")
            next_btn = page.locator("#ctl00_ContentPlaceHolder1_BtnNext")
            if next_btn.count() == 0 or is_button_disabled(next_btn):
                ts_print(f"    [{tier_label} P{page_num_for_logging}] No active Next. End results for '{search_name}'.")
                break
            current_page_in_tier += 1
            if current_page_in_tier >= max_pages_this_tier:
                ts_print(f"    [{tier_label}] Reached max_pages_per_tier ({max_pages_this_tier}) for '{search_name}'.")
                break
            ts_print(f"    [{tier_label}] Clicking Next for page {current_page_in_tier + 1} for '{search_name}'...")
            next_btn.click(); page.wait_for_timeout(POLITE_DELAY_AFTER_PAGINATION_CLICK_S * 1000 + 500)
            try: page.wait_for_load_state("domcontentloaded", timeout=SEARCH_RESULTS_TIMEOUT)
            except PlaywrightTimeout: ts_print(f"    [WARN {tier_label}] Timeout waiting for DOM load after Next.")
    except Exception as e:
        ts_print(f"  [ERROR {tier_label}] Search execution failed for '{search_name}': {e}")
        _capture_screenshot(page, f"ts_error_in_tier_search_{search_name.replace(' ','_')}_{tier_label}_att{overall_attempt_num}")
    ts_print(f"  [{tier_label}] Finished Grantor-ONLY search for '{search_name}'. Found {len(records_for_this_search_term)} rows.")
    return records_for_this_search_term

def execute_tiered_rp_search(
    page: Page, 
    decedent_last: str, 
    decedent_first: str, 
    search_date_from_str: str, 
    search_date_to_str: str,
    tier_settings_dict: dict, # Expects "max_pages_per_tier", "common_surnames", "enable_tier_3_lastname_only"
    overall_attempt_num: int
) -> list:
    all_results_for_lead = []
    processed_search_names = set() # To avoid re-searching identical standardized names

    # --- TIER 1: LAST FIRST (Exact or Standardized First Part) ---
    tier1_name = standardize_name_for_search(decedent_last, decedent_first)
    ts_print(f"[TIER_1] Attempting search with: '{tier1_name}' (Original: Last='{decedent_last}', First='{decedent_first}')")
    if tier1_name.upper() not in processed_search_names:
        results_t1 = _execute_single_search(
            page, 
            tier1_name, 
            "TIER_1_EXACT_STD", 
            search_date_from_str, 
            search_date_to_str, 
            tier_settings_dict.get("max_pages_per_tier", 2), # Use .get for safety
            overall_attempt_num
        )
        processed_search_names.add(tier1_name.upper())
        if results_t1:
            all_results_for_lead.extend(results_t1)
            # Decision Point: Do we stop if Tier 1 is successful?
            # For maximum recall, we can continue. If high precision on first hit is preferred, return here.
            # ts_print(f"[INFO] Tier 1 found {len(results_t1)} records. Current total: {len(all_results_for_lead)}.")
            # return all_results_for_lead # Uncomment this line to stop after a successful Tier 1
    else:
        ts_print(f"[TIER_1] Search name '{tier1_name}' already processed. Skipping.")


    # --- TIER 2: NICKNAME EXPANSION (Replaces old Last + First Initial) ---
    # This tier will run if Tier 1 didn't return immediately (if the return above is commented out) 
    # OR if Tier 1 found nothing.
    if decedent_first and decedent_first.strip(): # Only run if there's a first name to get variants for
        ts_print(f"[TIER_2_NICK] Attempting nickname expansion for First Name: '{decedent_first}'")
        # Assuming NICKNAME_MAP is defined globally or passed appropriately
        nick_variants_to_search = _nickname_variants(decedent_first, NICKNAME_MAP) 
        
        # Use max_nickname_variants_to_search from tier_settings_dict if available, else default
        max_variants = tier_settings_dict.get("max_nickname_variants_to_search", 3)

        if nick_variants_to_search:
            actual_variants_searched_count = 0
            for i, nick in enumerate(nick_variants_to_search):
                if actual_variants_searched_count >= max_variants:
                    ts_print(f"  [TIER_2_NICK] Reached max nickname variants to search ({max_variants}).")
                    break
                if not nick.strip(): # Skip empty variant strings
                    continue

                tier2_nick_search_name = standardize_name_for_search(decedent_last, nick)
                
                if tier2_nick_search_name.upper() in processed_search_names:
                    ts_print(f"  [TIER_2_NICK] Variant '{nick}' results in already processed search name '{tier2_nick_search_name}'. Skipping.")
                    continue

                ts_print(f"  [TIER_2_NICK.{i+1}] Searching with standardized nickname: '{tier2_nick_search_name}' (from variant: '{nick}')")
                results_t2_nick = _execute_single_search(
                    page, 
                    tier2_nick_search_name, 
                    f"TIER_2_NICK_{nick.upper()}", 
                    search_date_from_str, 
                    search_date_to_str, 
                    tier_settings_dict.get("max_pages_per_tier", 2), 
                    overall_attempt_num
                )
                processed_search_names.add(tier2_nick_search_name.upper())
                actual_variants_searched_count += 1
                if results_t2_nick:
                    all_results_for_lead.extend(results_t2_nick)
                    # Decision Point: Stop after first successful nickname hit?
                    # ts_print(f"[INFO] Tier 2 Nickname ('{nick}') found {len(results_t2_nick)} records. Current total: {len(all_results_for_lead)}.")
                    # return all_results_for_lead # Uncomment to stop after first successful nickname variant
        else:
            ts_print(f"  [TIER_2_NICK] No common nickname variants found or applicable for '{decedent_first}'.")
    else:
        ts_print("[INFO] No first name provided for decedent; Tier 2 Nickname expansion skipped.")

    # If results were found in Tier 1 or Tier 2 Nickname, decide whether to proceed to Tier 3
    # This depends on your strategy: if any hit is good, or if you want to exhaust more precise tiers first.
    if all_results_for_lead and not tier_settings_dict.get("always_run_tier_3_if_enabled", False):
         ts_print(f"[INFO] Results found in Tier 1 or Tier 2 Nickname ({len(all_results_for_lead)} records). Skipping Tier 3 unless 'always_run_tier_3_if_enabled' is True.")
         return all_results_for_lead


    # --- TIER 3: LAST NAME ONLY (for rare surnames) ---
    # This tier will run if:
    # 1. tier_settings_dict["enable_tier_3_lastname_only"] is True AND
    # 2. No results were found in prior tiers OR tier_settings_dict["always_run_tier_3_if_enabled"] is True
    if tier_settings_dict.get("enable_tier_3_lastname_only", False):
        is_rare = _is_rare_surname(decedent_last, tier_settings_dict.get("common_surnames", set()))
        if is_rare:
            tier3_name = standardize_name_for_search(decedent_last) # Search with only last name
            ts_print(f"[TIER_3_RARE] Surname '{decedent_last}' is rare. Attempting search with: '{tier3_name}'")

            if tier3_name.upper() in processed_search_names:
                 ts_print(f"[INFO] Tier 3 name '{tier3_name}' matches an already processed search name. Skipping Tier 3.")
            else:
                results_t3 = _execute_single_search(
                    page, 
                    tier3_name, 
                    "TIER_3_RARE_LN_ONLY", 
                    search_date_from_str, 
                    search_date_to_str, 
                    tier_settings_dict.get("max_pages_per_tier", 2), 
                    overall_attempt_num
                )
                processed_search_names.add(tier3_name.upper())
                if results_t3:
                    all_results_for_lead.extend(results_t3)
                    # ts_print(f"[INFO] Tier 3 (Rare Last Name) found {len(results_t3)} records. Current total: {len(all_results_for_lead)}.")
                    # return all_results_for_lead # Usually if Tier 3 hits, it's the last resort.
        else:
            ts_print(f"[INFO] Tier 3 (LastName Only) skipped because surname '{decedent_last}' is common.")
    else:
        ts_print("[INFO] Tier 3 (LastName Only) is disabled by TIER_SETTINGS.")
        
    ts_print(f"[INFO] Completed all tiers for {decedent_last}, {decedent_first}. Total records found: {len(all_results_for_lead)}")
    return all_results_for_lead


# --- MODIFIED search_rp_for_decedent_and_extract to pass lead_dict and add lead data (v12.1) ---
def search_rp_for_decedent_and_extract(page: Page, lead_dict: dict, probate_filing_date_obj: date | None) -> list:
    decedent_last_raw = str(lead_dict.get("decedent_last","")).strip()
    decedent_first_raw = str(lead_dict.get("decedent_first","")).strip()
    
    ts_print(f"--- Starting RP Search Orchestration for: {decedent_last_raw}, {decedent_first_raw or ''} (Probate File Date: {probate_filing_date_obj}) ---")
    if not probate_filing_date_obj: 
        ts_print(f"[WARN] No valid probate filing date for {decedent_last_raw}. Skipping search."); return []
    search_date_from = probate_filing_date_obj - timedelta(days=365); search_date_to = probate_filing_date_obj + timedelta(days=365)   
    search_date_from_str = search_date_from.strftime("%m/%d/%Y"); search_date_to_str = search_date_to.strftime("%m/%d/%Y")
    all_properties_for_decedent_this_lead = []

    ts_print(f"  Initial navigation to {PORTAL_URL} for lead {decedent_last_raw}")
    try:
        page.goto(PORTAL_URL, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT_INITIAL); page.wait_for_timeout(1000)
        if not verify_rp_form_ready(page, timeout_ms=PAGE_LOAD_TIMEOUT_INITIAL // 2):
            _capture_screenshot(page, f"ts_initial_form_not_ready_{decedent_last_raw.replace(' ','_')}")
            raise RuntimeError("RP Form not ready at initial load for lead.")
    except Exception as e_initial_nav:
        ts_print(f"[ERROR] Initial navigation/form ready check failed for lead {decedent_last_raw}: {e_initial_nav}")
        _capture_screenshot(page, f"ts_initial_nav_failed_{decedent_last_raw.replace(' ','_')}")
        return [] 

    for attempt in range(MAX_SEARCH_RETRIES_TARGETED + 1):
        overall_attempt_num_for_log = attempt + 1
        ts_print(f"[ATTEMPT {overall_attempt_num_for_log} of Tiered Search] For {decedent_last_raw}, {decedent_first_raw or ''}")
        try:
            tiered_results = execute_tiered_rp_search(
                page, decedent_last_raw, decedent_first_raw or "", 
                search_date_from_str, search_date_to_str,
                TIER_SETTINGS, 
                overall_attempt_num_for_log 
            )
            if tiered_results:
                for rec in tiered_results: # Process each flattened row
                    # Add probate lead information
                    rec["probate_lead_county"] = lead_dict.get("county", "")
                    rec["probate_lead_case_number"] = lead_dict.get("case_number", "")
                    rec["probate_lead_filing_date"] = probate_filing_date_obj.strftime("%Y-%m-%d")
                    rec["probate_lead_decedent_first"] = decedent_first_raw # Original from lead
                    rec["probate_lead_decedent_last"] = decedent_last_raw   # Original from lead
                    rec["probate_lead_type_desc"] = lead_dict.get("type_desc", "")
                    rec["probate_lead_subtype"] = lead_dict.get("subtype", "")
                    rec["probate_lead_status"] = lead_dict.get("status", "")
                    rec["probate_lead_signal_strength"] = lead_dict.get("signal_strength", "")
                    
                    # Calculate RP signal strength (uses rp_prefixed keys from rec)
                    rec["rp_signal_strength"] = compute_signal_rp_score_for_record(rec, probate_filing_date_obj)
                all_properties_for_decedent_this_lead.extend(tiered_results)
                ts_print(f"[ATTEMPT {overall_attempt_num_for_log} SUCCESS] Found {len(tiered_results)} rows for {decedent_last_raw}. Total rows: {len(all_properties_for_decedent_this_lead)}")
                break 
            else:
                ts_print(f"[ATTEMPT {overall_attempt_num_for_log}] No records found after all tiers for {decedent_last_raw}.")
                if overall_attempt_num_for_log <= MAX_SEARCH_RETRIES_TARGETED :
                    ts_print(f"  No results in attempt {overall_attempt_num_for_log}. Will retry if allowed.")
        except RuntimeError as e_runtime:
             ts_print(f"[ERROR ATTEMPT {overall_attempt_num_for_log}] Runtime error for {decedent_last_raw}: {e_runtime}")
             if overall_attempt_num_for_log <= MAX_SEARCH_RETRIES_TARGETED:
                ts_print(f"  Retrying entire tiered search in {3*(overall_attempt_num_for_log)}s...")
                page.wait_for_timeout(3000 * overall_attempt_num_for_log)
             else:
                ts_print(f"[ERROR] All {MAX_SEARCH_RETRIES_TARGETED + 1} attempts failed for {decedent_last_raw}."); break
        except Exception as e_general:
            ts_print(f"[UNEXPECTED ERROR ATTEMPT {overall_attempt_num_for_log}] for {decedent_last_raw}: {e_general}")
            _capture_screenshot(page, f"ts_tiered_search_unexpected_err_att{overall_attempt_num_for_log}_{decedent_last_raw.replace(' ','_')}")
            if overall_attempt_num_for_log <= MAX_SEARCH_RETRIES_TARGETED:
                 ts_print(f"  Retrying entire tiered search in {3*(overall_attempt_num_for_log)}s...")
                 page.wait_for_timeout(3000 * overall_attempt_num_for_log)
            else:
                ts_print(f"[ERROR] All {MAX_SEARCH_RETRIES_TARGETED + 1} attempts failed for {decedent_last_raw}."); break
                
    ts_print(f"--- Finished RP Search Orchestration for: {decedent_last_raw}, {decedent_first_raw or ''}. Found {len(all_properties_for_decedent_this_lead)} rows. ---")
    return all_properties_for_decedent_this_lead

def _capture_screenshot(page, name_suffix): 
    if page and not page.is_closed(): 
        try: 
            timestamp=datetime.now().strftime('%H%M%S')
            filename = OUTPUT_DIR / f"debug_rp_targeted_{name_suffix}_{timestamp}.png"
            page.screenshot(path=filename); ts_print(f"  [SCREENSHOT] Saved: {filename.name}")
        except Exception as e_ss: ts_print(f"  [WARN _capture_screenshot] Failed: {e_ss}")

# --- MODIFIED search_rp_for_decedent_and_extract to pass lead_dict and add lead data (v12.1) ---
# ... (other functions remain the same) ...

def run_targeted_rp_scrape() -> pd.DataFrame:
    ts_print(f"--- Starting Harris County RP TARGETED Scraper (v12.1) ---")
    ts_print(f"Reading leads from: {INPUT_PROBATE_LEADS_CSV}")
    ts_print(f"Output CSV: {OUT_TARGETED_CSV}")
    ts_print(f"Tier Settings: Enable Tier 3 = {TIER_SETTINGS['enable_tier_3']}, Max Pages per Tier = {TIER_SETTINGS['max_pages_per_tier']}")
    all_found_property_records_all_leads = []
    STOP_AFTER_FIRST_SUCCESSFUL_LEAD = False
    
    try:
        probate_leads_df = pd.read_csv(INPUT_PROBATE_LEADS_CSV, sep=';', dtype=str).fillna("")
        ts_print(f"Loaded {len(probate_leads_df)} leads from CSV.")

        # --- NEW: Filter leads based on signal_strength ---
        # Ensure 'signal_strength' column exists and convert to numeric, coercing errors to NaN then to 0
        if 'signal_strength' in probate_leads_df.columns:
            probate_leads_df['signal_strength_numeric'] = pd.to_numeric(probate_leads_df['signal_strength'], errors='coerce').fillna(0)
            original_lead_count = len(probate_leads_df)
            probate_leads_df = probate_leads_df[probate_leads_df['signal_strength_numeric'] >= 3]
            filtered_lead_count = len(probate_leads_df)
            ts_print(f"Filtered leads: Kept {filtered_lead_count} of {original_lead_count} leads (signal_strength >= 3).")
        else:
            ts_print(f"[WARN] 'signal_strength' column not found in {INPUT_PROBATE_LEADS_CSV}. Processing all leads.")
        # --- End of new filter ---

        leads_to_process = probate_leads_df.to_dict('records')
        # For testing, limit to a small number of leads AFTER filtering if desired
        # leads_to_process = leads_to_process[:10] 
        
        if not leads_to_process:
            ts_print("No leads to process after filtering. Exiting.")
            return pd.DataFrame()

    except FileNotFoundError:
        ts_print(f"[FATAL] Input CSV not found: {INPUT_PROBATE_LEADS_CSV}")
        return pd.DataFrame()
    except Exception as e_csv:
        ts_print(f"[FATAL] Error reading or processing CSV: {e_csv}")
        return pd.DataFrame()
    
    with sync_playwright() as p:
        browser = None
        page_for_screenshot_context = None
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
            page = context.new_page()
            page_for_screenshot_context = page # For capturing screenshots in except blocks

            for i, lead_dict_from_csv in enumerate(leads_to_process):
                ts_print(f"--- Processing lead {i+1} of {len(leads_to_process)}: {lead_dict_from_csv.get('decedent_last','N/A')}, {lead_dict_from_csv.get('decedent_first','')} (Original Signal: {lead_dict_from_csv.get('signal_strength','N/A')}) ---")
                
                probate_filing_date_str = str(lead_dict_from_csv.get("filing_date","")).strip()
                if not str(lead_dict_from_csv.get("decedent_last","")).strip():
                    ts_print(f"[WARN] Lead {i+1} missing last name. Skip.")
                    continue
                
                probate_filing_date_obj = parse_probate_filing_date_from_input(probate_filing_date_str)
                if not probate_filing_date_obj:
                    ts_print(f"[WARN] Lead {i+1} ('{lead_dict_from_csv.get('decedent_last')}') invalid/missing filing_date ('{probate_filing_date_str}'). Skip.")
                    continue
                
                if i > 0 : page.wait_for_timeout(1000) # Small polite delay between leads
                
                property_records_this_lead = search_rp_for_decedent_and_extract(page, lead_dict_from_csv, probate_filing_date_obj)
                
                if property_records_this_lead:
                    ts_print(f"[LEAD SUCCESS] Found {len(property_records_this_lead)} property rows for {lead_dict_from_csv.get('decedent_last')}, {lead_dict_from_csv.get('decedent_first') or ''}.")
                    all_found_property_records_all_leads.extend(property_records_this_lead)
                    if STOP_AFTER_FIRST_SUCCESSFUL_LEAD:
                        ts_print(f"[INFO][TEST_MODE] STOP_AFTER_FIRST_SUCCESSFUL_LEAD is True. Stopping processing further leads.")
                        break
                else:
                    ts_print(f"--- No property records found for {lead_dict_from_csv.get('decedent_last')}, {lead_dict_from_csv.get('decedent_first') or ''}. Proceeding to next lead. ---")
        
        except PlaywrightTimeout as e_fto:
            ts_print(f"[FATAL] Playwright Timeout during scraping: {e_fto}")
            _capture_screenshot(page_for_screenshot_context, "fatal_timeout_in_main_loop")
        except PlaywrightError as e_fpw:
            ts_print(f"[FATAL] Playwright Error during scraping: {e_fpw}")
            _capture_screenshot(page_for_screenshot_context, "fatal_playwright_error_in_main_loop")
        except Exception as e_main_loop:
            ts_print(f"[FATAL] Main scraping loop failed: {e_main_loop}")
            _capture_screenshot(page_for_screenshot_context, "fatal_unexpected_error_in_main_loop")
        finally:
            ts_print("Closing browser.")
            if browser:
                try:
                    browser.close()
                except Exception as e_bc:
                    ts_print(f"[WARN] Error closing browser: {e_bc}")

    if not all_found_property_records_all_leads:
        ts_print("No property records collected overall after processing leads.")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_found_property_records_all_leads)
    ts_print(f"Total property rows collected (flattened, before final processing): {len(df)}")
    
    if not df.empty:
        cols = [
            "probate_lead_county", "probate_lead_case_number", "probate_lead_filing_date",
            "probate_lead_decedent_first", "probate_lead_decedent_last",
            "probate_lead_type_desc", "probate_lead_subtype", "probate_lead_status", "probate_lead_signal_strength",
            "rp_file_number", "rp_file_date", "rp_instrument_type",
            "rp_party_type", "rp_party_last_name", "rp_party_first_name",
            "rp_legal_description_text", "rp_legal_lot", "rp_legal_block", "rp_legal_subdivision",
            "rp_legal_abstract", "rp_legal_survey", "rp_legal_tract", "rp_legal_sec",
            "rp_signal_strength", "rp_found_by_search_term", "rp_search_tier"
        ]
        
        for c in cols:
            if c not in df:
                if c == "rp_signal_strength" or c == "probate_lead_signal_strength": df[c] = 0
                else: df[c] = ""
            else:
                if c == "rp_signal_strength" or c == "probate_lead_signal_strength":
                     # Ensure existing signal strength columns are also numeric
                    df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
                else:
                    df[c] = df[c].fillna("") # Fill NaNs in string columns
        
        df = df[cols]
        
        ts_print(f"Total property rows to save to CSV: {len(df)}")
        if not df.empty:
            df.to_csv(OUT_TARGETED_CSV, index=False, sep=';', quoting=csv.QUOTE_ALL)
            ts_print(f"Saved {len(df)} rows to {OUT_TARGETED_CSV}")
            print("--- First few records (up to 3) from the output CSV: ---")
            print(df.head(min(3, len(df))).to_string())
            print("---")
        else:
            ts_print(f"No data remained after processing. CSV '{OUT_TARGETED_CSV}' not created/updated.")
    else:
        ts_print(f"No data extracted from any lead, {OUT_TARGETED_CSV} not created.")
    
    ts_print(f"--- Harris County RP TARGETED Scraper (v12.1) Finished ---")
    return df

# ... (rest of the script, including if __name__ == "__main__":)
if __name__ == "__main__":
    run_targeted_rp_scrape()