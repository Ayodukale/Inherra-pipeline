# --- Full script with THE DEEP's LATEST extract_data_from_current_page_rp ---
import asyncio
from datetime import datetime, timedelta, date
from pathlib import Path
import re
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout, Page, Locator, Error as PlaywrightError
import time
import csv 
import json 

# --- Precompiled Regex & Constants ---
LEGAL_PATTERNS = {
    'desc': re.compile(r'(?:DESC|DESCRIPTION)\s*:\s*(.*)', re.IGNORECASE),
    'lot': re.compile(r'LOT\s*:\s*(.*)', re.IGNORECASE),
    'block': re.compile(r'BLOCK\s*:\s*(.*)', re.IGNORECASE),
    'subdivision': re.compile(r'(?:SUBD|SUBDIVISION)\s*:\s*(.*)', re.IGNORECASE)
}
LEGAL_PATTERNS.update({
    'abstract': re.compile(r'ABSTRACT\s*:\s*(.*)', re.IGNORECASE),
    'survey': re.compile(r'SURVEY\s*:\s*(.*)', re.IGNORECASE),
    'tract': re.compile(r'TRACT\s*:\s*(.*)', re.IGNORECASE)
})

PORTAL_URL = "https://cclerk.hctx.net/Applications/WebSearch/RP.aspx"
TODAY_SCRIPT_RUN = datetime.today()
# DATE_FROM_OBJ = TODAY_SCRIPT_RUN.date() - timedelta(days=90) # Default: 90 days
DATE_FROM_OBJ = TODAY_SCRIPT_RUN.date() - timedelta(days=1) # For Testing: 1 day
DATE_TO_OBJ = TODAY_SCRIPT_RUN.date()

DATE_FROM_STR = DATE_FROM_OBJ.strftime("%m/%d/%Y")
DATE_TO_STR = DATE_TO_OBJ.strftime("%m/%d/%Y")

OUTPUT_DIR = Path("data/raw"); OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUTPUT_DIR / Path(f"harris_rp_deeds_{TODAY_SCRIPT_RUN.strftime('%Y%m%d')}.csv")

MAX_RECORDS_PER_CHUNK = 400
MAX_PAGES_TO_SCRAPE_PER_CHUNK = 50 
MAX_SEARCH_RETRIES = 2
POLITE_DELAY_AFTER_PAGINATION_CLICK_S = 1
DEFAULT_TIMEOUT = 45_000
PAGE_LOAD_TIMEOUT = 90_000
SEARCH_RESULTS_TIMEOUT = 20_000 
MAX_ROWS_TO_DEBUG_HTML = 3 
MIN_MAIN_RECORD_CELLS_FLEXIBLE = 5 # Used in The Deep's new extract logic
MAX_CONSECUTIVE_EMPTY_PAGES = 3   

def ts_print(message: str): print(f"[{datetime.now().isoformat()}] {message}")
def clean_cell_text(raw_text: str) -> str:
    if raw_text is None: return ""
    return re.sub(r"\s+", " ", raw_text).strip()
def parse_party_name(name_str: str) -> dict:
    if not name_str: return {"last": "", "first": ""}
    parts = name_str.strip().split()
    if not parts: return {"last": "", "first": ""}
    return {"last": parts[0], "first": " ".join(parts[1:]) if len(parts) > 1 else ""}
def compute_signal_rp(instrument_type: str) -> int: return 0
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

def verify_rp_form_ready(page: Page): 
    ts_print("[DEBUG verify_rp_form_ready] Verifying RP form context...")
    try:
        page.locator('input[name="ctl00$ContentPlaceHolder1$txtFrom"]').wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        page.locator('input[name="ctl00$ContentPlaceHolder1$txtTo"]').wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        search_btn = page.locator('input[name="ctl00$ContentPlaceHolder1$btnSearch"]')
        search_btn.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        if not search_btn.is_enabled(timeout=DEFAULT_TIMEOUT): ts_print("[WARN verify_rp_form_ready] Search button not enabled."); return False
        ts_print("[DEBUG verify_rp_form_ready] RP Form inputs appear usable."); return True
    except PlaywrightTimeout: ts_print("[WARN verify_rp_form_ready] RP Inputs/Btn not vis/enabled (timeout)."); return False
    except Exception as e: ts_print(f"[ERROR verify_rp_form_ready] Error: {e}"); return False

def locate_results_table_rp(page: Page) -> Locator | None: 
    ts_print("[DEBUG locate_table_rp] Finding results table...")
    selectors = [
        "table:has(tr:has-text('File Number'))",  
        "table:has(tr:has-text('RP-'))",          
        "table#ItemPlaceholderContainer",         
        "table.table-striped.table-condensed"     
    ]
    located_table: Locator | None = None; successful_selector = ""
    for i, selector_str in enumerate(selectors):
        ts_print(f"[DEBUG locate_table_rp] Attempting selector {i+1}: {selector_str}")
        current_locator = page.locator(selector_str).first 
        if current_locator.count() > 0: 
            try:
                if current_locator.is_visible(timeout=5000): 
                    ts_print(f"[INFO locate_table_rp] Found visible table using selector: {selector_str}")
                    located_table = current_locator; successful_selector = selector_str; break 
            except PlaywrightTimeout: ts_print(f"[DEBUG] Selector {selector_str} found but not visible quickly.")
            except Exception as e_vis: ts_print(f"[DEBUG] Error checking visibility for {selector_str}: {e_vis}")
        else: ts_print(f"[DEBUG] Selector {selector_str} did not yield any elements.")
            
    if not located_table:
        no_records_loc = page.locator("text=/No Records Found/i") 
        if no_records_loc.count() > 0:
            try:
                if no_records_loc.first.is_visible(timeout=3000): ts_print("[INFO] 'No Records Found' detected."); return None
            except: pass 
        ts_print("[WARN] No suitable results table found, and no clear 'No Records' message.");
        page.screenshot(path=OUTPUT_DIR / "debug_locate_table_rp_failed.png"); return None
    try:
        if located_table.locator("tr").count() > 0: 
            ts_print(f"[DEBUG] Chosen table (found by '{successful_selector}') has {located_table.locator('tr').count()} tr elements."); return located_table
        else: ts_print(f"[WARN] Chosen table (by '{successful_selector}') has no tr elements."); return None
    except Exception as e_val: ts_print(f"[WARN] Error validating chosen table: {e_val}"); return None

def perform_search_and_get_count_rp(page: Page, date_from_str: str, date_to_str: str) -> int: 
    ts_print(f"[DEBUG p_search_rp] Search: {date_from_str}-{date_to_str}")
    ts_print(f"[DEBUG p_search_rp] Navigating to {PORTAL_URL} for fresh state.")
    try:
        page.goto(PORTAL_URL, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT); page.wait_for_timeout(3000) 
    except Exception as e_goto: ts_print(f"[ERROR] Navigation error: {e_goto}"); raise RuntimeError(f"Nav failed: {PORTAL_URL}") from e_goto
    if not verify_rp_form_ready(page):
        page.screenshot(path=OUTPUT_DIR / f"debug_form_not_usable_{date_from_str.replace('/', '-')}.png")
        raise RuntimeError(f"RP Form for {date_from_str}-{date_to_str} inputs not usable.")
    page.locator('input[name="ctl00$ContentPlaceHolder1$txtFrom"]').fill(date_from_str)
    page.locator('input[name="ctl00$ContentPlaceHolder1$txtTo"]').fill(date_to_str)
    search_button = page.locator('input[name="ctl00$ContentPlaceHolder1$btnSearch"]')
    ts_print(f"[DEBUG p_search_rp] Clicking search..."); search_button.click()
    ts_print(f"[DEBUG p_search_rp] Waiting for results indication (table or 'No Records')...")
    try: 
        page.wait_for_selector("table:has(tr:has-text('File Number'))", state="attached", timeout=SEARCH_RESULTS_TIMEOUT)
        ts_print("[DEBUG] Table indication (header 'File Number') attached.")
    except PlaywrightTimeout:
        ts_print("[DEBUG] Table indication not attached. Checking for 'No Records'.")
        try: page.wait_for_selector("text=/No Records Found/i", state="attached", timeout=5000); ts_print("[DEBUG] 'No Records' attached.")
        except PlaywrightTimeout: ts_print("[WARN] Timeout: Neither table nor 'No Records' attached quickly.")
    page.wait_for_timeout(3000) 
    ts_print(f"[DEBUG] Capturing screenshot/HTML for {date_from_str}-{date_to_str} post-wait.")
    page.screenshot(path=OUTPUT_DIR / f"debug_after_search_click_{date_from_str.replace('/', '-')}.png")
    try:
        page_content_for_debug = page.content() # Capture content for debug
        with open(OUTPUT_DIR / f"debug_after_search_click_{date_from_str.replace('/', '-')}.html", "w", encoding="utf-8") as f: f.write(page_content_for_debug)
        ts_print(f"[DEBUG] HTML content saved. Length: {len(page_content_for_debug)} chars")
    except Exception as e_html: ts_print(f"[WARN] Could not write HTML debug file: {e_html}")

    table_for_counting = locate_results_table_rp(page); record_count = -1
    if table_for_counting:
        ts_print("[INFO] Table located for counting."); 
        count_banner_loc = page.locator("span#ctl00_ContentPlaceHolder1_lblCount")
        if count_banner_loc.count() > 0 and count_banner_loc.is_visible(timeout=2000):
            match = re.search(r"(\d+)", clean_cell_text(count_banner_loc.inner_text()))
            if match: record_count = int(match.group(1)); ts_print(f"[INFO] Parsed {record_count} from banner.")
            else: ts_print(f"[WARN] Banner text not matched. Estimate."); record_count = -1
        else: record_count = -1
        if record_count == -1:
            try:
                visible_rows = table_for_counting.locator("tr:not(:first-child):has(td)").count() 
                ts_print(f"[DEBUG] Visible data-like rows for estimation: {visible_rows}")
                if visible_rows > 0:
                    next_btn = page.locator("#ctl00_ContentPlaceHolder1_BtnNext")
                    if is_button_disabled(next_btn): record_count = visible_rows; ts_print(f"[INFO] Exact count (single page): {record_count}")
                    else: record_count = visible_rows * 3; ts_print(f"[INFO] Est. count (multi-page): ~{record_count}")
                else: record_count = 0; ts_print(f"[INFO] Table located, but no data-like rows for estimation.")
            except Exception as e_est: ts_print(f"[WARN] Error estimating from rows: {e_est}. Fallback."); record_count = MAX_RECORDS_PER_CHUNK // 2
        return record_count if record_count >= 0 else 0
    else: ts_print("[INFO] No table located by locate_results_table_rp. Assuming 0 records."); return 0

def perform_search_with_retry_rp(page: Page, date_from_str: str, date_to_str: str, max_retries=MAX_SEARCH_RETRIES) -> int:
    for attempt in range(max_retries):
        try:
            return perform_search_and_get_count_rp(page, date_from_str, date_to_str)
        except RuntimeError as e:
            ts_print(f"[WARN retry_rp] Attempt {attempt + 1} failed with RuntimeError for {date_from_str}-{date_to_str}: {e}")
            if attempt < max_retries - 1: ts_print(f"[WARN retry_rp] Retrying..."); page.wait_for_timeout(3000 * (attempt + 1))
            else: ts_print(f"[ERROR retry_rp] Final attempt failed with RuntimeError."); raise
        except PlaywrightTimeout as e_timeout_main:
            ts_print(f"[WARN retry_rp] Attempt {attempt + 1} PlaywrightTimeout for {date_from_str}-{date_to_str}: {e_timeout_main}")
            if attempt < max_retries - 1: ts_print(f"[WARN retry_rp] Retrying..."); page.wait_for_timeout(3000 * (attempt + 1))
            else: ts_print(f"[ERROR retry_rp] Final attempt failed with PlaywrightTimeout."); raise RuntimeError(f"Max retries PlaywrightTimeout on {date_from_str}-{date_to_str}") from e_timeout_main
    return 0

# THIS IS THE DEEP'S LATEST VERSION OF extract_data_from_current_page_rp
def extract_data_from_current_page_rp(table_locator: Locator, page_num: int) -> list:
    ts_print(f"[DEBUG extract_data_rp] P{page_num}: Starting extraction with enhanced debugging")
    recs = []
    
    all_trs = table_locator.locator("tr").all()
    num_total_trs = len(all_trs)
    ts_print(f"[DEBUG extract_data_rp] P{page_num}: Found {num_total_trs} total <tr> elements")

    if num_total_trs == 0:
        ts_print(f"[WARN extract_data_rp] P{page_num}: No <tr> elements found")
        return []

    k = 0
    while k < num_total_trs:
        current_tr = all_trs[k]
        td_elements = current_tr.locator("td")
        num_tds = td_elements.count()

        # Enhanced debugging for first 20 rows
        if k < 20: # Log details for the first 20 TRs encountered
            ts_print(f"[TR_DEBUG P{page_num}R{k+1}] Cells: {num_tds}")
            try:
                for j in range(min(num_tds, 5)):  # Log first 5 cells
                    cell_text = clean_cell_text(td_elements.nth(j).inner_text(timeout=1000))
                    ts_print(f"  Cell {j}: '{cell_text[:50]}'")  # Print first 50 chars
            except Exception as e_debug_detail:
                ts_print(f"  Error getting detailed TR debug info for P{page_num}R{k+1}: {e_debug_detail}")
        
        # More flexible main record detection (The Deep's logic)
        # MIN_MAIN_RECORD_CELLS_FLEXIBLE is 5
        if num_tds >= MIN_MAIN_RECORD_CELLS_FLEXIBLE:  
            try:
                file_number_text = ""
                # Check multiple cells for file number pattern
                for cell_idx in [0, 1, 2]:  # Check first 3 cells
                    if cell_idx < num_tds: # Ensure cell index is valid
                        try:
                            text = clean_cell_text(td_elements.nth(cell_idx).inner_text(timeout=1000))
                            if any(text.startswith(prefix) for prefix in ["RP-", "RM-", "RT-"]):
                                file_number_text = text
                                break
                        except: continue # Ignore error for this cell, try next

                if file_number_text: # If a file number was found
                    ts_print(f"[MAIN_RECORD P{page_num}R{k+1}] Found file#: {file_number_text}")
                    current_record = {
                        "file_number": file_number_text,
                        # These indices need to be relative to where File Number was found, or absolute if consistent
                        # Assuming a somewhat consistent main row structure if File Number is found:
                        "file_date": clean_cell_text(td_elements.nth(1).inner_text(timeout=2000)) if num_tds > 1 else "",
                        "instrument_type": clean_cell_text(td_elements.nth(2).inner_text(timeout=2000)).split()[0] if num_tds > 2 and td_elements.nth(2).inner_text(timeout=100) else "",
                        "grantors": [], "grantees": [], "trustees": []
                    }
                    
                    legal_desc_raw = ""
                    # Try to find legal description in subsequent cells (e.g., 3, 4, 5 if File# is 0)
                    for cell_idx in range(1, num_tds): # Start from cell 1 if File# is 0, or adjust
                        if cell_idx < num_tds:
                            try:
                                text = clean_cell_text(td_elements.nth(cell_idx).inner_text(timeout=1000))
                                if "DESC:" in text.upper() or "DESCRIPTION:" in text.upper() or len(text) > 50 : # Heuristic for legal desc
                                    legal_desc_raw = text #  This might grab more than just legal desc if not careful
                                    # A better approach is to find the specific legal desc cell based on its content or a more unique preceding cell
                                    # For now, let's assume it's often in cell index 4 if File# is 0
                                    if num_tds > 4: legal_desc_raw = clean_cell_text(td_elements.nth(4).inner_text(timeout=2000))
                                    break
                            except: continue
                    
                    parsed_legal = {}; remaining_lines = []
                    if legal_desc_raw:
                        for line in legal_desc_raw.splitlines():
                            ls=line.strip(); matched=False
                            for key,p in LEGAL_PATTERNS.items():
                                m=p.search(ls)
                                if m: parsed_legal[key]=clean_cell_text(m.group(1)); matched=True; break
                            if not matched and ls: remaining_lines.append(ls)
                    if 'desc' not in parsed_legal and remaining_lines: parsed_legal['desc']=" ".join(remaining_lines)
                    elif 'desc' in parsed_legal and remaining_lines: parsed_legal['desc']=(parsed_legal['desc']+" "+" ".join(remaining_lines)).strip()
                    
                    current_record.update({
                        "legal_description_text": parsed_legal.get('desc',""), "legal_lot": parsed_legal.get('lot',""),
                        "legal_block": parsed_legal.get('block',""), "legal_subdivision": parsed_legal.get('subdivision',""),
                        "legal_abstract": parsed_legal.get('abstract',""), "legal_survey": parsed_legal.get('survey',""),
                        "legal_tract": parsed_legal.get('tract',""), "signal_strength_rp": compute_signal_rp(current_record["instrument_type"])
                    })

                    # Process sub-rows
                    k_sub_loop_start_index = k + 1
                    k = k_sub_loop_start_index # Advance outer loop k to where sub-loop starts

                    for k_sub_idx in range(k_sub_loop_start_index, num_total_trs):
                        sub_tr = all_trs[k_sub_idx]
                        sub_tds_loc = sub_tr.locator("td")
                        sub_num_tds = sub_tds_loc.count()
                        
                        # Check if we hit next main record
                        if sub_num_tds >= MIN_MAIN_RECORD_CELLS_FLEXIBLE: # Use flexible cell count
                            try:
                                next_file_check_text = ""
                                for sub_cell_idx_check in [0,1,2]: # Check first few cells of sub_tr
                                    if sub_cell_idx_check < sub_num_tds:
                                        text_check = clean_cell_text(sub_tds_loc.nth(sub_cell_idx_check).inner_text(timeout=500))
                                        if any(text_check.startswith(prefix) for prefix in ["RP-", "RM-", "RT-"]):
                                            next_file_check_text = text_check
                                            break
                                if next_file_check_text: # If a file number pattern is found
                                    ts_print(f"[DEBUG extract_data_rp] Next main record ({next_file_check_text}) found at TR index {k_sub_idx}. Ending current block.")
                                    k = k_sub_idx - 1 # Set outer k to point to this new main record for next outer loop iteration
                                    break # Break from sub-row processing loop
                            except: pass # Ignore errors in checking for next main record, continue parsing sub-rows
                        
                        # Process Grantor/Grantee/Trustee rows (typically 2 cells)
                        if sub_num_tds == 2:
                            try:
                                label = clean_cell_text(sub_tds_loc.nth(0).inner_text(timeout=1000)).upper()
                                value_cell_loc = sub_tds_loc.nth(1)
                                value = clean_cell_text(value_cell_loc.inner_text(timeout=1000))
                                span_in_td = value_cell_loc.locator("span")
                                if span_in_td.count() > 0: value = clean_cell_text(span_in_td.first.inner_text(timeout=1000))
                                
                                if "GRANTOR" in label: current_record["grantors"].append(parse_party_name(value))
                                elif "GRANTEE" in label: current_record["grantees"].append(parse_party_name(value))
                                elif "TRUSTEE" in label: current_record["trustees"].append(parse_party_name(value))
                            except Exception as e_sub: ts_print(f"[WARN extract_data_rp] Error processing sub-row at TR index {k_sub_idx}: {e_sub}")
                        elif sub_num_tds != 0 and sub_num_tds < MIN_MAIN_RECORD_CELLS_FLEXIBLE : # Not 0, not 2, not a main record
                             ts_print(f"[DEBUG extract_data_rp] TR index {k_sub_idx} has {sub_num_tds} cells, unusual sub-row. Possibly end of block.")
                             # If it's not a known pattern, it might be the end of the current record's group
                             # We could break here, or make this more intelligent based on observed patterns
                             # For safety, let's assume it could be the end of relevant sub-rows.
                             # k = k_sub_idx -1 # To re-evaluate this row in the outer loop if needed
                             # break # This break was too aggressive, let it try the next sub_tr
                        
                        if k_sub_idx == num_total_trs - 1: # If this is the very last TR in the table
                            k = k_sub_idx # Make sure outer loop k is set to the end.
                            # No break here, let it fall through to add the current_record

                    current_record["grantors"] = json.dumps(current_record["grantors"])
                    current_record["grantees"] = json.dumps(current_record["grantees"])
                    current_record["trustees"] = json.dumps(current_record["trustees"])
                    recs.append(current_record)
                    # k is already advanced (or set to re-evaluate next main) by the inner loop.
                    # The outer loop's k += 1 will then correctly move to the next starting point.
                    continue # Crucial: continue to the next iteration of the outer while loop
                
            except Exception as e_main_proc:
                ts_print(f"[ERROR extract_data_rp] Error processing potential main record at TR index {k}: {e_main_proc}")
                if k < MAX_ROWS_TO_DEBUG_HTML:
                    try: ts_print(f"[DEBUG extract_data_rp] HTML of problematic row: {current_tr.inner_html(timeout=1000)}")
                    except: pass
        k += 1 
    ts_print(f"[INFO extract_data_rp] P{page_num}: Extracted {len(recs)} records from {num_total_trs} TRs.")
    return recs

def scrape_records_for_date_range_rp(page: Page, date_from: date, date_to: date) -> list:
    all_recs_for_range = []
    df_s, dt_s = date_from.strftime('%m/%d/%Y'), date_to.strftime('%m/%d/%Y')
    ts_print(f"[scrape_range_rp CALLED] {df_s}-{dt_s}, Days:{(date_to-date_from).days}")
    rec_cnt_for_chunk = perform_search_with_retry_rp(page, df_s, dt_s)
    ts_print(f"[INFO scrape_range_rp] Initial search for {df_s}-{dt_s} indicates {rec_cnt_for_chunk} records (approx).")
    if rec_cnt_for_chunk == 0: ts_print(f"[INFO scrape_range_rp] No records for {df_s}-{dt_s}."); return []
    if rec_cnt_for_chunk > MAX_RECORDS_PER_CHUNK and (date_to - date_from).days > 0:
        ts_print(f"[INFO] Splitting {df_s}-{dt_s} ({rec_cnt_for_chunk} recs).")
        mid_days = (date_to - date_from).days // 2; mid_dt = date_from + timedelta(days=mid_days)
        if mid_dt < date_from: mid_dt = date_from
        if mid_dt >= date_to: mid_dt = date_to - timedelta(days=1) if (date_to - timedelta(days=1)) >= date_from else date_from
        if date_from <= mid_dt: all_recs_for_range.extend(scrape_records_for_date_range_rp(page, date_from, mid_dt))
        next_start = mid_dt + timedelta(days=1)
        if next_start <= date_to: all_recs_for_range.extend(scrape_records_for_date_range_rp(page, next_start, date_to))
        return all_recs_for_range
    current_chunk_records = []; table_l = locate_results_table_rp(page)
    if not table_l:
        ts_print(f"[ERROR] No table located for {df_s}-{dt_s} pagination start.");
        page.screenshot(path=OUTPUT_DIR / f"debug_no_table_pagin_start_{df_s.replace('/', '-')}.png"); return []
    page_scrape_count = 0; consecutive_empty_pages = 0
    first_record_text_selector_relative = "tr:not(:has(th)):first-of-type td:first-child" 
    if table_l.locator("tbody").count() > 0 : first_record_text_selector_relative = "tbody tr:not(:has(th)):first-of-type td:first-child"
    previous_first_record_text = f"INITIAL_SENTINEL_{df_s}_{dt_s}"
    while page_scrape_count < MAX_PAGES_TO_SCRAPE_PER_CHUNK:
        current_page_display_num = page_scrape_count + 1
        ts_print(f"[INFO pagin_rp] Scraping P{current_page_display_num} for {df_s}-{dt_s}...")
        try: table_l.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        except PlaywrightTimeout: ts_print(f"[ERROR] P{current_page_display_num}: Table not visible."); page.screenshot(path=OUTPUT_DIR/f"debug_pg_tbl_not_vis_p{current_page_display_num}.png"); break
        current_page_first_rec_text = ""
        if table_l.count() > 0 :
            first_rec_loc = table_l.locator(first_record_text_selector_relative)
            if first_rec_loc.count() > 0:
                try: current_page_first_rec_text = clean_cell_text(first_rec_loc.first.inner_text(timeout=10000))
                except: pass 
        if page_scrape_count > 0 and current_page_first_rec_text and current_page_first_rec_text == previous_first_record_text:
            ts_print(f"[INFO] P{current_page_display_num}: First record '{current_page_first_rec_text}' SAME. End."); break
        previous_first_record_text = current_page_first_rec_text
        page_data = extract_data_from_current_page_rp(table_l, current_page_display_num)
        if not page_data: 
            consecutive_empty_pages += 1
            ts_print(f"[WARN pagin_rp] P{current_page_display_num}: No main records extracted. Consecutive empty: {consecutive_empty_pages}")
            if consecutive_empty_pages >= MAX_CONSECUTIVE_EMPTY_PAGES: ts_print(f"[INFO] Reached {MAX_CONSECUTIVE_EMPTY_PAGES} consecutive empty pages. Stop."); break
        else: consecutive_empty_pages = 0 
        current_chunk_records.extend(page_data)
        ts_print(f"[INFO] P{current_page_display_num}: Extracted {len(page_data)} main records. Chunk total: {len(current_chunk_records)}")
        next_btn = page.locator("#ctl00_ContentPlaceHolder1_BtnNext") 
        if next_btn.count() == 0: ts_print(f"[INFO] P{current_page_display_num}: Next btn not found by ID. Last page."); break
        if is_button_disabled(next_btn): ts_print(f"[INFO] P{current_page_display_num}: Next btn disabled. Last page."); break
        page_scrape_count += 1
        if page_scrape_count >= MAX_PAGES_TO_SCRAPE_PER_CHUNK: ts_print(f"[WARN] Reached MAX_PAGES for {df_s}-{dt_s}."); break
        ts_print(f"[INFO] Clicking Next for P{page_scrape_count + 1}...")
        try:
            next_btn.click(timeout=DEFAULT_TIMEOUT)
            page.wait_for_timeout(POLITE_DELAY_AFTER_PAGINATION_CLICK_S * 1000 + 2000) 
            ts_print(f"[DEBUG] Waiting for page content update after Next...")
            page.wait_for_load_state("domcontentloaded", timeout=DEFAULT_TIMEOUT + 10000) 
            ts_print(f"[INFO] P{page_scrape_count + 1} content likely updated.")
            table_l = locate_results_table_rp(page) 
            if not table_l: ts_print(f"[ERROR] Failed to re-locate table on P{page_scrape_count + 1}."); page.screenshot(path=OUTPUT_DIR/f"debug_pg_no_table_p{page_scrape_count + 1}.png"); break
        except PlaywrightTimeout as e_next_to: ts_print(f"[WARN] Timeout Next click/wait for P{page_scrape_count + 1}: {e_next_to}."); page.screenshot(path=OUTPUT_DIR/f"debug_pg_nav_timeout_p{page_scrape_count + 1}.png"); break
        except Exception as e_next: ts_print(f"[ERROR] Error clicking Next/waiting: {e_next}."); page.screenshot(path=OUTPUT_DIR/f"debug_pg_nav_err_p{page_scrape_count + 1}.png"); break
    all_recs_for_range.extend(current_chunk_records); return all_recs_for_range

def _capture_screenshot(page, name_suffix): 
    if page and not page.is_closed(): 
        try: page.screenshot(path=OUTPUT_DIR / f"debug_rp_{name_suffix}.png")
        except Exception as e_ss: ts_print(f"[WARN _capture_screenshot] Failed to take screenshot {name_suffix}: {e_ss}")

def run_rp_scrape() -> pd.DataFrame:
    ts_print(f"--- Starting Harris County RP Scraper ---"); ts_print(f"Target: {DATE_FROM_STR} to {DATE_TO_STR}"); ts_print(f"Output: {OUT_CSV}")
    all_scraped_records = []; browser = None; page_for_screenshot = None
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
            context.set_default_timeout(DEFAULT_TIMEOUT); page = context.new_page()
            page_for_screenshot = page 
            all_scraped_records = scrape_records_for_date_range_rp(page, DATE_FROM_OBJ, DATE_TO_OBJ)
        except PlaywrightTimeout as e_fatal_to: ts_print(f"[FATAL] Playwright Timeout: {e_fatal_to}"); _capture_screenshot(page_for_screenshot, "fatal_timeout")
        except PlaywrightError as e_fatal_pw: ts_print(f"[FATAL] Playwright Error: {e_fatal_pw}"); _capture_screenshot(page_for_screenshot, "fatal_playwright_error")
        except Exception as e_main: ts_print(f"[FATAL] Main loop failed: {e_main}"); _capture_screenshot(page_for_screenshot, "fatal_unexpected_error")
        finally: 
            ts_print("Closing browser.")
            if browser: 
                try: browser.close()
                except Exception as e_close: ts_print(f"[WARN run_rp_scrape] Error closing browser: {e_close}")
    if not all_scraped_records: ts_print("No records extracted overall."); return pd.DataFrame()
    df = pd.DataFrame(all_scraped_records); ts_print(f"Total records scraped before dedupe: {len(df)}")
    if not df.empty:
        cols = ["file_number","file_date","instrument_type","grantors","grantees","trustees","legal_description_text","legal_lot","legal_block","legal_subdivision","legal_abstract","legal_survey","legal_tract","signal_strength_rp"]
        for c in cols: 
            if c not in df: df[c] = "[]" if c in ["grantors","grantees","trustees"] else ""
            else: 
                 if c in ["grantors","grantees","trustees"]: df[c] = df[c].fillna("[]")
                 else: df[c] = df[c].fillna("")
        df = df[cols]; df.drop_duplicates(subset=["file_number","file_date","instrument_type"],keep="first",inplace=True)
        ts_print(f"Records after dedupe: {len(df)}")
        if not df.empty: df.to_csv(OUT_CSV,index=False,sep=';',quoting=csv.QUOTE_ALL); ts_print(f"Saved {len(df)} to {OUT_CSV}"); print(df.head(3).to_string())
        else: ts_print(f"No data after dedupe. {OUT_CSV} not created.")
    else: ts_print(f"No data extracted, {OUT_CSV} not created.")
    ts_print("--- Harris County RP Scraper Finished ---"); return df

if __name__ == "__main__":
    run_rp_scrape()