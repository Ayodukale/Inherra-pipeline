from datetime import datetime, timedelta, date
from pathlib import Path
import re
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import spacy
import time
import csv # <--- IMPORT CSV FOR QUOTING CONSTANTS

# ---------------------------------------------------------------
# harris_scraper_v0.27.py — Robust CSV Quoting during Export
# ---------------------------------------------------------------

# --- Precompiled Regex & Constants ---
BLOCKLIST = {"IN THE GUARDIANSHIP OF", "IN THE MATTER OF", "RE ESTATE OF", "IN THE GUARDIANSHIP", "IN THE CONSERVATORSHIP OF"}
ESTATE_REGEX = re.compile(r"ESTATE OF[:\s]*(.+?)(?:,|\s+DECEASED)", re.IGNORECASE)
SUFFIX_REGEX = re.compile(r"\s+(?:Jr\.?|Sr\.?|I{2,3}|IV)$", re.IGNORECASE)
TITLE_CASE_REGEX = re.compile(r"\b[A-Za-z][A-Za-z'’\-]+(?:\s+[A-Za-z][A-Za-z'’\-]+)+\b")

MAX_RECORDS_PER_CHUNK = 400
MAX_PAGES_TO_SCRAPE_PER_CHUNK = 50
MAX_FRAME_DETECTION_ATTEMPTS = 3
MAX_SEARCH_RETRIES = 2 
POLITE_DELAY_AFTER_PAGINATION_CLICK_S = 1 

PORTAL_URL = "https://cclerk.hctx.net/applications/websearch/courtsearch.aspx?casetype=probate"
TODAY_SCRIPT_RUN = datetime.today() 
DATE_FROM_STR = (TODAY_SCRIPT_RUN - timedelta(days=90)).strftime("%m/%d/%Y") 
DATE_TO_STR = TODAY_SCRIPT_RUN.strftime("%m/%d/%Y")
OUT_CSV = Path("harris_sample.csv")

_nlp = None
def get_nlp(): # Condensed
    global _nlp
    if _nlp is None: _nlp = spacy.load("en_core_web_sm", disable=["parser", "tagger", "textcat"])
    return _nlp

def clean_cell_text(raw_text: str) -> str: # Condensed
    return re.sub(r"\s+", " ", raw_text).strip()

def ts_print(message: str): 
    print(f"[{datetime.now().isoformat()}] {message}")

def extract_decedent(text: str): # Condensed
    nlp=get_nlp(); substr=SUFFIX_REGEX.sub("",text.strip());
    if text.isupper(): substr=substr.title()
    for p in BLOCKLIST:
        if substr.upper().startswith(p): substr=substr[len(p):].strip(":, "); break
    doc=nlp(substr); persons=[ent.text for ent in doc.ents if ent.label_=="PERSON"]
    if persons: span=max(persons,key=len);parts=span.split();return (" ".join(parts[:-1]),parts[-1]) if len(parts)>=2 else (parts[0],"")
    m=ESTATE_REGEX.search(substr)
    if m: parts=m.group(1).split();return (" ".join(parts[:-1]),parts[-1]) if len(parts)>=2 else (parts[0],"")
    runs=TITLE_CASE_REGEX.findall(substr)
    if runs: name=max(runs,key=len);parts=name.split();return (" ".join(parts[:-1]),parts[-1]) if len(parts)>=2 else (parts[0],"")
    toks=[t for t in substr.replace(',',' ').split() if t.isalpha()];fillers={'ESTATE','IN','THE','OF','DECEASED'}
    toks=[t for t in toks if t.upper() not in fillers]
    if len(toks)>=2: return " ".join(toks[:-1]),toks[-1]
    return "",""

def compute_signal(type_desc: str, subtype: str) -> int: # Condensed
    if not hasattr(compute_signal,"_logged"): compute_signal._logged=set()
    combo=f"{type_desc} {subtype}".lower().strip()
    if not combo:
        if "_EMPTY" not in compute_signal._logged: ts_print("[DEBUG] Empty combo");compute_signal._logged.add("_EMPTY")
        return 0
    if any(kw in combo for kw in ["probate of will","letters testamentary","application for probate"]): return 5
    if "independent administration" in combo and ("with will annexed" in combo or "heirship" in combo): return 4
    if "ancillary administration" in combo: return 3
    if "dependent administration" in combo or "will deposit" in combo: return 2
    if "muniment of title" in combo: return 1
    if combo not in compute_signal._logged: ts_print(f"[DEBUG] No signal: '{combo}'");compute_signal._logged.add(combo)
    return 0

def extract_data_from_current_page(table_locator, idx: dict, page_num: int) -> list: # Condensed
    ts_print(f"[DEBUG extract_data] P{page_num}: Starting.");recs=[];rows=table_locator.locator("tbody tr")
    rc=rows.count();ts_print(f"[DEBUG extract_data] P{page_num}: Found {rc} rows.")
    if rc==0:return[]
    for k in range(rc):
        tr=rows.nth(k);td_el=tr.locator("td");num_tds=td_el.count()
        if num_tds==0:continue
        cells=[clean_cell_text(td_el.nth(j).inner_text()) for j in range(num_tds)]
        if len(cells)<len(idx):cells+=[""]*(len(idx)-len(cells))
        try:
            case_v=cells[idx["Case"]].strip();
            if not case_v:continue
            style=cells[idx["Style"]];parties=cells[idx["Parties"]] if "Parties" in idx and idx["Parties"]<len(cells)else""
            type_d=cells[idx["Type Desc"]];sub_t=cells[idx["Subtype"]];f_dt=cells[idx["File Date"]];stat=cells[idx["Status"]]
        except(IndexError,KeyError)as e:ts_print(f"[WARN extract_data]P{page_num}R{k}:Cell err({e}).Cells:{cells},Idx:{idx}");continue
        fn,ln=extract_decedent(f"{style} {parties}".strip())
        if not fn and not ln and f"{style} {parties}".strip()and not hasattr(extract_data_from_current_page,"_n_dbg"):
            ts_print(f"[DEBUG]Blank name:{style} {parties}");extract_data_from_current_page._n_dbg=True
        recs.append({"county":"Harris","case_number":case_v,"filing_date":f_dt,"decedent_first":fn,"decedent_last":ln,
                     "type_desc":type_d,"subtype":sub_t,"status":stat,"signal_strength":compute_signal(type_d,sub_t)})
    return recs

def wait_for_form_ready_after_clear(page, timeout=15000): # (v0.24)
    ts_print("[DEBUG wait_for_form_ready_after_clear] Waiting for form to be ready...")
    try:
        page.wait_for_function("""() => {
            const directInput = document.querySelector('input[id$="txtFrom"]');
            if (directInput && directInput.offsetParent !== null) return true;
            const iframe = document.querySelector('iframe[name="SearchCriteria"]');
            if (!iframe) {
                 const anyIframeWithInput = Array.from(document.querySelectorAll('iframe')).find(
                    f => f.contentDocument && f.contentDocument.querySelector('input[id$="txtFrom"]')
                 );
                 if (anyIframeWithInput) return true;
                 return false;
            }
            return iframe.contentDocument && iframe.contentDocument.readyState === 'complete' &&
                   iframe.contentDocument.querySelector('input[id$="txtFrom"]');
        }""", timeout=timeout)
        ts_print("[DEBUG wait_for_form_ready_after_clear] Form appears ready.")
        return True
    except PlaywrightTimeout: ts_print("[WARN wait_for_form_ready_after_clear] Timed out."); page.screenshot(path="debug_form_not_ready_post_clear.png"); return False

def pick_search_frame(page, attempt=0): # (v0.24)
    sel = "input[id$='txtFrom']"; ts_print(f"[DEBUG pick_search_frame attempt {attempt+1}]")
    if attempt >= MAX_FRAME_DETECTION_ATTEMPTS:
        ts_print("[DEBUG pick_search_frame] MAX ATTEMPTS. Dumping frame info:")
        try:
            frames_info_js = page.evaluate("""() => Array.from(document.querySelectorAll('iframe')).map(f => ({name: f.name, id: f.id, src: f.src,ready: f.contentDocument && f.contentDocument.readyState === 'complete'}))""")
            ts_print(f"[DEBUG pick_search_frame] JS Frame diagnostics: {frames_info_js}")
            ts_print(f"[DEBUG pick_search_frame] Playwright Frames names: {[f.name for f in page.frames if f.name]}")
            ts_print(f"[DEBUG pick_search_frame] Playwright Frames URLs: {[f.url for f in page.frames]}")
        except Exception as e_diag: ts_print(f"[DEBUG pick_search_frame] Frame diagnostics failed: {e_diag}")
        page.screenshot(path="debug_frame_search_exhausted.png"); raise RuntimeError("❌ pick_search_frame: Max attempts locating date inputs.")
    try: 
        if page.locator(sel).first.is_visible(timeout=5000): ts_print("[DEBUG pick_search_frame] Found inputs on main page."); return page
    except Exception as e_main_chk: ts_print(f"[DEBUG pick_search_frame] Main page check: {e_main_chk}")
    ts_print(f"[DEBUG pick_search_frame attempt {attempt+1}] Checking {len(page.frames)} Playwright frame(s)...")
    for frame_obj in page.frames:
        try: 
            if "CourtSearch_R.aspx" not in frame_obj.url and "courtsearch.aspx" not in frame_obj.url.lower(): continue
            ts_print(f"[DEBUG pick_search_frame] Checking relevant frame: Name='{frame_obj.name}', URL='{frame_obj.url}'")
            frame_obj.wait_for_load_state(state="domcontentloaded", timeout=5000)
            if frame_obj.locator(sel).first.is_visible(timeout=3000): ts_print(f"[DEBUG pick_search_frame] Found inputs in frame: '{frame_obj.name}'"); return frame_obj
        except Exception as e_frame_chk: ts_print(f"[DEBUG pick_search_frame] Frame '{frame_obj.name}' check failed: {e_frame_chk}")
    wait_time = min(2000, 500 * (attempt + 1)); ts_print(f"[DEBUG pick_search_frame attempt {attempt+1}] Inputs not found. Wait {wait_time}ms..."); page.wait_for_timeout(wait_time)
    return pick_search_frame(page, attempt + 1)

def verify_form_ready(form_context, page_for_fallback_search_btn): # (v0.24)
    ts_print(f"[DEBUG verify_form_ready] Verifying form context...");
    try:
        form_context.locator("input[id$='txtFrom']").wait_for(state="visible", timeout=7000)
        form_context.locator("input[id$='txtTo']").wait_for(state="visible", timeout=7000)
        search_btn_locs = ['input[id$="btnSearchCase"]', 'input[name*="btnSearch"][type="submit"]']
        s_btn_ok = False
        for sel_s in search_btn_locs: 
            btn = form_context.locator(sel_s).first
            if btn.count() > 0 and btn.is_visible(timeout=1000) and btn.is_enabled(timeout=1000): s_btn_ok = True; break
        if not s_btn_ok and form_context is not page_for_fallback_search_btn: 
             for sel_s in search_btn_locs:
                btn = page_for_fallback_search_btn.locator(sel_s).first 
                if btn.count() > 0 and btn.is_visible(timeout=1000) and btn.is_enabled(timeout=1000): s_btn_ok = True; break
        if not s_btn_ok: ts_print(f"[WARN verify_form_ready] Search button not vis/enabled."); 
        ts_print(f"[DEBUG verify_form_ready] Form inputs appear usable."); return True
    except PlaywrightTimeout: ts_print(f"[WARN verify_form_ready] Inputs/Btn not vis/enabled (timeout)."); return False
    except Exception as e: ts_print(f"[ERROR verify_form_ready] Error: {e}"); return False

def perform_search_and_get_count(page, date_from_str: str, date_to_str: str) -> int: # (v0.24)
    ts_print(f"[DEBUG p_search] Search: {date_from_str}-{date_to_str}")
    if hasattr(perform_search_and_get_count, "_first_call_v26"): 
        ts_print("[DEBUG p_search] Subsequent search - navigating to PORTAL_URL for fresh state.")
        page.goto(PORTAL_URL, wait_until="networkidle", timeout=60000); page.wait_for_timeout(2000)
    else: ts_print("[DEBUG p_search] First call to perform_search_and_get_count.")
    perform_search_and_get_count._first_call_v26 = True
    form_context = pick_search_frame(page) 
    if not verify_form_ready(form_context, page): 
        page.screenshot(path=f"debug_form_not_usable_{date_from_str.replace('/', '-')}.png"); raise RuntimeError(f"Form for {date_from_str}-{date_to_str} located but inputs not usable.")
    try: 
        eval_arg=form_context if form_context is not page else None 
        form_state=form_context.evaluate("(node)=>{const d=node?(node.contentDocument||node):document;const fI=d.querySelector('input[id$=\"txtFrom\"]');const tI=d.querySelector('input[id$=\"txtTo\"]');const sB=d.querySelector('input[id$=\"btnSearchCase\"]')||d.querySelector('input[name*=\"btnSearch\"][type=\"submit\"]');return{fF:!!fI,tF:!!tI,sF:!!sB,fV:fI?(fI.offsetParent!==null):!1,tV:tI?(tI.offsetParent!==null):!1,sV:sB?(sB.offsetParent!==null):!1}}",eval_arg)
        ts_print(f"[DEBUG p_search] JS Form state:{form_state}")
        if not(form_state.get('fF')and form_state.get('fV')):ts_print(f"[WARN p_search]JS:FromDate missing/not vis for {date_from_str}-{date_to_str}.")
    except Exception as e_fse:ts_print(f"[WARN p_search]Err JS form state eval:{e_fse}")
    form_context.locator("input[id$='txtFrom']").fill(date_from_str);form_context.locator("input[id$='txtTo']").fill(date_to_str)
    s_sels=["input[id$='btnSearchCase']","input[name*='btnSearch'][type='submit']"];s_b=None 
    for s_sel_opt in[form_context,page]:
        for s in s_sels:
            if s_sel_opt.locator(s).count()>0 and s_sel_opt.locator(s).first.is_visible(timeout=1000):s_b=s_sel_opt.locator(s).first;break
        if s_b:break
    if not s_b:
        s_b_r_f=form_context.get_by_role("button",name="Search",exact=True);s_b_r_m=page.get_by_role("button",name="Search",exact=True)
        if s_b_r_f.count()>0 and s_b_r_f.is_visible(timeout=1000):s_b=s_b_r_f.first
        elif s_b_r_m.count()>0 and s_b_r_m.is_visible(timeout=1000):s_b=s_b_r_m.first
        else:page.screenshot(path=f"debug_no_srch_btn_{date_from_str.replace('/','-')}.png");raise RuntimeError("❌ No search button.")
    ts_print(f"[DEBUG p_search]Clicking search:{date_from_str}-{date_to_str}.");s_b.click()
    rc_loc=page.locator("span#ctl00_ContentPlaceHolder1_lblCount");nd_loc=page.locator(r"text=/No\s*(data|Records)\s*found\.?/i")
    try: 
        st=time.time();tout_s=30;outcome=False;p_cnt=-1;ts_print(f"[DEBUG p_search]Wait outcome:{date_from_str}-{date_to_str}...")
        while time.time()-st<tout_s:
            if rc_loc.count()>0 and rc_loc.is_visible(timeout=200):
                b_txt=rc_loc.inner_text().strip();ts_print(f"[DDEBUG]ID Banner rpr:{repr(b_txt)}|'{b_txt}'")
                m_rgx=r"(\d+)\s*Record\(s\)\s*Found\.?";match=re.search(m_rgx,b_txt,re.IGNORECASE);ts_print(f"[DDEBUG]Regex'{m_rgx}'on'{b_txt}'.Match:{match}")
                if match:p_cnt=int(match.group(1));ts_print(f"[INFO]Parsed {p_cnt}(ID banner)");outcome=True;break
                elif re.search(r"No\s*(data|Records)\s*found\.?",b_txt,re.IGNORECASE):ts_print(f"[INFO]ID banner 'No data':'{b_txt}'");p_cnt=0;outcome=True;break
                else:ts_print(f"[WARN]ID banner unexpected:'{b_txt}'")
            if not outcome and nd_loc.count()>0:
                for i in range(nd_loc.count()):
                    nd_inst=nd_loc.nth(i)
                    if nd_inst.is_visible(timeout=200):nd_txt=nd_inst.inner_text().strip();ts_print(f"[INFO]Generic 'No data':'{nd_txt}'");p_cnt=0;outcome=True;break
                if outcome:break
            if not outcome:page.wait_for_timeout(500)
        if not outcome:page.screenshot(path=f"debug_tout_outcome_{date_from_str.replace('/','-')}.png");raise RuntimeError(f"Timeout outcome:{date_from_str}-{date_to_str}")
        if p_cnt==-1:page.screenshot(path=f"debug_crit_pcnt_{date_from_str.replace('/','-')}.png");raise RuntimeError(f"Logic err p_cnt:{date_from_str}-{date_to_str}")
        return p_cnt
    except Exception as e_gen:ts_print(f"[ERROR]Banner general err {date_from_str}-{date_to_str}:{e_gen}");page.screenshot(path=f"debug_bnr_gen_err_{date_from_str.replace('/','-')}.png");raise

def perform_search_with_retry(page, date_from_str: str, date_to_str: str, max_retries=MAX_SEARCH_RETRIES) -> int: # (v0.24)
    for attempt in range(max_retries):
        try: return perform_search_and_get_count(page,date_from_str,date_to_str)
        except RuntimeError as e:
            if ("date‐range inputs" in str(e) or "Form located but inputs not usable" in str(e)) and attempt<max_retries-1 : 
                ts_print(f"[WARN retry] Attempt {attempt+1} form err:{e}. Forcing nav for next attempt...");page.screenshot(path=f"debug_retry_force_nav_att{attempt+1}_{date_from_str.replace('/','-')}.png")
                page.goto(PORTAL_URL, wait_until="networkidle", timeout=60000);page.wait_for_timeout(3000) 
                if hasattr(perform_search_and_get_count,"_first_call_v26"): del perform_search_and_get_count._first_call_v26 
                continue
            else: ts_print(f"[ERROR retry] Final attempt {attempt+1} fail/unrecoverable:{e}");raise
    return 0 

def locate_results_table(page): # (v0.24)
    ts_print("[DEBUG locate_table] Finding table...");
    try: 
        pri_loc=page.locator("table#itemPlaceholderContainer")
        if pri_loc.count()>0 and pri_loc.first.is_visible(timeout=5000):ts_print("[DEBUG locate_table] Found by ID.");return pri_loc
    except Exception as e_pri:ts_print(f"[DEBUG locate_table] Err primary ID:{e_pri}")
    try: 
        bnr=page.locator("span#ctl00_ContentPlaceHolder1_lblCount")
        if bnr.count()>0 and bnr.first.is_visible(timeout=2000):
            tbls_aft=bnr.locator("xpath=following::table[.//thead[.//th]]")
            for i in range(tbls_aft.count()):
                t=tbls_aft.nth(i);
                if t.is_visible(timeout=1000) and t.locator("thead tr th").count() > 1: ts_print(f"[DEBUG locate_table] Fallback1: vis table {i} post-banner.");return t
    except Exception as e_bnr: ts_print(f"[DEBUG locate_table] Err near banner:{e_bnr}")
    try: 
        pot_tbls_case=page.locator("table:has(thead tr th:has-text('Case'))")
        for i in range(pot_tbls_case.count()):
            t=pot_tbls_case.nth(i);
            if t.is_visible(timeout=3000) and t.locator("thead tr th").count() > 1:ts_print(f"[DEBUG locate_table] Fallback2: vis table {i} with 'Case'.");return t
    except Exception as e_case: ts_print(f"[DEBUG locate_table] Err 'Case' table:{e_case}")
    try: 
        gen_tbls=page.locator("table:has(thead tr th)")
        for i in range(gen_tbls.count()):
            t=gen_tbls.nth(i);
            if t.locator("thead tr th").count()>2 and t.is_visible(timeout=1000):ts_print(f"[DEBUG locate_table] Final: vis table {i} >2hdrs.");return t
        if gen_tbls.count()>0 and gen_tbls.first.is_visible(timeout=1000):ts_print("[DEBUG locate_table] Final: 1st generic vis table.");return gen_tbls.first
    except Exception as e_gen:ts_print(f"[DEBUG locate_table] Err generic table:{e_gen}")
    page.screenshot(path="debug_FAIL_locate_table.png");raise RuntimeError("locate_table: No results table found.")

# --- NEW PAGINATION HELPER (for v0.25/v0.26) ---
def is_button_disabled(button_locator): # Takes a Playwright Locator
    if not button_locator or button_locator.count() == 0: ts_print("[DEBUG is_btn_disabled] No button found by locator."); return True 
    try:
        # Check a few common ways a button/link might be "disabled"
        if not button_locator.is_visible(timeout=1000): ts_print("[DEBUG is_btn_disabled] Button not visible."); return True 
        if not button_locator.is_enabled(timeout=1000): ts_print("[DEBUG is_btn_disabled] Button not enabled (by Playwright)."); return True
        
        # Specifically for <a> tags, check for 'disabled' attribute as seen in HTML
        # Evaluate runs JS in the browser context
        is_anchor_disabled = button_locator.evaluate("el => el.tagName.toLowerCase() === 'a' && el.hasAttribute('disabled')")
        if is_anchor_disabled:
            ts_print("[DEBUG is_btn_disabled] Found <a> tag with 'disabled' attribute.")
            return True
            
    except PlaywrightTimeout: ts_print("[DEBUG is_btn_disabled] Timeout checking button state, assuming disabled."); return True 
    except Exception as e: ts_print(f"[ERROR is_btn_disabled] Error checking button: {e}"); return True 
    ts_print("[DEBUG is_btn_disabled] Button appears active and enabled."); return False

def scrape_records_for_date_range(page, date_from: date, date_to: date, headers_idx: dict) -> list: # Updated for v0.26
    all_recs=[];df_s=date_from.strftime('%m/%d/%Y');dt_s=date_to.strftime('%m/%d/%Y');ts_print(f"[scrape_range CALLED] {df_s}-{dt_s}, Days:{(date_to-date_from).days}")
    rec_cnt=perform_search_with_retry(page,df_s,dt_s);ts_print(f"[INFO scrape_range] Found {rec_cnt} for {df_s}-{dt_s}")
    if rec_cnt==0:return[]
    if rec_cnt>MAX_RECORDS_PER_CHUNK and (date_to-date_from).days>0: # Date chunking
        ts_print(f"[INFO] Splitting {df_s}-{dt_s}({rec_cnt} recs).");mid_d=(date_to-date_from).days//2;mid_dt=date_from+timedelta(days=mid_d)
        if mid_dt<date_from:mid_dt=date_from
        if mid_dt>=date_to:mid_dt=date_to-timedelta(days=1) if(date_to-timedelta(days=1))>=date_from else date_from
        if date_from<=mid_dt:all_recs.extend(scrape_records_for_date_range(page,date_from,mid_dt,headers_idx))
        s_h_from=mid_dt+timedelta(days=1)
        if s_h_from<=date_to:all_recs.extend(scrape_records_for_date_range(page,s_h_from,date_to,headers_idx))
        return all_recs
    
    tbl_l=None 
    if rec_cnt > 0: 
        try: ts_print(f"[DEBUG scrape_range] Validating primary table vis for {df_s}-{dt_s}..."); page.locator("table#itemPlaceholderContainer").wait_for(state="visible",timeout=7_000)
        except PlaywrightTimeout: ts_print(f"[WARN scrape_range] Primary table not quick vis {df_s}-{dt_s}. locate_table will try.")
    try: tbl_l=locate_results_table(page); ts_print(f"[INFO scrape_range] Table located for {df_s}-{dt_s} pagin start.")
    except RuntimeError as e_ntps: ts_print(f"[ERROR scrape_range] No table {df_s}-{dt_s}({rec_cnt} recs):{e_ntps}");return[]
    
    pg_s=0;n_b_sels=["a.pgr:has-text('Next')","input[type='submit'][value='Next']","input[type='button'][value='Next']","a:has-text('Next')","button:has-text('Next')"]
    previous_first_record_text = f"INITIAL_SENTINEL_FOR_CHUNK_{df_s}_{dt_s}" 

    while pg_s<MAX_PAGES_TO_SCRAPE_PER_CHUNK: 
        c_p_d=pg_s+1;ts_print(f"[INFO pagin] Scraping p{c_p_d} for {df_s}-{dt_s}...")
        try:tbl_l.wait_for(state="visible",timeout=7000)
        except PlaywrightTimeout:ts_print(f"[ERROR] P{c_p_d}: Tbl not vis.");page.screenshot(path=f"debug_pg_tbl_not_vis_p{c_p_d}.png");break
        
        current_page_data = extract_data_from_current_page(tbl_l,headers_idx,c_p_d)
        if not current_page_data and pg_s > 0 : ts_print(f"[WARN pagin] P{c_p_d}: Extracted no data. End of records?"); break
        all_recs.extend(current_page_data)
        
        current_first_record_text = "";first_rec_sel="table#itemPlaceholderContainer tbody tr:first-child td:first-child a.doclinks"
        try: 
            if current_page_data: 
                if tbl_l.locator(first_rec_sel).count() > 0 : current_first_record_text = tbl_l.locator(first_rec_sel).inner_text().strip()
        except Exception as e_gt_curr: ts_print(f"[WARN pagin] P{c_p_d}: Could not get 1st rec text: {e_gt_curr}")

        if pg_s > 0 and current_first_record_text and current_first_record_text == previous_first_record_text:
            ts_print(f"[INFO pagin] P{c_p_d}: First record ('{current_first_record_text}') SAME as previous. End of unique results.")
            break
        previous_first_record_text = current_first_record_text 
        # pg_s incremented after successful scrape AND decision to continue to next page
        
        next_b=None 
        for s in n_b_sels: 
            p_b=page.locator(s).first
            if p_b.count()>0 and p_b.is_visible(timeout=1000): next_b=p_b; ts_print(f"[DEBUG] Next btn candidate:'{s}'"); break
        
        if is_button_disabled(next_b): 
            ts_print(f"[INFO pagin] P{c_p_d}: No active/enabled Next button. Last page."); break 
        
        # If we are here, Next button exists and is not disabled. Increment page count for *next* page.
        pg_s+=1 # Increment page counter as we are about to attempt to go to the next page

        ts_print(f"[INFO pagin] Clicking Next for logical page {pg_s+1} (actual attempt for page {c_p_d+1})...");
        next_b.click() 
        ts_print(f"[{datetime.now().isoformat()}] [DEBUG pagin] Clicked Next. Polite delay..."); page.wait_for_timeout(POLITE_DELAY_AFTER_PAGINATION_CLICK_S * 1000)
        
        page_transitioned_successfully = False
        try: 
            ts_print("[DEBUG pagin] Wait network idle post-Next...");page.wait_for_load_state("networkidle",timeout=15_000)
            if previous_first_record_text: # Use the text from the page we *just scraped* for comparison
                js_s=first_rec_sel.replace("'","\\'");js_t_c=previous_first_record_text.replace("'","\\'");
                ts_print(f"[DEBUG] Wait 1st rec change from '{previous_first_record_text}'...")
                page.wait_for_function(f"()=>{{const fl=document.querySelector('{js_s}');return fl&&fl.innerText.trim()!== '{js_t_c}';}}",timeout=10_000)
                ts_print(f"[INFO] P{pg_s+1}(1st rec changed)loaded.")
            else:ts_print(f"[INFO] P{pg_s+1}(net idle,no prior txt to compare)loaded.")
            page_transitioned_successfully = True
        except PlaywrightTimeout:ts_print(f"[WARN] Timeout p{pg_s+1} content change/load.");page.screenshot(path=f"debug_pg_tout_p{pg_s+1}.png");break
        except Exception as e_wf:ts_print(f"[ERROR] P{pg_s+1} transition err:{e_wf}");page.screenshot(path=f"debug_pg_err_p{pg_s+1}.png");break

        if page_transitioned_successfully:
            ts_print(f"[DEBUG] Re-locating table for p{pg_s+1}...");
            try: tbl_l=locate_results_table(page);ts_print(f"[INFO] Table re-located for p{pg_s+1}.")
            except RuntimeError as e_nrt:ts_print(f"[ERROR] Fail re-locate table p{pg_s+1}:{e_nrt}");break 
        else: ts_print(f"[WARN pagin] Page transition to p{pg_s+1} not confirmed. Stopping.");break
            
    if pg_s>=MAX_PAGES_TO_SCRAPE_PER_CHUNK:ts_print(f"[WARN] Max pgs {MAX_PAGES_TO_SCRAPE_PER_CHUNK} for {df_s}-{dt_s}.")
    return all_recs

# --- NEW HELPER for v0.26 ---
def get_known_good_date() -> str:
    dt = TODAY_SCRIPT_RUN - timedelta(days=3) # Start 3 days ago from script run
    for _ in range(7): 
        if dt.weekday() < 5: return dt.strftime("%m/%d/%Y") # Monday=0, Friday=4
        dt -= timedelta(days=1)
    return (TODAY_SCRIPT_RUN - timedelta(days=1)).strftime("%m/%d/%Y") # Fallback

def run_scrape() -> pd.DataFrame: 
    ts_print(f"[SETUP DEBUG] TODAY_SCRIPT_RUN is: {TODAY_SCRIPT_RUN.strftime('%m/%d/%Y')}")
    all_s_recs=[];init_df=datetime.strptime(DATE_FROM_STR,"%m/%d/%Y").date();init_dt=datetime.strptime(DATE_TO_STR,"%m/%d/%Y").date()
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True);page=browser.new_page();page.goto(PORTAL_URL,timeout=60_000)
        
        known_good_date_str = get_known_good_date()
        ts_print(f"[INFO] Initial header search using known-good date: {known_good_date_str}")
        H_FROM = known_good_date_str; H_TO = known_good_date_str
        
        cnt_h=perform_search_with_retry(page,H_FROM,H_TO)
        if cnt_h==0:page.screenshot(path="debug_FAIL_hdr_NO_RECS_ON_KNOWN_GOOD_DATE.png");browser.close();raise RuntimeError(f"Known-good date search ({H_FROM}) no recs retry.")
        
        try: 
            id_bnr_chk=page.locator("span#ctl00_ContentPlaceHolder1_lblCount")
            if id_bnr_chk.is_visible(timeout=5000):
                act_bnr_txt=id_bnr_chk.inner_text().strip();ts_print(f"[DEBUG run_scrape] Banner post-fixed search(ID loc):'{act_bnr_txt}'")
                if not re.search(r"\d+\s*Record\(s\)\s*Found\.?",act_bnr_txt):page.screenshot(path="debug_FAIL_hdr_ID_bnr_not_cnt.png");browser.close();raise RuntimeError(f"Fixed hdr search({H_FROM})ID bnr not cnt:'{act_bnr_txt}'.")
            else:
                ts_print("[WARN run_scrape] ID banner not quickly visible for sanity check, trying general.")
                gen_bnr_loc=page.locator(r"text=/(\d+\s*Record\(s\)\s*Found\.?|No\s*(data|Records)\s*found\.?)/i") 
                gen_bnr_loc.first.wait_for(state="visible",timeout=10000);act_bnr_txt=gen_bnr_loc.first.inner_text().strip()
                ts_print(f"[DEBUG run_scrape] Banner post-fixed search(gen loc):'{act_bnr_txt}'")
                if re.search(r"No\s*(data|Records)\s*found",act_bnr_txt,re.IGNORECASE):page.screenshot(path="debug_FAIL_hdr_got_no_recs_bnr.png");browser.close();raise RuntimeError(f"Fixed hdr search({H_FROM})'No data'bnr, though count was {cnt_h}.")
        except PlaywrightTimeout:page.screenshot(path="debug_FAIL_hdr_bnr_tout.png");browser.close();raise RuntimeError("Banner sanity chk fail post-FIXED search.")
        
        ts_print("[INFO run_scrape] Locating table for headers...");
        try:tbl_f_h=locate_results_table(page);ts_print(f"[INFO run_scrape] Header table located.")
        except RuntimeError as e_ntfh:ts_print(f"[ERROR run_scrape] Fail locate table for headers:{e_ntfh}");browser.close();raise
        
        h_elms=tbl_f_h.locator("thead tr th")
        if h_elms.count()==0:page.screenshot(path="debug_FAIL_no_th_in_hdr_tbl.png");browser.close();raise RuntimeError(f"No th elm in chosen table.HTML:{tbl_f_h.inner_html(timeout=2000)}")
        h_list=[clean_cell_text(h_elms.nth(i).inner_text()) for i in range(h_elms.count())];h_idx={h:j for j,h in enumerate(h_list)};ts_print(f"[INFO]Hdrs:{h_idx}")
        REQ_FLDS=["Case","File Date","Type Desc","Subtype","Status","Style"];missing=[f for f in REQ_FLDS if f not in h_idx]
        if missing:browser.close();raise ValueError(f"Missing hdrs:{', '.join(missing)}.Found:{h_list}")
        
        ts_print(f"[INFO] Scraping target range:{DATE_FROM_STR}-{DATE_TO_STR}");all_s_recs=scrape_records_for_date_range(page,init_df,init_dt,h_idx);browser.close() 
    df=pd.DataFrame(all_s_recs)
    if not df.empty:df.drop_duplicates(subset=["case_number"],keep="first",inplace=True);df.to_csv(OUT_CSV,index=False,sep=';', quoting=csv.QUOTE_ALL) # Added QUOTE_ALL
    else:ts_print(f"No data extracted,{OUT_CSV} not created/updated.")
    return df

if __name__ == "__main__":
    ts_print(f"--- Starting Harris County Probate Scraper v0.26 ---");ts_print(f"Target Date Range: {DATE_FROM_STR} to {DATE_TO_STR}");df_res=run_scrape()
    if not df_res.empty:ts_print(f"Saved {len(df_res)} unique rows to {OUT_CSV}");ts_print("Sample:");ts_print(df_res.head())
    else:ts_print("No records processed or matched criteria.")