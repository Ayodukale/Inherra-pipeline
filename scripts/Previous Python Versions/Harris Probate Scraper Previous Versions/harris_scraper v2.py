from datetime import datetime, timedelta
from pathlib import Path
import re
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import spacy

# ---------------------------------------------------------------
# harris_scraper_v0_16.py  —  Robust row skipping, refined signal, parties handling
# ---------------------------------------------------------------
# Ensure you’re using the 'inherra' Conda env (Python 3.11+) with spaCy & model installed.

# Precompile regex patterns for performance
BLOCKLIST = {
    "IN THE GUARDIANSHIP OF",
    "IN THE MATTER OF",
    "RE ESTATE OF",
    "IN THE GUARDIANSHIP",
    "IN THE CONSERVATORSHIP OF",
}
ESTATE_REGEX = re.compile(r"ESTATE OF[:\s]*(.+?)(?:,|\s+DECEASED)", re.IGNORECASE)
SUFFIX_REGEX = re.compile(r"\s+(?:Jr\.?|Sr\.?|I{2,3}|IV)$", re.IGNORECASE)
TITLE_CASE_REGEX = re.compile(r"\b[A-Za-z][A-Za-z'’\-]+(?:\s+[A-Za-z][A-Za-z'’\-]+)+\b")

# Lazy spaCy loading
_nlp = None

def get_nlp():
    global _nlp
    if _nlp is None:
        _nlp = spacy.load("en_core_web_sm", disable=["parser", "tagger", "textcat"])
    return _nlp

def pick_search_frame(page):
    sel = "input[id$='txtFrom']"
    if page.locator(sel).count():
        return page
    lvl1 = page.frame_locator("iframe[name='SearchCriteria']")
    if lvl1.locator(sel).count():
        return lvl1
    nested = lvl1.frame_locator("iframe").first
    if nested.locator(sel).count():
        return nested
    raise RuntimeError("❌ Could not locate the date‐range inputs.")

def extract_decedent(text: str):
    nlp = get_nlp()
    # Apply suffix removal first
    substr = SUFFIX_REGEX.sub("", text.strip())

    # If original text was all uppercase, title case the suffix-stripped version
    if text.isupper(): # Check original text's case
        substr = substr.title() # Title case the current state of substr

    # Remove leading blocklist phrases from the (potentially title-cased) substr
    for prefix in BLOCKLIST:
        if substr.upper().startswith(prefix): # Compare with upper case of substr
            substr = substr[len(prefix):].strip(":, ")
            break

    # 1️⃣ NER on the cleaned substr
    doc = nlp(substr)
    persons = [ent.text for ent in doc.ents if ent.label_ == "PERSON"]
    if persons:
        span = max(persons, key=lambda s: len(s.split()))
        parts = span.split()
        if len(parts) >= 2:
            return " ".join(parts[:-1]), parts[-1]
        return parts[0], ""

    # 2️⃣ Regex fallback on the cleaned substr
    m = ESTATE_REGEX.search(substr)
    if m:
        parts = m.group(1).split()
        if len(parts) >= 2:
            return " ".join(parts[:-1]), parts[-1]
        return parts[0], ""

    # 3️⃣ Title-case run fallback on the cleaned substr
    runs = TITLE_CASE_REGEX.findall(substr)
    if runs:
        name = max(runs, key=lambda s: len(s.split()))
        parts = name.split()
        if len(parts) >= 2:
            return " ".join(parts[:-1]), parts[-1]
        return parts[0], ""

    # 4️⃣ Final fallback: generic token filter on the cleaned substr
    tokens = [t for t in substr.replace(',', ' ').split() if t.isalpha()]
    fillers = {'ESTATE','IN','THE','OF','DECEASED'}
    tokens = [t for t in tokens if t.upper() not in fillers]
    if len(tokens) >= 2:
        return " ".join(tokens[:-1]), tokens[-1]

    return "", ""


def compute_signal(type_desc: str, subtype: str) -> int:
    # Ensure _logged attribute exists on the function object
    if not hasattr(compute_signal, "_logged"):
        compute_signal._logged = set()

    combo = f"{type_desc} {subtype}".lower().strip() # .strip() here

    if not combo: # check if combo is empty after strip
        if "_EMPTY" not in compute_signal._logged: # Check the attribute directly
            print("[DEBUG] Empty type/subtype combo.")
            compute_signal._logged.add("_EMPTY")   # Modify the attribute directly
        return 0
    
    # Keyword checks
    if any(kw in combo for kw in ["probate of will", "letters testamentary", "application for probate"]):
        return 5
    if "independent administration" in combo and ("with will annexed" in combo or "heirship" in combo):
        return 4
    if "ancillary administration" in combo:
        return 3
    if "dependent administration" in combo or "will deposit" in combo:
        return 2
    if "muniment of title" in combo:
        return 1
    
    # If no specific signal matched, log if new and return 0
    if combo not in compute_signal._logged: # Check the attribute
        print(f"[DEBUG] No signal match for: '{combo}'")
        compute_signal._logged.add(combo)   # Modify the attribute
    return 0


PORTAL_URL = (
    "https://cclerk.hctx.net/applications/websearch/"
    "courtsearch.aspx?casetype=probate"
)
TODAY     = datetime.today()
DATE_FROM = (TODAY - timedelta(days=90)).strftime("%m/%d/%Y")
DATE_TO   = TODAY.strftime("%m/%d/%Y")
OUT_CSV   = Path("harris_sample.csv")

def run_scrape() -> pd.DataFrame:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(PORTAL_URL, timeout=60_000)

        form = pick_search_frame(page)
        form.locator("input[id$='txtFrom']").fill(DATE_FROM)
        form.locator("input[id$='txtTo']").fill(DATE_TO)
        form.locator("input[id$='btnSearchCase']").click()

        try:
            banner = page.locator(r"text=/\d+ Record\(s\) Found\./")
            banner.wait_for(state="visible", timeout=30_000)
        except PlaywrightTimeout:
            browser.close() # Close browser on timeout before raising
            raise RuntimeError("Record-count banner never appeared.")

        tables = banner.locator("xpath=following::table")
        table = tables.nth(1)
        table.wait_for(state="visible", timeout=30_000)

        headers_elements = table.locator("thead tr th")
        headers = [headers_elements.nth(i).inner_text().strip() for i in range(headers_elements.count())]
        idx = {h: j for j, h in enumerate(headers)}

        REQUIRED_FIELDS = ["Case", "File Date", "Type Desc", "Subtype", "Status", "Style"]
        missing = [f for f in REQUIRED_FIELDS if f not in idx]
        if missing:
            browser.close() # Close browser before raising
            raise ValueError(f"Missing expected headers: {', '.join(missing)}. Found: {', '.join(headers)}")

        rows = table.locator("tbody tr")
        recs = []
        for k in range(rows.count()):
            tr = rows.nth(k)
            
            td_elements = tr.locator("td")
            num_actual_tds = td_elements.count()

            # Skip if the table row (<tr>) contains no data cells (<td>)
            if num_actual_tds == 0:
                # print(f"[DEBUG] Skipping row {k} (0-indexed) as it has no <td> elements.")
                continue 
            
            cells = [re.sub(r"\s+", " ", td_elements.nth(j).inner_text()).strip() for j in range(num_actual_tds)]

            # Pad if the row has some cells, but fewer than the number of headers
            if len(cells) < len(headers):
                cells += ["" for _ in range(len(headers) - len(cells))]

            # Skip if a critical identifier (e.g., Case Number) is empty after extraction
            case_number_val = cells[idx["Case"]].strip()
            if not case_number_val:
                # print(f"[DEBUG] Skipping row {k} (0-indexed) due to empty 'Case' value. Cells content: {cells}")
                continue

            style_txt = cells[idx["Style"]]
            
            # Robust handling for "Parties" column (which is not in REQUIRED_FIELDS)
            parties_txt = "" # Default to empty
            if "Parties" in idx: # Check if "Parties" header was actually found
                parties_txt = cells[idx["Parties"]]
            else:
                # Optional: Log if "Parties" header is missing, if this is unexpected
                # if not hasattr(run_scrape, "_parties_header_missing_logged"):
                # print("[DEBUG] 'Parties' header not found in table. Proceeding without Parties data.")
                # run_scrape._parties_header_missing_logged = True
                pass # parties_txt remains ""

            combined = f"{style_txt} {parties_txt}"
            fn, ln = extract_decedent(combined)
            
            # Debug for blank names, if combined text was not empty
            if not fn and not ln and combined.strip(): 
                if not hasattr(run_scrape, "_name_debugged"):
                    print(f"[DEBUG] Blank name from combined: {combined}")
                    run_scrape._name_debugged = True
            
            type_desc_val = cells[idx["Type Desc"]]
            subtype_val = cells[idx["Subtype"]]
            
            recs.append({
                "county": "Harris",
                "case_number": case_number_val, # Use the stripped one
                "filing_date": cells[idx["File Date"]],
                "decedent_first": fn,
                "decedent_last": ln,
                "type_desc": type_desc_val,
                "subtype": subtype_val,
                "status": cells[idx["Status"]],
                "signal_strength": compute_signal(type_desc_val, subtype_val)
            })

        browser.close()
        df = pd.DataFrame(recs)
        if not df.empty: # Only save if there's data
            df.to_csv(OUT_CSV, index=False, sep=';')
        else:
            print(f"No data extracted, {OUT_CSV} will not be created/updated.")
        return df

if __name__ == "__main__":
    df = run_scrape()
    if not df.empty: # Check if DataFrame is not empty
        print(f"Saved {len(df)} rows to {OUT_CSV}")
        print(df.head())
    else:
        print("No records were processed or matched the criteria.")