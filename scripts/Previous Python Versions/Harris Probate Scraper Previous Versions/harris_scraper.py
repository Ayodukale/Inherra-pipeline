from datetime import datetime, timedelta
from pathlib import Path
import re
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import spacy

# ---------------------------------------------------------------
# harris_scraper_v0_12.py  —  Final: Style-first NER + fallback to Parties
# ---------------------------------------------------------------
# Ensure you’re using the 'inherra' Conda env (Python 3.11+) with spaCy & model installed.

# Load spaCy English small model (keep only NER + tok2vec)
nlp = spacy.load(
    "en_core_web_sm", disable=["parser", "tagger", "textcat"]
)

PORTAL_URL = (
    "https://cclerk.hctx.net/applications/websearch/"
    "courtsearch.aspx?casetype=probate"
)
TODAY     = datetime.today()
DATE_FROM = (TODAY - timedelta(days=90)).strftime("%m/%d/%Y")
DATE_TO   = TODAY.strftime("%m/%d/%Y")
OUT_CSV   = Path("harris_sample.csv")


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
    raise RuntimeError("Could not locate date-range inputs.")


def extract_decedent(text: str):
    """
    1) NER-based: extract PERSON entities and choose the longest span.
    2) Regex fallback for 'Estate of...Deceased'.
    3) Title-case run fallback.
    Returns (first_name, last_name) or ('','').
    """
    # NER
    doc = nlp(text)
    persons = [ent.text for ent in doc.ents if ent.label_ == "PERSON"]
    if persons:
        span = max(persons, key=lambda s: len(s.split()))
        parts = span.split()
        if len(parts) >= 2:
            return parts[0], parts[-1]
        return parts[0], ""

    # regex fallback
    m = re.search(r"ESTATE OF[:\s]*(.+?)(?:,|\s+DECEASED)", text, re.IGNORECASE)
    if m:
        parts = m.group(1).strip().split()
        if len(parts) >= 2:
            return parts[0], parts[-1]
        return parts[0], ""

    # title-case run fallback
    runs = re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b", text)
    if runs:
        name = max(runs, key=lambda s: len(s.split()))
        parts = name.split()
        return parts[0], parts[-1]

    return "", ""


def compute_signal(type_desc: str, subtype: str) -> int:
    combo = f"{type_desc} {subtype}".lower()
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
    return 0


def run_scrape() -> pd.DataFrame:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(PORTAL_URL, timeout=60_000)

        # 1) Fill date range and execute search
        form = pick_search_frame(page)
        form.locator("input[id$='txtFrom']").fill(DATE_FROM)
        form.locator("input[id$='txtTo']").fill(DATE_TO)
        form.locator("input[id$='btnSearchCase']").click()

        # 2) Wait for banner
        try:
            banner = page.locator(r"text=/\d+ Record\(s\) Found\./")
            banner.wait_for(state="visible", timeout=30_000)
        except PlaywrightTimeout:
            browser.close()
            raise RuntimeError("Record-count banner never appeared.")

        # 3) Identify results table
        tables = banner.locator("xpath=following::table")
        table = None
        for i in range(tables.count()):
            cand = tables.nth(i)
            if cand.locator("xpath=./thead/tr/th").count():
                table = cand
                break
        if not table and tables.count()>1:
            table = tables.nth(1)
        if not table:
            browser.close()
            raise RuntimeError("Results table not found.")
        table.wait_for(state="visible", timeout=30_000)

        # 4) Build headers map
        head = table.locator("xpath=./thead/tr/th")
        if head.count():
            headers = [head.nth(i).inner_text().strip() for i in range(head.count())]
        else:
            r1 = table.locator("xpath=./tbody/tr[1]/td")
            headers = [r1.nth(i).inner_text().strip() for i in range(r1.count())]
        idx = {h: j for j,h in enumerate(headers)}
        print("Detected headers:", headers)

        # 5) Extract rows
        rows = table.locator("xpath=./tbody/tr")
        start = 1 if head.count() else 0
        recs = []
        for k in range(start, rows.count()):
            tr = rows.nth(k)
            cells = [tr.locator("td").nth(j).inner_text().strip().replace("\n"," ")
                     for j in range(tr.locator("td").count())]
            if len(cells)<len(headers) or cells[0].startswith("<<"):
                continue

            case       = cells[idx.get("Case",0)]
            date       = cells[idx.get("File Date",2)]
            stat       = cells[idx.get("Status",4)]
            td_desc    = cells[idx.get("Type Desc",5)]
            st_desc    = cells[idx.get("Subtype",6)]
            style_txt  = cells[idx.get("Style",7)]
            parties_txt= cells[idx.get("Parties",8)]

            # Style-first name extraction, then Parties fallback
            fn, ln = extract_decedent(style_txt)
            if not fn:
                fn, ln = extract_decedent(parties_txt)

            sig = compute_signal(td_desc, st_desc)
            recs.append({
                "county":"Harris",
                "case_number":case,
                "filing_date":date,
                "decedent_first":fn,
                "decedent_last":ln,
                "type_desc":td_desc,
                "subtype":st_desc,
                "status":stat,
                "signal_strength":sig
            })

        browser.close()
        return pd.DataFrame(recs)


if __name__=="__main__":
    df = run_scrape()
    if df.empty:
        print("No rows found — check selectors or headers.")
    else:
        df.to_csv(OUT_CSV, index=False, sep=";")
        print(f"Saved {len(df)} rows to {OUT_CSV}")
        print(df.head())
