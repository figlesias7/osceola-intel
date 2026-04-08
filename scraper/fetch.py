"""
Osceola County Florida – Motivated Seller Lead Scraper
Collects public records from the Clerk portal and enriches with
Property Appraiser parcel data. Outputs records.json + GHL CSV.
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import traceback
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fetch")

# ── constants ─────────────────────────────────────────────────────────────────
CLERK_BASE   = "https://osceolaclerk.com/records-center/"
LOOKBACK     = int(os.getenv("LOOKBACK_DAYS", "7"))
OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]
GHL_CSV_PATH = Path("data/ghl_export.csv")

# Document type categories  {code: (category, label)}
DOC_TYPES = {
    "LP":      ("foreclosure",  "Lis Pendens"),
    "NOFC":    ("foreclosure",  "Notice of Foreclosure"),
    "TAXDEED": ("tax",          "Tax Deed"),
    "JUD":     ("judgment",     "Judgment"),
    "CCJ":     ("judgment",     "Certified Judgment"),
    "DRJUD":   ("judgment",     "Domestic Judgment"),
    "LNCORPTX":("lien",        "Corp Tax Lien"),
    "LNIRS":   ("lien",        "IRS Lien"),
    "LNFED":   ("lien",        "Federal Lien"),
    "LN":      ("lien",        "Lien"),
    "LNMECH":  ("lien",        "Mechanic Lien"),
    "LNHOA":   ("lien",        "HOA Lien"),
    "MEDLN":   ("lien",        "Medicaid Lien"),
    "PRO":     ("probate",     "Probate Document"),
    "NOC":     ("notice",      "Notice of Commencement"),
    "RELLP":   ("notice",      "Release Lis Pendens"),
}

# ── property-appraiser parcel lookup ─────────────────────────────────────────
class ParcelLookup:
    """Download the Osceola PA bulk DBF and build a name→parcel map."""

    # Known candidate URLs; we try them in order
    PA_CANDIDATES = [
        "https://www.property-appraiser.org/osceola/bulk/parcel.zip",
        "https://www.osceolapropertyappraiser.com/downloads/parcel.zip",
        "https://www.osceolapropertyappraiser.com/bulk/parcel.dbf",
        "https://www.osceolapropertyappraiser.com/data/parcel.zip",
    ]
    PA_SEARCH_SEED = "https://www.osceolapropertyappraiser.com/"

    def __init__(self):
        self._by_name: dict[str, dict] = {}    # normalised-name → parcel row
        self._by_parcel: dict[str, dict] = {}  # parcel_id → parcel row
        self._loaded = False

    # ------------------------------------------------------------------
    def load(self):
        if self._loaded:
            return
        log.info("Loading parcel data from Property Appraiser…")
        dbf_bytes = self._fetch_dbf()
        if dbf_bytes:
            self._parse_dbf(dbf_bytes)
        else:
            log.warning("Could not download parcel DBF – address enrichment disabled.")
        self._loaded = True

    # ------------------------------------------------------------------
    def _fetch_dbf(self) -> Optional[bytes]:
        """Try known URLs, then scrape the PA site for a download link."""
        for url in self.PA_CANDIDATES:
            data = self._try_download(url)
            if data:
                return data

        # Scrape the PA website for a download/bulk-data link
        try:
            r = requests.get(self.PA_SEARCH_SEED, timeout=20)
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"].lower()
                if any(k in href for k in ["parcel", "bulk", "dbf", "download", "data"]):
                    full = urljoin(self.PA_SEARCH_SEED, a["href"])
                    data = self._try_download(full)
                    if data:
                        return data
        except Exception as exc:
            log.debug("PA site scrape failed: %s", exc)

        return None

    def _try_download(self, url: str, attempts: int = 3) -> Optional[bytes]:
        for i in range(attempts):
            try:
                log.debug("Trying PA URL: %s", url)
                r = requests.get(url, timeout=60, stream=True)
                if r.status_code == 200 and len(r.content) > 1000:
                    content = r.content
                    # If it's a ZIP, extract the first DBF
                    if url.endswith(".zip") or content[:2] == b"PK":
                        z = zipfile.ZipFile(io.BytesIO(content))
                        for name in z.namelist():
                            if name.lower().endswith(".dbf"):
                                return z.read(name)
                        return None
                    return content
            except Exception as exc:
                log.debug("Attempt %d for %s failed: %s", i+1, url, exc)
                time.sleep(2 ** i)
        return None

    # ------------------------------------------------------------------
    def _parse_dbf(self, data: bytes):
        try:
            from dbfread import DBF
        except ImportError:
            log.warning("dbfread not installed – skipping parcel load")
            return

        try:
            tmp = Path("/tmp/parcel_tmp.dbf")
            tmp.write_bytes(data)
            tbl = DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True)
            for row in tbl:
                parcel = self._parse_row(row)
                if not parcel:
                    continue
                pid = parcel.get("parcel_id", "")
                if pid:
                    self._by_parcel[pid] = parcel
                for variant in self._name_variants(parcel.get("owner", "")):
                    self._by_name[variant] = parcel
            log.info("Parcel lookup loaded: %d parcels, %d name variants",
                     len(self._by_parcel), len(self._by_name))
        except Exception as exc:
            log.error("DBF parse error: %s", exc)

    def _parse_row(self, row) -> Optional[dict]:
        """Normalise column names across different DBF schemas."""
        r = {k.upper(): (v or "").strip() if isinstance(v, str) else (v or "") for k, v in row.items()}

        owner = r.get("OWNER") or r.get("OWN1") or r.get("OWNERNAME") or ""
        if not owner:
            return None

        site_addr = r.get("SITE_ADDR") or r.get("SITEADDR") or r.get("SITUS") or ""
        site_city = r.get("SITE_CITY") or r.get("SITECITY") or r.get("SCITY") or "Kissimmee"
        site_zip  = r.get("SITE_ZIP")  or r.get("SITEZIP")  or r.get("SZIP")  or ""

        mail_addr = r.get("ADDR_1") or r.get("MAILADR1") or r.get("MAILADDR") or ""
        mail_city = r.get("CITY")   or r.get("MAILCITY") or ""
        mail_state= r.get("STATE")  or r.get("MAILSTATE") or "FL"
        mail_zip  = r.get("ZIP")    or r.get("MAILZIP")  or ""

        parcel_id = r.get("PARCEL") or r.get("PARCELID") or r.get("APN") or ""

        return {
            "parcel_id":   str(parcel_id),
            "owner":       owner,
            "prop_address":site_addr,
            "prop_city":   site_city,
            "prop_state":  "FL",
            "prop_zip":    site_zip,
            "mail_address":mail_addr,
            "mail_city":   mail_city,
            "mail_state":  mail_state,
            "mail_zip":    mail_zip,
        }

    # ------------------------------------------------------------------
    @staticmethod
    def _name_variants(name: str) -> list[str]:
        """Return normalised lookup keys for a name string."""
        name = name.strip().upper()
        variants = {name}
        # "LAST, FIRST" → "FIRST LAST"
        if "," in name:
            parts = [p.strip() for p in name.split(",", 1)]
            variants.add(f"{parts[1]} {parts[0]}")
            variants.add(f"{parts[0]} {parts[1]}")
        return [v for v in variants if v]

    def lookup(self, owner_name: str) -> dict:
        """Return parcel dict (or empty dict) for an owner name."""
        if not self._loaded:
            self.load()
        for variant in self._name_variants(owner_name):
            if variant in self._by_name:
                return self._by_name[variant]
        return {}


# ── NewVision clerk portal scraper ────────────────────────────────────────────
#
# The Osceola Clerk uses NewVision Systems (© 2018 NewVision Systems Corp).
# Actual search app URL discovered at runtime from the records-center page.
# UI anatomy (confirmed from screenshot):
#   • Tabs: Party | Document Type | Instrument Number | Book/Page
#   • "Document Type (Optional)" → plain text input  (comma-separated codes)
#   • "Select Document Types (Optional)" → checkbox list (we use the text input)
#   • "Date Range (Optional)" → From / To  MM/DD/YYYY inputs
#   • Quicklinks: "7 Days" / "30 Days" / "90 Days"   (we click "7 Days")
#   • [Search] button top-right of the form
#   • Results tab shows a table; paginated with Next link
#
# Strategy: navigate to the search app, click the "Document Type" tab,
# type our comma-separated codes into the Document Type text input,
# click "7 Days" quicklink, then click Search.  Repeat per batch if needed
# (portal may cap results per search; we batch ≤10 codes at a time).

# We discover the actual app URL from the clerk landing page link
_NEWVISION_SEARCH_URL: Optional[str] = None

NEWVISION_SEEDS = [
    # Most common NewVision deploy pattern for FL clerks
    "https://or.osceolaclerk.com/or/",
    "https://www.osceolaclerk.com/official-records/",
    "https://osceolaclerk.com/official-records/",
]


async def _discover_search_url(page) -> str:
    """
    Navigate to the clerk landing page and extract the actual search-app URL,
    or fall back to known NewVision patterns.
    """
    global _NEWVISION_SEARCH_URL
    if _NEWVISION_SEARCH_URL:
        return _NEWVISION_SEARCH_URL

    try:
        await page.goto(CLERK_BASE, wait_until="domcontentloaded", timeout=20000)
        # Look for any link whose href contains typical NewVision path segments
        for a in await page.locator("a").all():
            try:
                href = await a.get_attribute("href") or ""
                text = (await a.inner_text()).lower()
                if any(k in href.lower() for k in ["/or/", "official-record", "newvision", "search.aspx"]):
                    _NEWVISION_SEARCH_URL = href if href.startswith("http") else urljoin(CLERK_BASE, href)
                    log.info("Discovered search URL from landing page: %s", _NEWVISION_SEARCH_URL)
                    return _NEWVISION_SEARCH_URL
                if any(k in text for k in ["official record", "search record", "public record"]):
                    if href and not href.startswith("#"):
                        _NEWVISION_SEARCH_URL = href if href.startswith("http") else urljoin(CLERK_BASE, href)
                        log.info("Discovered search URL via link text: %s", _NEWVISION_SEARCH_URL)
                        return _NEWVISION_SEARCH_URL
            except Exception:
                pass
    except Exception as exc:
        log.debug("Landing page discovery failed: %s", exc)

    # Fall back: try each seed URL, use the first that loads with NewVision markup
    for url in NEWVISION_SEEDS:
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            if resp and resp.status < 400:
                content = await page.content()
                if "newvision" in content.lower() or "party name" in content.lower() or "document type" in content.lower():
                    _NEWVISION_SEARCH_URL = page.url
                    log.info("Using seed search URL: %s", _NEWVISION_SEARCH_URL)
                    return _NEWVISION_SEARCH_URL
        except Exception:
            pass

    _NEWVISION_SEARCH_URL = NEWVISION_SEEDS[0]
    log.warning("Could not discover search URL; defaulting to %s", _NEWVISION_SEARCH_URL)
    return _NEWVISION_SEARCH_URL


async def scrape_clerk(doc_types: list[str], start_date: str, end_date: str) -> list[dict]:
    """
    Drive the NewVision clerk portal with Playwright.
    Batches doc types in groups of 10 (comma-separated in the type field).
    Returns a list of raw record dicts.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("playwright not installed")
        return []

    records = []
    log.info("Launching Playwright (NewVision clerk portal)…")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
        )
        page = await ctx.new_page()

        # Discover the real search app URL once
        search_url = await _discover_search_url(page)

        # Batch doc types: ≤10 per search to avoid hitting result caps
        batch_size = 10
        batches = [doc_types[i:i+batch_size] for i in range(0, len(doc_types), batch_size)]

        for batch in batches:
            codes = ",".join(batch)
            try:
                batch_recs = await _run_newvision_search(page, search_url, codes, start_date, end_date)
                records.extend(batch_recs)
                log.info("  Batch [%s] → %d records", codes, len(batch_recs))
            except Exception as exc:
                log.error("Batch [%s] failed: %s", codes, exc)
                log.debug(traceback.format_exc())
            await asyncio.sleep(1.5)   # polite pause between batches

        await browser.close()

    return records


async def _run_newvision_search(
    page, search_url: str, doc_codes: str, start_date: str, end_date: str
) -> list[dict]:
    """
    Execute one NewVision search for a comma-separated set of doc type codes.
    Handles pagination and returns all matching records.
    """
    from playwright.async_api import TimeoutError as PWTimeout

    # ── 1. Load the search page ──────────────────────────────────────────────
    for attempt in range(3):
        try:
            await page.goto(search_url, wait_until="domcontentloaded", timeout=25000)
            # Wait for the Party Name input to confirm the app loaded
            await page.wait_for_selector(
                "input[placeholder*='Party'], input[placeholder*='party'], "
                "#PartyName, input[id*='arty']",
                timeout=15000,
            )
            break
        except PWTimeout:
            if attempt == 2:
                log.warning("Search page never loaded for codes: %s", doc_codes)
                return []
            await asyncio.sleep(3)

    # ── 2. Click the "Document Type" tab ────────────────────────────────────
    # NewVision has sub-tabs: Party | Document Type | Instrument Number | Book/Page
    try:
        dt_tab = page.locator(
            "a:has-text('Document Type'), li:has-text('Document Type'), "
            "[role='tab']:has-text('Document Type')"
        )
        if await dt_tab.count() > 0:
            await dt_tab.first.click()
            await page.wait_for_timeout(600)
    except Exception as exc:
        log.debug("Doc-type tab click failed (may not be needed): %s", exc)

    # ── 3. Fill the "Document Type (Optional)" text input ───────────────────
    # NewVision renders this as a plain <input> that accepts comma-separated codes
    dt_input = page.locator(
        "input[placeholder*='Document Type'], "
        "input[placeholder*='document type'], "
        "#DocumentType, "
        "input[id*='ocumentType'], "
        "input[id*='ocType']"
    )
    filled = False
    if await dt_input.count() > 0:
        await dt_input.first.triple_click()
        await dt_input.first.fill(doc_codes)
        filled = True
        log.debug("Filled Document Type input with: %s", doc_codes)
    else:
        log.warning("Could not find Document Type input – will search all types")

    # ── 4. Set date range — click "7 Days" quicklink ────────────────────────
    # The page has "7 Days", "30 Days", "90 Days" links that auto-fill the range
    clicked_quick = False
    try:
        seven_days = page.locator("a:has-text('7 Days'), button:has-text('7 Days')")
        if await seven_days.count() > 0:
            await seven_days.first.click()
            await page.wait_for_timeout(400)
            clicked_quick = True
            log.debug("Clicked '7 Days' quicklink")
    except Exception:
        pass

    # Fallback: manually fill From / To date inputs
    if not clicked_quick:
        for sel in [
            "input[placeholder='MM/DD/YYYY']",
            "input[id*='rom'], input[id*='Begin'], input[id*='Start']",
            "#DateFrom, #BeginDate, #StartDate",
        ]:
            try:
                inputs = await page.locator(sel).all()
                if len(inputs) >= 2:
                    await inputs[0].triple_click()
                    await inputs[0].fill(start_date)
                    await inputs[1].triple_click()
                    await inputs[1].fill(end_date)
                    break
                elif len(inputs) == 1:
                    await inputs[0].triple_click()
                    await inputs[0].fill(start_date)
            except Exception:
                pass

    # ── 5. Click Search ──────────────────────────────────────────────────────
    search_btn = page.locator(
        "input[value='Search'], button:has-text('Search'), "
        "#SearchButton, #btnSearch, .search-btn"
    )
    if await search_btn.count() == 0:
        log.warning("Search button not found for codes: %s", doc_codes)
        return []

    await search_btn.first.click()

    # Wait for Results tab / results table to appear
    try:
        await page.wait_for_selector(
            "table tr td, .results-table, #resultsTable, "
            "[id*='esult'] table, a:has-text('Results')",
            timeout=20000,
        )
    except PWTimeout:
        log.warning("Results never appeared for codes: %s", doc_codes)
        return []

    # Small extra wait for JS rendering
    await page.wait_for_timeout(800)

    # ── 6. Paginate and collect ───────────────────────────────────────────────
    records = []
    page_num = 0
    base_url = page.url

    while True:
        page_num += 1
        html = await page.content()
        new_recs = _parse_newvision_results(html, doc_codes, base_url)
        records.extend(new_recs)
        log.debug("  Page %d: %d new records (running total %d)", page_num, len(new_recs), len(records))

        # NewVision pagination: "Next" link or ">>" button
        next_btn = page.locator(
            "a:has-text('Next'), a:has-text('>>'), "
            ".pager a:last-child, [aria-label='Next page'], "
            "a[title='Next Page']"
        )
        if await next_btn.count() > 0:
            is_disabled = await next_btn.first.get_attribute("class") or ""
            if "disabled" in is_disabled.lower():
                break
            try:
                await next_btn.first.click()
                await page.wait_for_load_state("domcontentloaded", timeout=15000)
                await page.wait_for_timeout(600)
            except Exception:
                break
        else:
            break

        if page_num >= 50:
            log.warning("Hit 50-page cap for codes: %s", doc_codes)
            break

    return records


def _parse_newvision_results(html: str, doc_codes: str, base_url: str) -> list[dict]:
    """
    Parse the NewVision results page HTML.

    NewVision results tables typically have these columns:
      Instrument #  |  Type  |  Date  |  Grantor  |  Grantee  |  Book/Page  |  Description/Legal

    Each row links to the document detail via the Instrument # anchor.
    """
    soup = BeautifulSoup(html, "lxml")
    records = []

    # Find every <table> that has result-like columns
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # Build header map from first <tr> containing <th> or the first row
        header_row = rows[0]
        headers = [c.get_text(strip=True).lower() for c in header_row.find_all(["th", "td"])]
        joined = " | ".join(headers)

        # Must look like a results table
        if not any(k in joined for k in ["instrument", "type", "grantor", "date", "recorded"]):
            continue

        col = _newvision_col_map(headers)

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            try:
                rec = _newvision_extract_row(cells, col, doc_codes, base_url)
                if rec:
                    records.append(rec)
            except Exception as exc:
                log.debug("Row parse error: %s", exc)

    return records


def _newvision_col_map(headers: list[str]) -> dict:
    """Map column indices for the NewVision results table."""
    col = {}
    for i, h in enumerate(headers):
        h = h.lower()
        if any(k in h for k in ["instrument", "instr", "cf#", "doc #", "doc#", "number"]):
            col.setdefault("doc_num", i)
        if any(k in h for k in ["type", "doc type", "document type"]) and "instrument" not in h:
            col.setdefault("doc_type", i)
        if any(k in h for k in ["date", "filed", "recorded", "record date"]):
            col.setdefault("date", i)
        if any(k in h for k in ["grantor", "seller", "from party", "owner"]):
            col.setdefault("grantor", i)
        if any(k in h for k in ["grantee", "buyer", "to party"]):
            col.setdefault("grantee", i)
        if any(k in h for k in ["amount", "consideration", "value", "total"]):
            col.setdefault("amount", i)
        if any(k in h for k in ["legal", "description", "desc", "book"]):
            col.setdefault("legal", i)
    return col


def _newvision_extract_row(cells: list, col: dict, fallback_type: str, base_url: str) -> Optional[dict]:
    texts = [c.get_text(" ", strip=True) for c in cells]
    if not any(texts):
        return None

    def get(key, default_idx):
        idx = col.get(key, default_idx)
        if 0 <= idx < len(texts):
            return texts[idx].strip()
        return ""

    doc_num  = get("doc_num", 0)
    doc_type = get("doc_type", 1) or fallback_type
    filed    = get("date", 2)
    grantor  = get("grantor", 3)
    grantee  = get("grantee", 4)
    amount   = get("amount", -1)
    legal    = get("legal", -1)

    # Skip empty / header-repeat rows
    if not doc_num or doc_num.lower() in ("instrument #", "cf#", "doc #", "number"):
        return None

    # Extract direct link from the Instrument # cell (col 0 typically)
    link = ""
    link_cell_idx = col.get("doc_num", 0)
    if link_cell_idx < len(cells):
        a = cells[link_cell_idx].find("a", href=True)
        if a:
            href = a["href"]
            link = href if href.startswith("http") else urljoin(base_url, href)

    return {
        "doc_num":  doc_num,
        "doc_type": doc_type.upper().strip(),
        "filed":    _norm_date(filed),
        "grantor":  grantor,
        "grantee":  grantee,
        "amount":   _parse_amount(amount),
        "legal":    legal,
        "clerk_url":link,
    }


# ── helpers ───────────────────────────────────────────────────────────────────
def _re_find(pattern: str, text: str) -> str:
    m = re.search(pattern, text, re.IGNORECASE)
    return m.group(1).strip() if m else ""


def _norm_date(raw: str) -> str:
    """Normalise various date formats to YYYY-MM-DD."""
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%d-%b-%Y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw


def _parse_amount(raw: str) -> float:
    if not raw:
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", raw)
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _is_this_week(date_str: str, today: datetime) -> bool:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return (today - d).days <= 7
    except Exception:
        return False


# ── seller score & flags ──────────────────────────────────────────────────────
def compute_flags_and_score(rec: dict, today: datetime) -> tuple[list[str], int]:
    flags = []
    doc_type = rec.get("doc_type", "").upper()
    cat      = rec.get("cat", "")
    owner    = rec.get("owner", rec.get("grantor", ""))
    amount   = rec.get("amount", 0) or 0
    has_addr = bool(rec.get("prop_address") or rec.get("mail_address"))
    filed    = rec.get("filed", "")

    if doc_type in ("LP", "RELLP") or cat == "foreclosure":
        flags.append("Lis pendens")
    if doc_type in ("NOFC", "LP"):
        flags.append("Pre-foreclosure")
    if doc_type in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")
    if doc_type in ("TAXDEED", "LNCORPTX", "LNIRS", "LNFED"):
        flags.append("Tax lien")
    if doc_type == "LNMECH":
        flags.append("Mechanic lien")
    if doc_type == "PRO":
        flags.append("Probate / estate")
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b|\bTRUST\b", owner.upper()):
        flags.append("LLC / corp owner")
    if _is_this_week(filed, today):
        flags.append("New this week")

    # Score
    score = 30
    score += 10 * len([f for f in flags if f not in ("New this week", "LLC / corp owner")])
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    if amount >= 100_000:
        score += 15
    elif amount >= 50_000:
        score += 10
    if "New this week" in flags:
        score += 5
    if has_addr:
        score += 5

    score = min(score, 100)
    return flags, score


# ── enrich records ────────────────────────────────────────────────────────────
def enrich(raw_records: list[dict], parcel: ParcelLookup, today: datetime) -> list[dict]:
    enriched = []
    for r in raw_records:
        try:
            doc_type = r.get("doc_type", "").upper()
            cat_info = DOC_TYPES.get(doc_type, ("other", doc_type))
            cat, cat_label = cat_info

            owner   = r.get("grantor", "")
            p_data  = parcel.lookup(owner) if owner else {}

            rec = {
                "doc_num":      r.get("doc_num", ""),
                "doc_type":     doc_type,
                "filed":        r.get("filed", ""),
                "cat":          cat,
                "cat_label":    cat_label,
                "owner":        owner,
                "grantee":      r.get("grantee", ""),
                "amount":       r.get("amount", 0),
                "legal":        r.get("legal", ""),
                "prop_address": p_data.get("prop_address", ""),
                "prop_city":    p_data.get("prop_city", ""),
                "prop_state":   p_data.get("prop_state", "FL"),
                "prop_zip":     p_data.get("prop_zip", ""),
                "mail_address": p_data.get("mail_address", ""),
                "mail_city":    p_data.get("mail_city", ""),
                "mail_state":   p_data.get("mail_state", "FL"),
                "mail_zip":     p_data.get("mail_zip", ""),
                "clerk_url":    r.get("clerk_url", ""),
                "flags":        [],
                "score":        0,
            }

            flags, score = compute_flags_and_score(rec, today)
            rec["flags"] = flags
            rec["score"] = score
            enriched.append(rec)
        except Exception as exc:
            log.debug("Enrich error: %s | %s", exc, r)

    return enriched


# ── output ────────────────────────────────────────────────────────────────────
def save_json(records: list[dict], today: datetime, start_date: str, end_date: str):
    payload = {
        "fetched_at":    today.isoformat(),
        "source":        "Osceola County Clerk of Courts – Official Records",
        "date_range":    {"from": start_date, "to": end_date},
        "total":         len(records),
        "with_address":  sum(1 for r in records if r.get("prop_address") or r.get("mail_address")),
        "records":       sorted(records, key=lambda r: r.get("score", 0), reverse=True),
    }
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2))
        log.info("Saved %s (%d records)", path, len(records))


def save_ghl_csv(records: list[dict]):
    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "First Name", "Last Name", "Mailing Address", "Mailing City",
        "Mailing State", "Mailing Zip", "Property Address", "Property City",
        "Property State", "Property Zip", "Lead Type", "Document Type",
        "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score",
        "Motivated Seller Flags", "Source", "Public Records URL",
    ]
    with GHL_CSV_PATH.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in records:
            owner = r.get("owner", "")
            parts = owner.replace(",", "").split()
            first = parts[0] if parts else ""
            last  = " ".join(parts[1:]) if len(parts) > 1 else ""

            w.writerow({
                "First Name":          first,
                "Last Name":           last,
                "Mailing Address":     r.get("mail_address", ""),
                "Mailing City":        r.get("mail_city", ""),
                "Mailing State":       r.get("mail_state", "FL"),
                "Mailing Zip":         r.get("mail_zip", ""),
                "Property Address":    r.get("prop_address", ""),
                "Property City":       r.get("prop_city", ""),
                "Property State":      r.get("prop_state", "FL"),
                "Property Zip":        r.get("prop_zip", ""),
                "Lead Type":           r.get("cat_label", ""),
                "Document Type":       r.get("doc_type", ""),
                "Date Filed":          r.get("filed", ""),
                "Document Number":     r.get("doc_num", ""),
                "Amount/Debt Owed":    r.get("amount", ""),
                "Seller Score":        r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":              "Osceola County Clerk",
                "Public Records URL":  r.get("clerk_url", ""),
            })
    log.info("GHL CSV saved: %s", GHL_CSV_PATH)


# ── main ──────────────────────────────────────────────────────────────────────
async def main():
    today      = datetime.now()
    start_dt   = today - timedelta(days=LOOKBACK)
    start_date = start_dt.strftime("%m/%d/%Y")
    end_date   = today.strftime("%m/%d/%Y")

    log.info("=" * 60)
    log.info("Osceola Motivated Seller Scraper")
    log.info("Date range: %s → %s", start_date, end_date)
    log.info("Doc types:  %s", ", ".join(DOC_TYPES.keys()))
    log.info("=" * 60)

    # 1. Load parcel data
    parcel = ParcelLookup()
    parcel.load()

    # 2. Scrape clerk portal
    raw = await scrape_clerk(list(DOC_TYPES.keys()), start_date, end_date)
    log.info("Total raw records from clerk: %d", len(raw))

    # 3. Enrich
    records = enrich(raw, parcel, today)

    # 4. Save
    save_json(records, today, start_date, end_date)
    save_ghl_csv(records)

    log.info("Done. %d leads, %d with addresses.",
             len(records),
             sum(1 for r in records if r.get("prop_address") or r.get("mail_address")))


if __name__ == "__main__":
    asyncio.run(main())
