"""
Osceola County Florida – Motivated Seller Lead Scraper
=======================================================
Confirmed search portal: https://officialrecords.osceolaclerk.org/browserview/
(NewVision Systems Angular SPA, © 2018 NewVision Systems Corporation)

Strategy — two-track approach:
  1. Try the NewVision JSON REST API directly (fast, no browser needed).
  2. If the API is blocked/changed, fall back to Playwright driving the UI.

The Angular app loads data via XHR calls to /api/search/DocumentType (and
similar endpoints). We intercept/replicate those calls with requests + a
session that first loads the SPA to pick up any auth cookies/tokens.
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import time
import traceback
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fetch")

# ── constants ─────────────────────────────────────────────────────────────────
# Confirmed via Google search index:
#   https://officialrecords.osceolaclerk.org/browserview/
NEWVISION_BASE   = "https://officialrecords.osceolaclerk.org"
NEWVISION_BROWSE = "https://officialrecords.osceolaclerk.org/browserview/"

# NewVision API patterns (standard across NewVision deployments).
NEWVISION_API_CANDIDATES = [
    "/api/search/DocumentType",
    "/api/OfficialRecords/search",
    "/api/search",
    "/browserview/api/search/DocumentType",
]

LOOKBACK     = int(os.getenv("LOOKBACK_DAYS", "7"))
OUTPUT_PATHS = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_CSV_PATH = Path("data/ghl_export.csv")

# Document type map  {code: (category, label)}
DOC_TYPES = {
    "LP":       ("foreclosure", "Lis Pendens"),
    "NOFC":     ("foreclosure", "Notice of Foreclosure"),
    "TAXDEED":  ("tax",         "Tax Deed"),
    "JUD":      ("judgment",    "Judgment"),
    "CCJ":      ("judgment",    "Certified Judgment"),
    "DRJUD":    ("judgment",    "Domestic Judgment"),
    "LNCORPTX": ("lien",       "Corp Tax Lien"),
    "LNIRS":    ("lien",       "IRS Lien"),
    "LNFED":    ("lien",       "Federal Lien"),
    "LN":       ("lien",       "Lien"),
    "LNMECH":   ("lien",       "Mechanic Lien"),
    "LNHOA":    ("lien",       "HOA Lien"),
    "MEDLN":    ("lien",       "Medicaid Lien"),
    "PRO":      ("probate",    "Probate Document"),
    "NOC":      ("notice",     "Notice of Commencement"),
    "RELLP":    ("notice",     "Release Lis Pendens"),
}

# ── helpers ───────────────────────────────────────────────────────────────────
def _norm_date(raw: str) -> str:
    raw = (raw or "").strip()
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%d-%b-%Y",
                "%B %d, %Y", "%Y/%m/%d", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw

def _parse_amount(raw) -> float:
    if not raw:
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", str(raw))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

def _is_this_week(date_str: str, today: datetime) -> bool:
    try:
        return (today - datetime.strptime(date_str, "%Y-%m-%d")).days <= 7
    except Exception:
        return False


# ── Property Appraiser parcel lookup ─────────────────────────────────────────
class ParcelLookup:
    PA_CANDIDATES = [
        "https://www.property-appraiser.org/osceola/bulk/parcel.zip",
        "https://www.osceolapropertyappraiser.com/downloads/parcel.zip",
        "https://www.osceolapropertyappraiser.com/bulk/parcel.dbf",
        "https://www.osceolapropertyappraiser.com/data/parcel.zip",
        "https://www.property-appraiser.org/osceola/downloads/parcel.zip",
    ]
    PA_SEED = "https://www.property-appraiser.org/osceola/"

    def __init__(self):
        self._by_name:   dict[str, dict] = {}
        self._by_parcel: dict[str, dict] = {}
        self._loaded = False

    def load(self):
        if self._loaded:
            return
        log.info("Loading parcel data from Property Appraiser...")
        data = self._fetch_dbf()
        if data:
            self._parse_dbf(data)
        else:
            log.warning("Could not download parcel DBF - address enrichment disabled.")
        self._loaded = True

    def _fetch_dbf(self) -> Optional[bytes]:
        for url in self.PA_CANDIDATES:
            d = self._try_url(url)
            if d:
                return d
        try:
            r = requests.get(self.PA_SEED, timeout=20,
                             headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"].lower()
                if any(k in href for k in ["parcel", "bulk", "dbf", "download"]):
                    full = urljoin(self.PA_SEED, a["href"])
                    d = self._try_url(full)
                    if d:
                        return d
        except Exception as exc:
            log.debug("PA site scrape: %s", exc)
        return None

    def _try_url(self, url: str, attempts: int = 3) -> Optional[bytes]:
        for i in range(attempts):
            try:
                r = requests.get(url, timeout=60,
                                 headers={"User-Agent": "Mozilla/5.0"})
                if r.status_code == 200 and len(r.content) > 500:
                    content = r.content
                    if content[:2] == b"PK":
                        z = zipfile.ZipFile(io.BytesIO(content))
                        for name in z.namelist():
                            if name.lower().endswith(".dbf"):
                                return z.read(name)
                    return content
            except Exception as exc:
                log.debug("PA attempt %d %s: %s", i+1, url, exc)
                time.sleep(2 ** i)
        return None

    def _parse_dbf(self, data: bytes):
        try:
            from dbfread import DBF
        except ImportError:
            log.warning("dbfread not installed - skipping parcel enrichment")
            return
        try:
            tmp = Path("/tmp/_parcel.dbf")
            tmp.write_bytes(data)
            tbl = DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True)
            for row in tbl:
                p = self._parse_row(row)
                if not p:
                    continue
                if p.get("parcel_id"):
                    self._by_parcel[p["parcel_id"]] = p
                for v in self._variants(p["owner"]):
                    self._by_name[v] = p
            log.info("Parcels: %d parcels, %d name variants",
                     len(self._by_parcel), len(self._by_name))
        except Exception as exc:
            log.error("DBF parse: %s", exc)

    @staticmethod
    def _parse_row(row) -> Optional[dict]:
        r = {k.upper(): (str(v).strip() if v else "") for k, v in row.items()}
        owner = r.get("OWNER") or r.get("OWN1") or r.get("OWNERNAME") or ""
        if not owner:
            return None
        return {
            "parcel_id":   r.get("PARCEL") or r.get("PARCELID") or r.get("APN") or "",
            "owner":       owner,
            "prop_address":r.get("SITE_ADDR") or r.get("SITEADDR") or r.get("SITUS") or "",
            "prop_city":   r.get("SITE_CITY") or r.get("SITECITY") or "Kissimmee",
            "prop_state":  "FL",
            "prop_zip":    r.get("SITE_ZIP")  or r.get("SITEZIP") or "",
            "mail_address":r.get("ADDR_1")    or r.get("MAILADR1") or r.get("MAILADDR") or "",
            "mail_city":   r.get("CITY")      or r.get("MAILCITY") or "",
            "mail_state":  r.get("STATE")     or r.get("MAILSTATE") or "FL",
            "mail_zip":    r.get("ZIP")       or r.get("MAILZIP") or "",
        }

    @staticmethod
    def _variants(name: str) -> list[str]:
        name = name.strip().upper()
        vs = {name}
        if "," in name:
            parts = [p.strip() for p in name.split(",", 1)]
            vs.add(f"{parts[1]} {parts[0]}")
            vs.add(f"{parts[0]} {parts[1]}")
        return [v for v in vs if v]

    def lookup(self, owner: str) -> dict:
        if not self._loaded:
            self.load()
        for v in self._variants(owner):
            if v in self._by_name:
                return self._by_name[v]
        return {}


# ── NewVision API client (requests, no browser) ───────────────────────────────
class NewVisionClient:
    """
    Replicate the Angular app's XHR calls to the NewVision backend.
    Step 1: GET browserview/ to pick up session cookies.
    Step 2: POST /api/search/DocumentType with search criteria.
    """
    HEADERS = {
        "User-Agent":    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept":        "application/json, text/plain, */*",
        "Referer":       NEWVISION_BROWSE,
        "Origin":        NEWVISION_BASE,
        "X-Requested-With": "XMLHttpRequest",
    }

    def __init__(self):
        self.session  = requests.Session()
        self.session.headers.update(self.HEADERS)
        self._api_url: Optional[str] = None
        self._seeded  = False

    def _seed(self):
        if self._seeded:
            return
        try:
            r = self.session.get(NEWVISION_BROWSE, timeout=20)
            log.info("Session seeded from %s (HTTP %d)", NEWVISION_BROWSE, r.status_code)
        except Exception as exc:
            log.debug("Seed failed: %s", exc)
        self._seeded = True

    def _find_api(self) -> Optional[str]:
        if self._api_url:
            return self._api_url
        self._seed()

        test_payload = {
            "docTypes": ["LP"],
            "dateFrom": (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y"),
            "dateTo":   datetime.now().strftime("%m/%d/%Y"),
            "pageSize": 1,
            "pageNum":  1,
        }
        for path in NEWVISION_API_CANDIDATES:
            url = NEWVISION_BASE + path
            for method in ("POST", "GET"):
                try:
                    if method == "POST":
                        r = self.session.post(url, json=test_payload, timeout=15)
                    else:
                        r = self.session.get(url, params=test_payload, timeout=15)
                    ct = r.headers.get("content-type", "")
                    log.debug("API probe %s %s -> HTTP %d  ct=%s", method, url,
                              r.status_code, ct)
                    if r.status_code == 200 and "json" in ct:
                        self._api_url = url
                        log.info("NewVision API found: %s %s", method, url)
                        return url
                except Exception as exc:
                    log.debug("Probe %s %s: %s", method, url, exc)

        log.warning("NewVision JSON API not found - will use Playwright")
        return None

    def search(self, doc_type: str, start_date: str, end_date: str) -> list[dict]:
        api_url = self._find_api()
        if not api_url:
            return []
        records  = []
        page_num = 1
        while True:
            payload = {
                "docTypes":  [doc_type],
                "docType":   doc_type,
                "dateFrom":  start_date,
                "dateTo":    end_date,
                "pageSize":  200,
                "pageNum":   page_num,
                "startRow":  (page_num - 1) * 200,
            }
            try:
                r = self.session.post(api_url, json=payload, timeout=30)
                if r.status_code != 200:
                    log.warning("API %s p%d HTTP %d", doc_type, page_num, r.status_code)
                    break
                data = r.json()
                rows = self._extract_rows(data)
                if not rows:
                    break
                for row in rows:
                    rec = self._normalise(row, doc_type)
                    if rec:
                        records.append(rec)
                if len(rows) < 200:
                    break
                page_num += 1
                time.sleep(0.3)
            except Exception as exc:
                log.error("API %s p%d: %s", doc_type, page_num, exc)
                break
        return records

    @staticmethod
    def _extract_rows(data) -> list:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("results", "records", "data", "items", "rows", "Documents"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            if any(k in data for k in ("InstrumentNumber", "DocType", "Grantor")):
                return [data]
        return []

    @staticmethod
    def _normalise(row: dict, fallback_type: str) -> Optional[dict]:
        def g(*keys):
            for k in keys:
                for variant in (k, k.lower(), k.upper()):
                    v = row.get(variant)
                    if v:
                        return str(v).strip()
            return ""

        doc_num = g("InstrumentNumber","Instrument","DocNumber",
                    "OfficialRecordNumber","instrumentNumber","CF","cf")
        if not doc_num:
            return None

        doc_type = g("DocType","DocumentType","Type","docType") or fallback_type
        filed    = _norm_date(g("RecordedDate","DateRecorded","FiledDate",
                                "recordedDate","date","Date"))
        grantor  = g("Grantor","GrantorName","Party1","grantor")
        grantee  = g("Grantee","GranteeName","Party2","grantee")
        amount   = _parse_amount(g("Amount","ConsiderationAmount","Consideration","amount"))
        legal    = g("LegalDescription","Legal","Description","legal")
        instr    = doc_num.replace(" ", "")
        clerk_url = f"{NEWVISION_BROWSE}?InstrumentNumber={instr}"

        return {
            "doc_num":  doc_num,
            "doc_type": doc_type.upper().strip(),
            "filed":    filed,
            "grantor":  grantor,
            "grantee":  grantee,
            "amount":   amount,
            "legal":    legal,
            "clerk_url":clerk_url,
        }


# ── Playwright fallback ───────────────────────────────────────────────────────
async def _playwright_search(
        doc_types: list[str], start_date: str, end_date: str) -> list[dict]:
    """
    Drive officialrecords.osceolaclerk.org/browserview/ with Playwright.
    Matches the exact UI shown in the screenshot:
      Tab bar   -> click "Document Type"
      Input     -> "Document Types"  (placeholder, accepts comma-separated codes)
      Quicklink -> "7 Days"
      Button    -> [Search]
    Also intercepts XHR responses to capture JSON data directly.
    """
    try:
        from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    except ImportError:
        log.error("playwright not installed")
        return []

    records: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
        )
        page = await ctx.new_page()

        # ── XHR interception — capture JSON from any API call ────────────────
        captured: list[dict] = []

        async def on_response(response):
            try:
                ct  = response.headers.get("content-type", "")
                url = response.url
                if "json" in ct and any(k in url for k in
                        ["/api/", "/search", "DocumentType", "OfficialRecord"]):
                    body = await response.json()
                    rows = NewVisionClient._extract_rows(body)
                    log.debug("XHR %s -> %d rows", url, len(rows))
                    captured.extend(rows)
            except Exception:
                pass

        page.on("response", on_response)

        # ── load the SPA ──────────────────────────────────────────────────────
        log.info("Playwright: navigating to %s", NEWVISION_BROWSE)
        try:
            await page.goto(NEWVISION_BROWSE, wait_until="networkidle", timeout=40000)
        except PWTimeout:
            log.warning("networkidle timeout - continuing")

        # ── wait for Angular to render the search form ────────────────────────
        # The Party tab is active by default; wait for its input
        angular_ready = False
        for sel in [
            "input[placeholder*='Party']",
            "input[placeholder*='party']",
            "input[ng-model*='party']",
            "form input",
        ]:
            try:
                await page.wait_for_selector(sel, timeout=15000)
                angular_ready = True
                log.info("Playwright: Angular form ready (%s)", sel)
                break
            except PWTimeout:
                pass

        if not angular_ready:
            log.warning("Angular form did not render - dumping page snippet")
            snippet = await page.content()
            log.debug("HTML[:3000]: %s", snippet[:3000])

        # ── click "Document Type" tab ─────────────────────────────────────────
        for tab_sel in [
            "text='Document Type'",
            "a:has-text('Document Type')",
            "li:has-text('Document Type')",
            "[role='tab']:has-text('Document Type')",
        ]:
            try:
                el = page.locator(tab_sel).first
                if await el.count() > 0:
                    await el.click(timeout=6000)
                    await page.wait_for_timeout(600)
                    log.info("Playwright: clicked Document Type tab")
                    break
            except Exception:
                pass

        # ── fill "Document Types" input ───────────────────────────────────────
        all_codes = ",".join(doc_types)
        filled = False
        for inp_sel in [
            "input[placeholder='Document Types']",
            "input[placeholder*='Document Type']",
            "input[placeholder*='document type']",
            "input[ng-model*='docType']",
            "input[name*='ocType']",
            "input[id*='ocType']",
            "input[id*='DocumentType']",
        ]:
            try:
                el = page.locator(inp_sel).first
                if await el.count() > 0:
                    await el.triple_click(timeout=4000)
                    await el.fill(all_codes)
                    filled = True
                    log.info("Playwright: filled Document Types = %s", all_codes)
                    break
            except Exception:
                pass
        if not filled:
            log.warning("Playwright: could not find Document Types input")

        # ── click "7 Days" ────────────────────────────────────────────────────
        clicked_quick = False
        for ql_sel in ["text='7 Days'", "a:has-text('7 Days')", "button:has-text('7 Days')"]:
            try:
                el = page.locator(ql_sel).first
                if await el.count() > 0:
                    await el.click(timeout=5000)
                    await page.wait_for_timeout(400)
                    clicked_quick = True
                    log.info("Playwright: clicked '7 Days'")
                    break
            except Exception:
                pass

        if not clicked_quick:
            log.warning("'7 Days' not found - filling dates manually")
            try:
                date_inputs = await page.locator(
                    "input[placeholder='MM/DD/YYYY']"
                ).all()
                if len(date_inputs) >= 2:
                    await date_inputs[0].fill(start_date)
                    await date_inputs[1].fill(end_date)
                    log.info("Playwright: filled date range %s - %s", start_date, end_date)
            except Exception as exc:
                log.warning("Date fill failed: %s", exc)

        # ── click Search ──────────────────────────────────────────────────────
        searched = False
        for btn_sel in [
            "input[value='Search']",
            "button:has-text('Search')",
            "input[type='submit']",
            "[type='submit']",
        ]:
            try:
                el = page.locator(btn_sel).first
                if await el.count() > 0:
                    await el.click(timeout=6000)
                    searched = True
                    log.info("Playwright: clicked Search")
                    break
            except Exception:
                pass
        if not searched:
            log.error("Playwright: Search button not found")
            await browser.close()
            return []

        # ── wait for results ──────────────────────────────────────────────────
        try:
            await page.wait_for_selector(
                "table tbody tr, .result-row, [class*='result']",
                timeout=25000,
            )
            await page.wait_for_timeout(1500)
        except PWTimeout:
            log.warning("Playwright: results table timeout - checking XHR data")

        # ── process captured XHR data ─────────────────────────────────────────
        if captured:
            log.info("Playwright: using %d XHR-captured rows", len(captured))
            for row in captured:
                dt  = str(row.get("DocType") or row.get("docType") or "").upper()
                rec = NewVisionClient._normalise(row, dt or "UNKNOWN")
                if rec:
                    records.append(rec)
        else:
            # ── paginate HTML results ─────────────────────────────────────────
            page_num = 0
            while True:
                page_num += 1
                html     = await page.content()
                new_recs = _parse_newvision_html(html)
                records.extend(new_recs)
                log.debug("HTML page %d: %d records (total %d)",
                          page_num, len(new_recs), len(records))

                next_btn = page.locator(
                    "a:has-text('Next'), button:has-text('Next'), "
                    "a:has-text('>>'), [aria-label='Next page']"
                )
                if await next_btn.count() == 0:
                    break
                cls = await next_btn.first.get_attribute("class") or ""
                if "disabled" in cls.lower():
                    break
                try:
                    await next_btn.first.click(timeout=5000)
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    await page.wait_for_timeout(600)
                except Exception:
                    break
                if page_num >= 50:
                    break

        await browser.close()

    return records


def _parse_newvision_html(html: str) -> list[dict]:
    soup    = BeautifulSoup(html, "lxml")
    records = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        headers = [c.get_text(strip=True).lower()
                   for c in rows[0].find_all(["th","td"])]
        if not any(k in " ".join(headers)
                   for k in ["instrument","type","grantor","date","recorded"]):
            continue
        col = {}
        for i, h in enumerate(headers):
            if any(k in h for k in ["instrument","instr","cf#","doc #","number"]):
                col.setdefault("doc_num",  i)
            if "type" in h and "instrument" not in h:
                col.setdefault("doc_type", i)
            if any(k in h for k in ["date","filed","recorded"]):
                col.setdefault("date",     i)
            if any(k in h for k in ["grantor","seller","from"]):
                col.setdefault("grantor",  i)
            if any(k in h for k in ["grantee","buyer"]):
                col.setdefault("grantee",  i)
            if any(k in h for k in ["amount","consideration"]):
                col.setdefault("amount",   i)
            if any(k in h for k in ["legal","description"]):
                col.setdefault("legal",    i)
        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells:
                continue
            texts = [c.get_text(" ", strip=True) for c in cells]
            def get(key, default=0):
                idx = col.get(key, default)
                return texts[idx].strip() if 0 <= idx < len(texts) else ""
            doc_num = get("doc_num", 0)
            if not doc_num or doc_num.lower() in ("instrument #","cf#"):
                continue
            link = ""
            li   = col.get("doc_num", 0)
            if li < len(cells):
                a = cells[li].find("a", href=True)
                if a:
                    h2 = a["href"]
                    link = h2 if h2.startswith("http") else urljoin(NEWVISION_BASE, h2)
            records.append({
                "doc_num":  doc_num,
                "doc_type": get("doc_type", 1).upper(),
                "filed":    _norm_date(get("date", 2)),
                "grantor":  get("grantor", 3),
                "grantee":  get("grantee", 4),
                "amount":   _parse_amount(get("amount")),
                "legal":    get("legal"),
                "clerk_url":link,
            })
    return records


# ── orchestrator ──────────────────────────────────────────────────────────────
async def scrape_clerk(
        doc_types: list[str], start_date: str, end_date: str) -> list[dict]:
    """Try JSON API first; fall back to Playwright."""

    log.info("Track 1: NewVision JSON API...")
    client  = NewVisionClient()
    api_url = client._find_api()
    records: list[dict] = []

    if api_url:
        for dt in doc_types:
            try:
                recs = client.search(dt, start_date, end_date)
                records.extend(recs)
                log.info("  API %-12s -> %d records", dt, len(recs))
            except Exception as exc:
                log.error("  API %s: %s", dt, exc)
            time.sleep(0.2)

    if not records:
        log.info("Track 2: Playwright on %s ...", NEWVISION_BROWSE)
        try:
            records = await _playwright_search(doc_types, start_date, end_date)
            log.info("Playwright total: %d records", len(records))
        except Exception as exc:
            log.error("Playwright failed: %s", exc)
            log.debug(traceback.format_exc())

    # Dedup by doc_num
    seen:   set[str]  = set()
    unique: list[dict] = []
    for r in records:
        key = r.get("doc_num", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(r)
        elif not key:
            unique.append(r)
    return unique


# ── score & flags ─────────────────────────────────────────────────────────────
def compute_flags_and_score(rec: dict, today: datetime) -> tuple[list[str], int]:
    flags    = []
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

    score = 30
    score += 10 * len([f for f in flags
                        if f not in ("New this week","LLC / corp owner")])
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
    return flags, min(score, 100)


# ── enrich ────────────────────────────────────────────────────────────────────
def enrich(raw: list[dict], parcel: ParcelLookup, today: datetime) -> list[dict]:
    out = []
    for r in raw:
        try:
            dt       = r.get("doc_type","").upper()
            cat, lbl = DOC_TYPES.get(dt, ("other", dt))
            owner    = r.get("grantor","")
            p        = parcel.lookup(owner) if owner else {}
            rec = {
                "doc_num":     r.get("doc_num",""),
                "doc_type":    dt,
                "filed":       r.get("filed",""),
                "cat":         cat,
                "cat_label":   lbl,
                "owner":       owner,
                "grantee":     r.get("grantee",""),
                "amount":      r.get("amount", 0),
                "legal":       r.get("legal",""),
                "prop_address":p.get("prop_address",""),
                "prop_city":   p.get("prop_city",""),
                "prop_state":  p.get("prop_state","FL"),
                "prop_zip":    p.get("prop_zip",""),
                "mail_address":p.get("mail_address",""),
                "mail_city":   p.get("mail_city",""),
                "mail_state":  p.get("mail_state","FL"),
                "mail_zip":    p.get("mail_zip",""),
                "clerk_url":   r.get("clerk_url",""),
                "flags":       [],
                "score":       0,
            }
            rec["flags"], rec["score"] = compute_flags_and_score(rec, today)
            out.append(rec)
        except Exception as exc:
            log.debug("Enrich error: %s | %s", exc, r)
    return out


# ── output ────────────────────────────────────────────────────────────────────
def save_json(records: list[dict], today: datetime,
              start_date: str, end_date: str):
    payload = {
        "fetched_at":  today.isoformat(),
        "source":      "Osceola County Official Records - officialrecords.osceolaclerk.org",
        "date_range":  {"from": start_date, "to": end_date},
        "total":       len(records),
        "with_address":sum(1 for r in records
                           if r.get("prop_address") or r.get("mail_address")),
        "records":     sorted(records, key=lambda r: r.get("score",0), reverse=True),
    }
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2))
        log.info("Saved %s (%d records)", path, len(records))


def save_ghl_csv(records: list[dict]):
    GHL_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "First Name","Last Name","Mailing Address","Mailing City",
        "Mailing State","Mailing Zip","Property Address","Property City",
        "Property State","Property Zip","Lead Type","Document Type",
        "Date Filed","Document Number","Amount/Debt Owed","Seller Score",
        "Motivated Seller Flags","Source","Public Records URL",
    ]
    with GHL_CSV_PATH.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in records:
            parts = r.get("owner","").replace(",","").split()
            w.writerow({
                "First Name":          parts[0] if parts else "",
                "Last Name":           " ".join(parts[1:]) if len(parts)>1 else "",
                "Mailing Address":     r.get("mail_address",""),
                "Mailing City":        r.get("mail_city",""),
                "Mailing State":       r.get("mail_state","FL"),
                "Mailing Zip":         r.get("mail_zip",""),
                "Property Address":    r.get("prop_address",""),
                "Property City":       r.get("prop_city",""),
                "Property State":      r.get("prop_state","FL"),
                "Property Zip":        r.get("prop_zip",""),
                "Lead Type":           r.get("cat_label",""),
                "Document Type":       r.get("doc_type",""),
                "Date Filed":          r.get("filed",""),
                "Document Number":     r.get("doc_num",""),
                "Amount/Debt Owed":    r.get("amount",""),
                "Seller Score":        r.get("score",0),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source":              "Osceola County Official Records",
                "Public Records URL":  r.get("clerk_url",""),
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
    log.info("Portal : %s", NEWVISION_BROWSE)
    log.info("Range  : %s -> %s  (%d days)", start_date, end_date, LOOKBACK)
    log.info("Types  : %s", ", ".join(DOC_TYPES.keys()))
    log.info("=" * 60)

    parcel = ParcelLookup()
    parcel.load()

    raw = await scrape_clerk(list(DOC_TYPES.keys()), start_date, end_date)
    log.info("Raw records: %d", len(raw))

    records = enrich(raw, parcel, today)
    save_json(records, today, start_date, end_date)
    save_ghl_csv(records)

    log.info("Done. %d leads, %d with addresses.",
             len(records),
             sum(1 for r in records
                 if r.get("prop_address") or r.get("mail_address")))


if __name__ == "__main__":
    asyncio.run(main())
