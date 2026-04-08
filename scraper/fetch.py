"""
Osceola County Florida – Motivated Seller Lead Scraper
=======================================================
Confirmed API (from DevTools):
  POST https://officialrecords.osceolaclerk.org/browserview/api/search
  Payload: {
    "Party":      "",          # leave blank – we search by doc type only
    "DocTypes":   "LP",        # comma-separated codes, e.g. "LP,NOFC"
    "FromDate":   "20260401",  # YYYYMMDD
    "ToDate":     "20260408",  # YYYYMMDD
    "MaxRows":    0,
    "RowsPerPage":0,
    "StartRow":   0
  }
  Response: JSON array of record objects.
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

# ── confirmed constants ────────────────────────────────────────────────────────
API_URL      = "https://officialrecords.osceolaclerk.org/browserview/api/search"
BROWSE_URL   = "https://officialrecords.osceolaclerk.org/browserview/"
DOC_URL      = "https://officialrecords.osceolaclerk.org/browserview/?InstrumentNumber={}"

LOOKBACK     = int(os.getenv("LOOKBACK_DAYS", "7"))
OUTPUT_PATHS = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_CSV_PATH = Path("data/ghl_export.csv")

# Batch size: how many doc-type codes to send per API call (comma-separated).
# The portal accepts multiple; we keep batches small to avoid server limits.
BATCH_SIZE = 4

# Document type map  {code: (category, label)}
DOC_TYPES = {
    "LP":       ("foreclosure", "Lis Pendens"),
    "DEATH":    ("death",      "death certificate"),
    "NOFC":     ("foreclosure", "Notice of Foreclosure"),
    "TAXDEED":  ("tax",         "Tax Deed"),
    "TAX":      ("tax",         "Tax Lien"),
    "JUDG":     ("judgment",    "Judgment"),
    "CCJ":      ("judgment",    "Certified Judgment"),
    "DRJUD":    ("judgment",    "Domestic Judgment"),
    "F JUDG":   ("judgment",   "Foreign Judgment"),
    "LNCORPTX": ("lien",       "Corp Tax Lien"),
    "LNIRS":    ("lien",       "IRS Lien"),
    "FTL":      ("lien",       "Federal Lien"),
    "LIEN":     ("lien",       "Lien"),
    "LNMECH":   ("lien",       "Mechanic Lien"),
    "LNHOA":    ("lien",       "HOA Lien"),
    "MEDLIEN":  ("lien",       "Medicaid Lien"),
    "PROB":     ("probate",    "Probate Document"),
    "PROBATE":  ("probate",    "Probate"), 
    "NOC":      ("notice",     "Notice of Commencement"),
    "RELLP":    ("notice",     "Release Lis Pendens"),
}

SESSION_HEADERS = {
    "User-Agent":     "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":         "application/json, text/plain, */*",
    "Content-Type":   "application/json",
    "Referer":        BROWSE_URL,
    "Origin":         "https://officialrecords.osceolaclerk.org",
    "X-Requested-With": "XMLHttpRequest",
}

# ── helpers ───────────────────────────────────────────────────────────────────
def _norm_date(raw: str) -> str:
    raw = (raw or "").strip()
    for fmt in ("%m/%d/%Y", "%Y%m%d", "%Y-%m-%d", "%m-%d-%Y",
                "%d-%b-%Y", "%B %d, %Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Handle millisecond timestamps: "/Date(1234567890000)/"
    m = re.search(r"/Date\((\d+)\)/", raw)
    if m:
        return datetime.utcfromtimestamp(int(m.group(1)) / 1000).strftime("%Y-%m-%d")
    return raw

def _parse_amount(raw) -> float:
    if not raw:
        return 0.0
    try:
        return float(re.sub(r"[^\d.]", "", str(raw)))
    except ValueError:
        return 0.0

def _is_recent(date_str: str, today: datetime, days: int = 7) -> bool:
    try:
        return (today - datetime.strptime(date_str, "%Y-%m-%d")).days <= days
    except Exception:
        return False

def _yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


# ── API client ────────────────────────────────────────────────────────────────
class OsceolaAPI:
    """
    Thin wrapper around the confirmed NewVision API endpoint.

    Confirmed payload shape (from browser DevTools):
      {
        "Party":      "",        # left blank – filter by doc type only
        "DocTypes":   "LP",      # comma-separated type codes
        "FromDate":   "20260401",
        "ToDate":     "20260408",
        "MaxRows":    0,
        "RowsPerPage":0,
        "StartRow":   0
      }
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(SESSION_HEADERS)
        self._seeded = False

    def _seed_session(self):
        """Load the SPA once so the server sets any required session cookies."""
        if self._seeded:
            return
        try:
            r = self.session.get(BROWSE_URL, timeout=20)
            log.info("Session seeded from %s  (HTTP %d)", BROWSE_URL, r.status_code)
        except Exception as exc:
            log.warning("Session seed failed: %s", exc)
        self._seeded = True

    def search_batch(self, doc_types: list[str],
                     from_date: datetime, to_date: datetime) -> list[dict]:
        """
        Call the API for a batch of doc-type codes.
        Returns the raw JSON array (list of record dicts).
        """
        self._seed_session()

        payload = {
            "Party":      "",
            "DocTypes":   ",".join(doc_types),
            "FromDate":   _yyyymmdd(from_date),
            "ToDate":     _yyyymmdd(to_date),
            "MaxRows":    0,
            "RowsPerPage":0,
            "StartRow":   0,
        }

        for attempt in range(3):
            try:
                log.debug("POST %s  payload=%s", API_URL, payload)
                r = self.session.post(API_URL, json=payload, timeout=30)
                log.debug("HTTP %d  len=%d", r.status_code, len(r.content))

                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, list):
                        return data
                    # Some NewVision versions wrap in an object
                    for key in ("results","records","data","items","Documents"):
                        if key in data and isinstance(data[key], list):
                            return data[key]
                    log.warning("Unexpected JSON shape: %s", str(data)[:200])
                    return []

                log.warning("HTTP %d for batch %s (attempt %d)",
                            r.status_code, doc_types, attempt + 1)
                time.sleep(2 ** attempt)

            except Exception as exc:
                log.error("API error (attempt %d): %s", attempt + 1, exc)
                time.sleep(2 ** attempt)

        return []

    def normalise(self, row: dict, batch_types: list[str]) -> Optional[dict]:
        """
        Map a raw API row to our internal schema.
        NewVision field names seen in typical responses:
          file_num, doc_type, rec_date, direct_name, indirect_name,
          legal_1, consid_1, book, page
        """
        def g(*keys):
            for k in keys:
                v = (row.get(k) or row.get(k.lower()) or
                     row.get(k.upper()) or "")
                if v:
                    return str(v).strip()
            return ""

        # Instrument / document number
        doc_num = g("file_num", "InstrumentNumber", "instrument_number",
                    "cfn", "CFN", "DocNumber")
        if not doc_num:
            return None

        doc_type = g("doc_type", "DocType", "type", "Type") or batch_types[0]
        filed    = _norm_date(g("rec_date", "RecordedDate", "date", "Date",
                                "filed_date"))
        grantor  = g("direct_name", "Grantor", "grantor", "Party1",
                     "direct_party", "grantorName")
        grantee  = g("indirect_name", "Grantee", "grantee", "Party2",
                     "indirect_party", "granteeName")
        amount   = _parse_amount(g("consid_1", "Amount", "Consideration",
                                   "consideration", "amount"))
        legal    = g("legal_1", "LegalDescription", "legal", "Legal")

        # Build a direct deep-link into the portal
        clerk_url = DOC_URL.format(doc_num.replace(" ", ""))

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


# ── scrape orchestrator ───────────────────────────────────────────────────────
def scrape_clerk(doc_types: list[str],
                 from_date: datetime, to_date: datetime) -> list[dict]:
    """
    Batch doc types, hit the confirmed API, return normalised records.
    Falls back to Playwright if the API returns nothing at all.
    """
    api     = OsceolaAPI()
    raw_all : list[dict] = []

    # Split into batches
    batches = [doc_types[i:i+BATCH_SIZE]
               for i in range(0, len(doc_types), BATCH_SIZE)]

    for batch in batches:
        codes = ",".join(batch)
        rows  = api.search_batch(batch, from_date, to_date)
        log.info("  API [%-40s] -> %d rows", codes, len(rows))

        for row in rows:
            rec = api.normalise(row, batch)
            if rec:
                raw_all.append(rec)

        time.sleep(0.5)   # be polite

    if raw_all:
        log.info("API total: %d records", len(raw_all))
        return _dedup(raw_all)

    # ── Playwright fallback ───────────────────────────────────────────────────
    log.warning("API returned 0 records — trying Playwright fallback")
    try:
        pw_recs = asyncio.run(_playwright_fallback(doc_types, from_date, to_date))
        log.info("Playwright fallback: %d records", len(pw_recs))
        return _dedup(pw_recs)
    except Exception as exc:
        log.error("Playwright fallback failed: %s", exc)
        log.debug(traceback.format_exc())
        return []


def _dedup(records: list[dict]) -> list[dict]:
    seen:   set[str]   = set()
    unique: list[dict] = []
    for r in records:
        key = r.get("doc_num", "")
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        unique.append(r)
    return unique


# ── Playwright fallback ───────────────────────────────────────────────────────
async def _playwright_fallback(doc_types: list[str],
                               from_date: datetime,
                               to_date: datetime) -> list[dict]:
    """
    Drive the Angular SPA with Playwright.
    Uses XHR interception — captures the same JSON the API returns,
    but lets the browser handle auth/cookies automatically.
    """
    try:
        from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    except ImportError:
        log.error("playwright not installed")
        return []

    captured: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = await browser.new_context(
            user_agent=SESSION_HEADERS["User-Agent"],
            viewport={"width": 1440, "height": 900},
        )
        page = await ctx.new_page()

        # Intercept the confirmed API endpoint
        async def on_response(response):
            if "api/search" in response.url and response.status == 200:
                try:
                    body = await response.json()
                    if isinstance(body, list):
                        log.info("XHR intercepted: %d rows from %s",
                                 len(body), response.url)
                        captured.extend(body)
                except Exception:
                    pass

        page.on("response", on_response)

        # Load the SPA
        try:
            await page.goto(BROWSE_URL, wait_until="domcontentloaded", timeout=30000)
        except PWTimeout:
            pass

        # Wait for Angular to render (look for any input)
        try:
            await page.wait_for_selector("input", timeout=20000)
        except PWTimeout:
            log.warning("Playwright: page inputs never appeared")

        # Process each batch
        batches = [doc_types[i:i+BATCH_SIZE]
                   for i in range(0, len(doc_types), BATCH_SIZE)]

        for batch in batches:
            # Use page.evaluate to call the API directly via fetch()
            # This runs inside the browser so it inherits all session cookies
            payload = {
                "Party":      "",
                "DocTypes":   ",".join(batch),
                "FromDate":   _yyyymmdd(from_date),
                "ToDate":     _yyyymmdd(to_date),
                "MaxRows":    0,
                "RowsPerPage":0,
                "StartRow":   0,
            }
            try:
                result = await page.evaluate("""
                    async (payload) => {
                        const r = await fetch('/browserview/api/search', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'Accept': 'application/json'
                            },
                            body: JSON.stringify(payload)
                        });
                        return await r.json();
                    }
                """, payload)
                if isinstance(result, list):
                    log.info("  PW eval [%s] -> %d rows",
                             ",".join(batch), len(result))
                    captured.extend(result)
            except Exception as exc:
                log.warning("PW eval failed for %s: %s", batch, exc)

            await asyncio.sleep(0.5)

        await browser.close()

    # Normalise captured rows
    api     = OsceolaAPI()
    records = []
    for row in captured:
        rec = api.normalise(row, doc_types)
        if rec:
            records.append(rec)
    return records


# ── Property Appraiser parcel lookup ─────────────────────────────────────────
class ParcelLookup:
    PA_CANDIDATES = [
        "https://www.property-appraiser.org/osceola/bulk/parcel.zip",
        "https://www.osceolapropertyappraiser.com/downloads/parcel.zip",
        "https://www.osceolapropertyappraiser.com/bulk/parcel.dbf",
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
            log.warning("Could not download parcel DBF – address enrichment disabled.")
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
                    d = self._try_url(urljoin(self.PA_SEED, a["href"]))
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
            log.warning("dbfread not installed – skipping parcel enrichment")
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
            log.info("Parcels: %d records, %d name variants",
                     len(self._by_parcel), len(self._by_name))
        except Exception as exc:
            log.error("DBF parse: %s", exc)

    @staticmethod
    def _parse_row(row) -> Optional[dict]:
        r = {k.upper(): (str(v).strip() if v else "")
             for k, v in row.items()}
        owner = (r.get("OWNER") or r.get("OWN1") or
                 r.get("OWNERNAME") or "")
        if not owner:
            return None
        return {
            "parcel_id":   (r.get("PARCEL") or r.get("PARCELID") or
                            r.get("APN") or ""),
            "owner":       owner,
            "prop_address":(r.get("SITE_ADDR") or r.get("SITEADDR") or
                            r.get("SITUS") or ""),
            "prop_city":   (r.get("SITE_CITY") or r.get("SITECITY") or
                            "Kissimmee"),
            "prop_state":  "FL",
            "prop_zip":    (r.get("SITE_ZIP") or r.get("SITEZIP") or ""),
            "mail_address":(r.get("ADDR_1") or r.get("MAILADR1") or
                            r.get("MAILADDR") or ""),
            "mail_city":   (r.get("CITY") or r.get("MAILCITY") or ""),
            "mail_state":  (r.get("STATE") or r.get("MAILSTATE") or "FL"),
            "mail_zip":    (r.get("ZIP") or r.get("MAILZIP") or ""),
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
    if _is_recent(filed, today, 7):
        flags.append("New this week")

    score = 30
    score += 10 * len([f for f in flags
                        if f not in ("New this week", "LLC / corp owner")])
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
def enrich(raw: list[dict], parcel: ParcelLookup,
           today: datetime) -> list[dict]:
    out = []
    for r in raw:
        try:
            dt       = r.get("doc_type", "").upper()
            cat, lbl = DOC_TYPES.get(dt, ("other", dt))
            owner    = r.get("grantor", "")
            p        = parcel.lookup(owner) if owner else {}

            rec = {
                "doc_num":     r.get("doc_num", ""),
                "doc_type":    dt,
                "filed":       r.get("filed", ""),
                "cat":         cat,
                "cat_label":   lbl,
                "owner":       owner,
                "grantee":     r.get("grantee", ""),
                "amount":      r.get("amount", 0),
                "legal":       r.get("legal", ""),
                "prop_address":p.get("prop_address", ""),
                "prop_city":   p.get("prop_city", ""),
                "prop_state":  p.get("prop_state", "FL"),
                "prop_zip":    p.get("prop_zip", ""),
                "mail_address":p.get("mail_address", ""),
                "mail_city":   p.get("mail_city", ""),
                "mail_state":  p.get("mail_state", "FL"),
                "mail_zip":    p.get("mail_zip", ""),
                "clerk_url":   r.get("clerk_url", ""),
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
              from_date: datetime, to_date: datetime):
    payload = {
        "fetched_at":  today.isoformat(),
        "source":      "Osceola County Official Records – officialrecords.osceolaclerk.org",
        "date_range":  {
            "from": from_date.strftime("%m/%d/%Y"),
            "to":   to_date.strftime("%m/%d/%Y"),
        },
        "total":       len(records),
        "with_address":sum(1 for r in records
                           if r.get("prop_address") or r.get("mail_address")),
        "records":     sorted(records,
                              key=lambda r: r.get("score", 0), reverse=True),
    }
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2))
        log.info("Saved %s  (%d records)", path, len(records))


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
            parts = r.get("owner", "").replace(",", "").split()
            w.writerow({
                "First Name":             parts[0] if parts else "",
                "Last Name":              " ".join(parts[1:]) if len(parts) > 1 else "",
                "Mailing Address":        r.get("mail_address", ""),
                "Mailing City":           r.get("mail_city", ""),
                "Mailing State":          r.get("mail_state", "FL"),
                "Mailing Zip":            r.get("mail_zip", ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city", ""),
                "Property State":         r.get("prop_state", "FL"),
                "Property Zip":           r.get("prop_zip", ""),
                "Lead Type":              r.get("cat_label", ""),
                "Document Type":          r.get("doc_type", ""),
                "Date Filed":             r.get("filed", ""),
                "Document Number":        r.get("doc_num", ""),
                "Amount/Debt Owed":       r.get("amount", ""),
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":                 "Osceola County Official Records",
                "Public Records URL":     r.get("clerk_url", ""),
            })
    log.info("GHL CSV saved: %s", GHL_CSV_PATH)


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    today     = datetime.now()
    from_date = today - timedelta(days=LOOKBACK)

    log.info("=" * 60)
    log.info("Osceola Motivated Seller Scraper")
    log.info("API    : %s", API_URL)
    log.info("Range  : %s -> %s  (%d days)",
             from_date.strftime("%m/%d/%Y"),
             today.strftime("%m/%d/%Y"),
             LOOKBACK)
    log.info("Types  : %s", ", ".join(DOC_TYPES.keys()))
    log.info("=" * 60)

    # 1. Load parcel data (address enrichment)
    parcel = ParcelLookup()
    parcel.load()

    # 2. Scrape via confirmed API
    raw = scrape_clerk(list(DOC_TYPES.keys()), from_date, today)
    log.info("Raw records: %d", len(raw))

    # 3. Enrich with category, flags, score, addresses
    records = enrich(raw, parcel, today)

    # 4. Save outputs
    save_json(records, today, from_date, today)
    save_ghl_csv(records)

    log.info("Done. %d leads, %d with addresses.",
             len(records),
             sum(1 for r in records
                 if r.get("prop_address") or r.get("mail_address")))


if __name__ == "__main__":
    main()
