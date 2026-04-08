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
CLERK_BASE   = "https://officialrecords.osceolaclerk.org/browserview/"
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
    "TAX": ("tax",          "Tax Deed"),
    "JUDG":     ("judgment",     "Judgment"),
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


# ── clerk portal scraper ──────────────────────────────────────────────────────
async def scrape_clerk(doc_types: list[str], start_date: str, end_date: str) -> list[dict]:
    """
    Use Playwright to drive the Osceola Clerk records search portal.
    Returns a list of raw record dicts.
    """
    try:
        from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    except ImportError:
        log.error("playwright not installed")
        return []

    records = []
    log.info("Launching Playwright for clerk portal…")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        for doc_type in doc_types:
            try:
                recs = await _search_doc_type(page, doc_type, start_date, end_date)
                records.extend(recs)
                log.info("  %s → %d records", doc_type, len(recs))
            except Exception as exc:
                log.error("Failed scraping %s: %s", doc_type, exc)
                log.debug(traceback.format_exc())

        await browser.close()

    return records


async def _search_doc_type(page, doc_type: str, start_date: str, end_date: str) -> list[dict]:
    """Search the clerk portal for one document type."""
    from playwright.async_api import TimeoutError as PWTimeout

    records = []

    for attempt in range(3):
        try:
            await page.goto(CLERK_BASE, wait_until="networkidle", timeout=30000)
            break
        except PWTimeout:
            if attempt == 2:
                log.warning("Timed out loading clerk portal for %s", doc_type)
                return []
            await asyncio.sleep(3)

    # Look for a search form – try several common patterns
    # Pattern 1: direct search form on the page
    found_form = False

    # Try to find document type input / dropdown
    try:
        # Many FL clerk portals use an iframe or redirect to a search app
        frames = page.frames
        for frame in frames:
            content = await frame.content()
            if "doc_type" in content.lower() or "document type" in content.lower():
                page = frame
                break
    except Exception:
        pass

    # Fill date range
    try:
        # Try common field names/IDs
        for sel in ["#DateFrom", "#BeginDate", "#startDate", "input[name*='date_from']",
                    "input[name*='DateFrom']", "input[name*='begin']"]:
            if await page.locator(sel).count() > 0:
                await page.fill(sel, start_date)
                break

        for sel in ["#DateTo", "#EndDate", "#endDate", "input[name*='date_to']",
                    "input[name*='DateTo']", "input[name*='end']"]:
            if await page.locator(sel).count() > 0:
                await page.fill(sel, end_date)
                break

        # Doc type
        for sel in ["#DocType", "#documentType", "select[name*='doc']",
                    "input[name*='DocType']"]:
            if await page.locator(sel).count() > 0:
                tag = await page.locator(sel).evaluate("el => el.tagName")
                if tag == "SELECT":
                    await page.select_option(sel, doc_type)
                else:
                    await page.fill(sel, doc_type)
                found_form = True
                break

    except Exception as exc:
        log.debug("Form fill error for %s: %s", doc_type, exc)

    if not found_form:
        # Fallback: try URL-based search (some FL clerk portals accept GET params)
        fallback_urls = [
            f"{CLERK_BASE}?DocType={doc_type}&DateFrom={start_date}&DateTo={end_date}",
            f"https://or.osceolaclerk.com/or/Search.aspx?DocType={doc_type}&DateFrom={start_date}&DateTo={end_date}",
            f"https://www.osceolaclerk.com/official-records/?doc_type={doc_type}&from={start_date}&to={end_date}",
        ]
        for url in fallback_urls:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                content = await page.content()
                recs = _parse_clerk_results(content, doc_type)
                if recs:
                    return recs
            except Exception:
                pass
        return []

    # Submit search
    try:
        for sel in ["#SearchButton", "button[type='submit']", "input[type='submit']",
                    "#btnSearch", ".search-button"]:
            if await page.locator(sel).count() > 0:
                await page.click(sel)
                await page.wait_for_load_state("networkidle", timeout=20000)
                break
    except Exception as exc:
        log.debug("Submit error for %s: %s", doc_type, exc)
        return []

    # Paginate through results
    page_num = 0
    while True:
        page_num += 1
        content = await page.content()
        new_recs = _parse_clerk_results(content, doc_type)
        records.extend(new_recs)

        # Check for next page
        next_sel = "a:has-text('Next'), a:has-text('>'), .pager-next, #nextPage"
        if await page.locator(next_sel).count() > 0:
            try:
                await page.click(next_sel, timeout=5000)
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                break
        else:
            break

        if page_num > 50:  # safety cap
            break

    return records


def _parse_clerk_results(html: str, doc_type: str) -> list[dict]:
    """Parse clerk search results HTML into record dicts."""
    soup = BeautifulSoup(html, "lxml")
    records = []

    # Common table patterns for FL clerk portals
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # Detect header row
        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        if not any(k in " ".join(headers) for k in ["doc", "instrument", "book", "grantor", "date"]):
            continue

        col = _map_columns(headers)

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            try:
                rec = _extract_row(cells, col, doc_type, soup.find("base", href=True))
                if rec:
                    records.append(rec)
            except Exception:
                pass

    # Also try definition-list / card style layouts
    for item in soup.select(".record-item, .result-row, .instrument-row"):
        try:
            rec = _extract_card(item, doc_type)
            if rec:
                records.append(rec)
        except Exception:
            pass

    return records


def _map_columns(headers: list[str]) -> dict:
    """Map semantic names to column indices."""
    col = {}
    for i, h in enumerate(headers):
        if any(k in h for k in ["instrument", "doc #", "doc num", "book", "cf#"]):
            col.setdefault("doc_num", i)
        if any(k in h for k in ["type", "doc type"]):
            col.setdefault("doc_type", i)
        if any(k in h for k in ["date", "filed", "recorded"]):
            col.setdefault("date", i)
        if any(k in h for k in ["grantor", "seller", "owner", "from"]):
            col.setdefault("grantor", i)
        if any(k in h for k in ["grantee", "buyer", "to"]):
            col.setdefault("grantee", i)
        if any(k in h for k in ["amount", "consideration", "value"]):
            col.setdefault("amount", i)
        if any(k in h for k in ["legal", "description", "desc"]):
            col.setdefault("legal", i)
    return col


def _extract_row(cells: list, col: dict, doc_type: str, base_tag) -> Optional[dict]:
    texts = [c.get_text(strip=True) for c in cells]
    if not any(texts):
        return None

    doc_num  = texts[col.get("doc_num",  0)] if "doc_num"  in col else ""
    filed    = texts[col.get("date",     1)] if "date"     in col else ""
    grantor  = texts[col.get("grantor",  2)] if "grantor"  in col else ""
    grantee  = texts[col.get("grantee",  3)] if "grantee"  in col else ""
    amount   = texts[col.get("amount",   4)] if "amount"   in col else ""
    legal    = texts[col.get("legal",    5)] if "legal"    in col else ""
    dtype    = texts[col.get("doc_type", -1)] if "doc_type" in col else doc_type

    if not doc_num and not grantor:
        return None

    # Find direct link
    link = ""
    for cell in cells:
        a = cell.find("a", href=True)
        if a:
            href = a["href"]
            if base_tag:
                href = urljoin(base_tag["href"], href)
            elif not href.startswith("http"):
                href = urljoin(CLERK_BASE, href)
            link = href
            break

    return {
        "doc_num":  doc_num,
        "doc_type": dtype or doc_type,
        "filed":    _norm_date(filed),
        "grantor":  grantor,
        "grantee":  grantee,
        "amount":   _parse_amount(amount),
        "legal":    legal,
        "clerk_url":link,
    }


def _extract_card(item, doc_type: str) -> Optional[dict]:
    text = item.get_text(" ", strip=True)
    link = ""
    a = item.find("a", href=True)
    if a:
        link = urljoin(CLERK_BASE, a["href"])

    return {
        "doc_num":  _re_find(r"(?:Instr|Doc|CF)#?\s*([\w-]+)", text),
        "doc_type": doc_type,
        "filed":    _norm_date(_re_find(r"(\d{1,2}/\d{1,2}/\d{4})", text)),
        "grantor":  _re_find(r"Grantor[:\s]+([^\n;]+)", text),
        "grantee":  _re_find(r"Grantee[:\s]+([^\n;]+)", text),
        "amount":   _parse_amount(_re_find(r"\$[\d,\.]+", text)),
        "legal":    _re_find(r"Legal[:\s]+([^\n;]{5,})", text),
        "clerk_url":link,
    } if _re_find(r"(?:Instr|Doc|CF)#?\s*([\w-]+)", text) else None


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
