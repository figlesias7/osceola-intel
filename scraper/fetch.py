"""
Osceola County Florida – Motivated Seller Lead Scraper
=======================================================
Confirmed API endpoint (from DevTools):
  POST https://officialrecords.osceolaclerk.org/browserview/api/search

Confirmed payload shape:
  {
    "Party":      "",          # MUST be empty — search by doc type only
    "DocTypes":   "LP,NOFC",   # comma-separated codes
    "FromDate":   "20260401",  # YYYYMMDD
    "ToDate":     "20260408",  # YYYYMMDD
    "MaxRows":    500,         # rows per page
    "RowsPerPage":500,
    "StartRow":   0            # 0-based offset for pagination
  }

Confirmed response fields (from live API sample):
  doc_id, party_code, party_name, cross_party_name,
  rec_date ("2026-01-13T00:00:00"), doc_type, file_num,
  book, page, legal_1, doc_status,
  _total_rows, _start_row, _end_row, _max_rows

Key facts:
  - Same document appears multiple times (once per party name variant)
    → deduplicate on file_num
  - party_name  = grantor/owner
  - cross_party_name = grantee
  - Comma-separated DocTypes confirmed working
"""

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
API_URL    = "https://officialrecords.osceolaclerk.org/browserview/api/search"
BROWSE_URL = "https://officialrecords.osceolaclerk.org/browserview/"
DOC_URL    = "https://officialrecords.osceolaclerk.org/browserview/?InstrumentNumber={}"

LOOKBACK     = int(os.getenv("LOOKBACK_DAYS", "7"))
OUTPUT_PATHS = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_CSV_PATH = Path("data/ghl_export.csv")

PAGE_SIZE  = 500   # rows per API call
BATCH_SIZE = 4     # doc-type codes per API call (comma-separated)

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

SESSION_HEADERS = {
    "User-Agent":       "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":           "application/json, text/plain, */*",
    "Content-Type":     "application/json",
    "Referer":          BROWSE_URL,
    "Origin":           "https://officialrecords.osceolaclerk.org",
    "X-Requested-With": "XMLHttpRequest",
}

# ── helpers ───────────────────────────────────────────────────────────────────
def _norm_date(raw) -> str:
    """Normalise any date format to YYYY-MM-DD."""
    if not raw:
        return ""
    raw = str(raw).strip()
    # ISO with time: "2026-01-13T00:00:00" → strip time
    raw = raw.split("T")[0]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d", "%m-%d-%Y",
                "%d-%b-%Y", "%B %d, %Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
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
    Direct client for the confirmed NewVision API.

    Pagination:
      Each response row contains _total_rows, _start_row, _end_row.
      We keep requesting with increasing StartRow until we have all rows.

    Deduplication:
      The API returns one row per party-name variant on a document.
      E.g. file_num 2026004910 appears 3× for "SMITH LANIS B",
      "SMITH LANIS BURT", "SMITH AMANDA R" on the same document.
      We dedup on file_num, keeping the row with party_code == "D"
      (direct/grantor) when available, else the first row seen.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(SESSION_HEADERS)
        self._seeded = False

    def _seed(self):
        if self._seeded:
            return
        try:
            r = self.session.get(BROWSE_URL, timeout=20)
            log.info("Session seeded  (HTTP %d)", r.status_code)
        except Exception as exc:
            log.warning("Session seed failed: %s", exc)
        self._seeded = True

    # ── single paginated fetch for one batch of doc types ────────────────────
    def fetch_batch(self, doc_codes: list[str],
                    from_date: datetime, to_date: datetime) -> list[dict]:
        """
        Fetch ALL pages for a comma-joined set of doc-type codes.
        Returns raw API rows (may contain duplicates — caller dedupes).
        """
        self._seed()
        codes     = ",".join(doc_codes)
        all_rows  : list[dict] = []
        start_row = 0

        while True:
            payload = {
                "Party":      "",          # empty = no name filter
                "DocTypes":   codes,
                "FromDate":   _yyyymmdd(from_date),
                "ToDate":     _yyyymmdd(to_date),
                "MaxRows":    PAGE_SIZE,
                "RowsPerPage":PAGE_SIZE,
                "StartRow":   start_row,
            }

            rows = self._post(payload)
            if not rows:
                break

            all_rows.extend(rows)

            # Read pagination metadata from first row
            total = rows[0].get("_total_rows") or rows[0].get("_max_rows") or 0
            end   = rows[0].get("_end_row", start_row + len(rows))
            log.debug("  [%s] rows %d-%d of %d", codes, start_row + 1, end, total)

            if end >= total or len(rows) < PAGE_SIZE:
                break
            start_row = end   # next page starts where this one ended

        return all_rows

    def _post(self, payload: dict) -> list[dict]:
        for attempt in range(3):
            try:
                r = self.session.post(API_URL, json=payload, timeout=30)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, list):
                        return data
                    # wrapped response
                    for key in ("results", "records", "data", "items"):
                        if isinstance(data.get(key), list):
                            return data[key]
                    log.warning("Unexpected JSON shape: %s", str(data)[:300])
                    return []
                log.warning("HTTP %d (attempt %d)", r.status_code, attempt + 1)
            except Exception as exc:
                log.error("POST error (attempt %d): %s", attempt + 1, exc)
            time.sleep(2 ** attempt)
        return []

    # ── normalise one raw API row ─────────────────────────────────────────────
    @staticmethod
    def normalise(row: dict) -> Optional[dict]:
        """
        Map confirmed API field names to our internal schema.

        Confirmed fields:
          file_num          → doc_num
          doc_type          → doc_type
          rec_date          → filed  (ISO "2026-01-13T00:00:00")
          party_name        → grantor / owner
          cross_party_name  → grantee
          legal_1           → legal
          book, page        → for building clerk URL
        """
        file_num = str(row.get("file_num") or "").strip()
        if not file_num:
            return None

        doc_type = str(row.get("doc_type") or "").upper().strip()
        filed    = _norm_date(row.get("rec_date", ""))
        grantor  = str(row.get("party_name") or "").strip()
        grantee  = str(row.get("cross_party_name") or "").strip()
        legal    = str(row.get("legal_1") or "").strip()
        # consid_1 may or may not be present in this endpoint
        amount   = _parse_amount(row.get("consid_1") or row.get("amount") or 0)

        clerk_url = DOC_URL.format(file_num)

        return {
            "doc_num":   file_num,
            "doc_type":  doc_type,
            "filed":     filed,
            "grantor":   grantor,
            "grantee":   grantee,
            "amount":    amount,
            "legal":     legal,
            "clerk_url": clerk_url,
            # keep party_code for dedup preference (D = direct/grantor)
            "_party_code": str(row.get("party_code") or "").upper(),
        }


# ── scrape orchestrator ───────────────────────────────────────────────────────
def scrape_clerk(doc_types: list[str],
                 from_date: datetime, to_date: datetime) -> list[dict]:
    """
    Batch doc types → hit confirmed API → paginate → dedup → return records.
    """
    api      = OsceolaAPI()
    # file_num → best row seen so far
    seen: dict[str, dict] = {}

    batches = [doc_types[i:i+BATCH_SIZE]
               for i in range(0, len(doc_types), BATCH_SIZE)]

    for batch in batches:
        codes = ",".join(batch)
        log.info("Fetching [%s]  %s → %s",
                 codes, _yyyymmdd(from_date), _yyyymmdd(to_date))

        raw_rows = api.fetch_batch(batch, from_date, to_date)
        batch_new = 0

        for row in raw_rows:
            rec = OsceolaAPI.normalise(row)
            if not rec:
                continue

            fn = rec["doc_num"]
            pc = rec.pop("_party_code", "")

            if fn not in seen:
                seen[fn] = rec
                batch_new += 1
            else:
                # Prefer the "direct" party (grantor) over indirect
                if pc == "D" and seen[fn].get("_kept_code") != "D":
                    seen[fn] = rec
                    seen[fn]["_kept_code"] = "D"

        log.info("  → %d unique new records  (batch raw: %d)",
                 batch_new, len(raw_rows))
        time.sleep(0.5)

    records = list(seen.values())
    # strip internal key
    for r in records:
        r.pop("_kept_code", None)

    log.info("Total unique records: %d", len(records))
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
            log.warning("Parcel DBF unavailable – address enrichment disabled.")
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
            log.info("Parcels loaded: %d records, %d name variants",
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
def compute_flags_and_score(rec: dict,
                            today: datetime) -> tuple[list[str], int]:
    flags    = []
    doc_type = rec.get("doc_type", "").upper()
    cat      = rec.get("cat", "")
    owner    = rec.get("owner", "")
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
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b|\bTRUST\b",
                 owner.upper()):
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
        "date_range": {
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
            # party_name format is "LAST FIRST" — split for GHL
            parts = r.get("owner", "").split()
            first = parts[-1] if len(parts) > 1 else ""
            last  = " ".join(parts[:-1]) if len(parts) > 1 else parts[0] if parts else ""
            w.writerow({
                "First Name":             first,
                "Last Name":              last,
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

    parcel = ParcelLookup()
    parcel.load()

    raw = scrape_clerk(list(DOC_TYPES.keys()), from_date, today)
    log.info("Raw unique records: %d", len(raw))

    records = enrich(raw, parcel, today)
    save_json(records, today, from_date, today)
    save_ghl_csv(records)

    log.info("Done. %d leads, %d with addresses.",
             len(records),
             sum(1 for r in records
                 if r.get("prop_address") or r.get("mail_address")))


if __name__ == "__main__":
    main()
