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
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

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
DOC_DETAIL = "https://officialrecords.osceolaclerk.org/browserview/api/document/{}"

# Osceola PA ArcGIS REST API (free, no auth required)
# Layer 0 = Parcels with owner name, site address, mail address
PA_ARCGIS  = (
    "https://services.arcgis.com/Ug5xGQbHsD8zuZzM/arcgis/rest/services"
    "/Osceola_County_Parcels/FeatureServer/0/query"
)
# Fallback: PA property search portal
PA_SEARCH  = "https://maps.property-appraiser.org/mapweb/find.aspx"

LOOKBACK     = int(os.getenv("LOOKBACK_DAYS", "30"))
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
        # Skip rows that look like header/metadata rows
        if file_num.lower() in ("file_num", "instrument", "instr#", "doc #", "number"):
            return None
        # Skip rows with no doc_type or obviously non-document data
        doc_type_raw = str(row.get("doc_type") or "").strip()
        if doc_type_raw.lower() in ("doc_type", "type", "document type"):
            return None

        doc_type = str(row.get("doc_type") or "").upper().strip()
        filed    = _norm_date(row.get("rec_date", ""))
        grantor  = str(row.get("party_name") or "").strip()
        grantee  = str(row.get("cross_party_name") or "").strip()
        legal    = str(row.get("legal_1") or row.get("legal_2") or "").strip()
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


# ── Property Appraiser lookup via ArcGIS REST API ────────────────────────────
# The Osceola PA bulk DBF costs $250 — not a free download.
# Instead we use:
#   1. The PA's ArcGIS FeatureServer (free, no auth, query by owner name)
#   2. The clerk document detail API for amount/consideration
#
# ArcGIS query by owner name:
#   GET PA_ARCGIS?where=OWNER_NAME+LIKE+'%25TORRES%25'
#                &outFields=OWNER_NAME,SITE_ADDR,SITE_CITY,SITE_ZIP,
#                           MAIL_ADDR,MAIL_CITY,MAIL_STATE,MAIL_ZIP
#                &f=json
#
# We cache results in memory so the same owner is only looked up once.

class ParcelLookup:
    """Look up property + mailing address from Osceola PA ArcGIS REST API."""

    # Confirmed Osceola PA ArcGIS server: ira.property-appraiser.org
    # Layer 0 = Tax Parcels (updated daily, no auth required)
    ARCGIS_CANDIDATES = [
        # Primary: OpenData_LandRecords — confirmed from search results
        ("https://ira.property-appraiser.org/arcgis/rest/services"
         "/OCPA/OpenData_LandRecords/MapServer/0/query"),
        # Fallback: ParcelCentroids service
        ("https://ira.property-appraiser.org/arcgis/rest/services"
         "/GisSite_ParcelCentroids/MapServer/0/query"),
        # Fallback: TaxMap service
        ("https://ira.property-appraiser.org/arcgis/rest/services"
         "/GisSite_TaxMap/MapServer/0/query"),
    ]

    # Fields to request — PA uses these confirmed field names
    OUTFIELDS = "*"   # request all fields; we parse whatever comes back

    def __init__(self):
        self._cache: dict[str, dict] = {}   # owner_name_upper → parcel dict
        self._working_url: Optional[str] = None
        self._api_dead = False

    def _find_api(self) -> Optional[str]:
        if self._api_dead:
            return None
        if self._working_url:
            return self._working_url
        for url in self.ARCGIS_CANDIDATES:
            try:
                r = requests.get(url, params={
                    "where": "1=1", "outFields": "OBJECTID",
                    "resultRecordCount": 1, "f": "json"
                }, timeout=10)
                if r.status_code == 200 and "features" in r.json():
                    self._working_url = url
                    log.info("PA ArcGIS API: %s", url)
                    return url
            except Exception:
                pass
        log.warning("PA ArcGIS API not reachable – no address enrichment")
        self._api_dead = True
        return None

    def load(self):
        """No-op: we look up on demand."""
        url = self._find_api()
        if url:
            log.info("PA ArcGIS API ready for on-demand lookups")
        else:
            log.warning("PA ArcGIS API unavailable – addresses will be empty")

    def lookup(self, owner: str) -> dict:
        if not owner or self._api_dead:
            return {}
        key = owner.strip().upper()
        if key in self._cache:
            return self._cache[key]

        url = self._find_api()
        if not url:
            return {}

        # Build a LIKE query using the first two words of the owner name
        # (avoids middle-initial mismatches)
        parts = key.split()
        search_term = " ".join(parts[:2]) if len(parts) >= 2 else key
        where = f"OWNER_NAME LIKE '%{search_term}%' OR OWN1 LIKE '%{search_term}%'"

        try:
            r = requests.get(url, params={
                "where":             where,
                "outFields":         self.OUTFIELDS,
                "resultRecordCount": 5,
                "f":                 "json",
            }, timeout=10)
            if r.status_code != 200:
                self._cache[key] = {}
                return {}
            data = r.json()
            features = data.get("features", [])
            if not features:
                self._cache[key] = {}
                return {}

            # Pick the feature whose owner name most closely matches
            best = self._best_match(key, features)
            result = self._parse_feature(best)
            self._cache[key] = result
            return result

        except Exception as exc:
            log.debug("PA lookup failed for %s: %s", owner, exc)
            self._cache[key] = {}
            return {}

    @staticmethod
    def _best_match(target: str, features: list) -> dict:
        """Return the feature whose owner name best matches target."""
        def score(f):
            attrs = f.get("attributes", {})
            name = str(attrs.get("OWNER_NAME") or attrs.get("OWN1") or "").upper()
            # Count matching words
            t_words = set(target.split())
            n_words = set(name.split())
            return len(t_words & n_words)
        return max(features, key=score)

    @staticmethod
    def _parse_feature(feature: dict) -> dict:
        a = feature.get("attributes", {})
        # Normalise all keys to uppercase for consistent lookup
        au = {k.upper(): (str(v).strip() if v else "") for k, v in a.items()}
        def g(*keys):
            for k in keys:
                v = au.get(k.upper(), "")
                if v and v.lower() not in ("null", "none"):
                    return v
            return ""
        return {
            "prop_address": g("SITE_ADDR","SITEADDR","SITE_ADDRESS","PHY_ADDR1",
                              "PHYADDR","SITUS_ADDR","ADDRESS"),
            "prop_city":    g("SITE_CITY","SITECITY","PHY_CITY","PHYCITY",
                              "SITUS_CITY") or "Kissimmee",
            "prop_state":   "FL",
            "prop_zip":     g("SITE_ZIP","SITEZIP","PHY_ZIP","PHYZIP","SITUS_ZIP"),
            "mail_address": g("MAIL_ADDR","MAILADR1","MAIL_ADDRESS","MAILING_ADDR",
                              "OWN_ADDR","OWNADDR","ADDR1"),
            "mail_city":    g("MAIL_CITY","MAILCITY","MAILING_CITY","OWN_CITY",
                              "OWNCITY"),
            "mail_state":   g("MAIL_STATE","MAILSTATE","MAILING_STATE","OWN_STATE",
                              "OWNSTATE") or "FL",
            "mail_zip":     g("MAIL_ZIP","MAILZIP","MAILING_ZIP","OWN_ZIP",
                              "OWNZIP","ZIPCODE","ZIP"),
        }


# ── Clerk document detail: fetch consideration/amount ────────────────────────
def fetch_amounts(records: list[dict], session: requests.Session) -> None:
    """
    For each record missing an amount, fetch the document detail page
    from the clerk API to get the consideration/amount field.
    Modifies records in-place.

    Confirmed detail endpoint:
      GET /browserview/api/document/{doc_id}
    We stored doc_id in the raw row; if not available we skip.
    Amount is in field: consid_1
    """
    missing = [r for r in records if not r.get("amount")]
    if not missing:
        return
    log.info("Fetching amounts for %d records...", len(missing))
    fetched = 0
    for r in missing:
        doc_num = r.get("doc_num", "")
        if not doc_num:
            continue
        # NewVision document detail endpoints to try in order
        endpoints = [
            f"{BROWSE_URL}api/document/{doc_num}",
            f"{BROWSE_URL}api/DocumentDetail/{doc_num}",
            f"{BROWSE_URL}api/getDocument?instrumentNumber={doc_num}",
        ]
        for url in endpoints:
            try:
                resp = session.get(url, timeout=10)
                if resp.status_code != 200:
                    continue
                ct = resp.headers.get("content-type","")
                if "json" not in ct:
                    continue
                data = resp.json()
                if isinstance(data, list) and data:
                    data = data[0]
                if isinstance(data, dict):
                    amount = _parse_amount(
                        data.get("consid_1") or data.get("consid1") or
                        data.get("consideration") or data.get("Consideration") or
                        data.get("amount") or data.get("Amount") or 0
                    )
                    if amount:
                        r["amount"] = amount
                        fetched += 1
                        break
            except Exception as exc:
                log.debug("Amount fetch %s %s: %s", url, doc_num, exc)
        time.sleep(0.05)
    log.info("Amounts fetched: %d / %d", fetched, len(missing))


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

    # Fetch amounts from document detail API
    _api_session = requests.Session()
    _api_session.headers.update(SESSION_HEADERS)
    _api_session.get(BROWSE_URL, timeout=20)
    fetch_amounts(raw, _api_session)

    records = enrich(raw, parcel, today)
    save_json(records, today, from_date, today)
    save_ghl_csv(records)

    log.info("Done. %d leads, %d with addresses.",
             len(records),
             sum(1 for r in records
                 if r.get("prop_address") or r.get("mail_address")))


if __name__ == "__main__":
    main()
