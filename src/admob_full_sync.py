"""
AdMob → BigQuery  (Single Unified Table)

All AdMob data sources write to ONE table: admob_unified_fact

Sources
-------
  admob_network        → Network Report  (no AD_TYPE)
  admob_network_adtype → Network Report  (with AD_TYPE, fewer metrics)
  admob_mediation      → Mediation Report
  admob_campaign       → Campaign Report (v1beta, optional)

Dimensions : 24
Metrics    : 14
Meta cols  : 4   (data_source, sync_timestamp, run_id, row_hash)
─────────────────
TOTAL      : 42 columns in ONE table

Money fields
────────────
  All stored as INT64 MICROS.  Divide by 1,000,000 to get USD.
  e.g.  1,500,000 micros  =  $1.50

Partition  : report_date   (DAY)
Cluster    : data_source, app_id, country_code, ad_format
"""

import os
import sys
import json
import time
import hashlib
import argparse
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import bigquery
from google.oauth2 import service_account


# =============================================================================
# CONFIG
# =============================================================================

PROJECT_ID          = os.environ.get("GCP_PROJECT_ID", "").strip()
DATASET_ID          = os.environ.get("BQ_DATASET_ID", "admob_raw").strip()
BQ_LOCATION         = os.environ.get("BQ_LOCATION", "US").strip()

ADMOB_PUBLISHER_ID  = os.environ.get("ADMOB_PUBLISHER_ID", "").strip()
ADMOB_CURRENCY      = os.environ.get("ADMOB_REPORT_CURRENCY", "USD").strip()
ENABLE_CAMPAIGN     = os.environ.get("ENABLE_ADMOB_BETA_CAMPAIGN", "false").lower() == "true"

CLIENT_ID           = os.environ.get("OAUTH_CLIENT_ID", "").strip()
CLIENT_SECRET       = os.environ.get("OAUTH_CLIENT_SECRET", "").strip()
REFRESH_TOKEN       = os.environ.get("OAUTH_REFRESH_TOKEN", "").strip()
BQ_CREDENTIALS_JSON = os.environ.get("GCP_CREDENTIALS_JSON", "").strip()

# Table names
FACT_TABLE      = "admob_unified_fact"
DIM_ACCOUNT     = "admob_account_dim"
DIM_APPS        = "admob_apps_dim"
DIM_AD_UNITS    = "admob_ad_units_dim"
LOG_TABLE       = "admob_sync_log"

# Retry config
MAX_RETRIES     = 3
RETRY_BACKOFF   = 5   # seconds — doubles each attempt


# =============================================================================
# BIGQUERY SCHEMA
# =============================================================================

# ─── Main unified fact table (42 columns) ────────────────────────────────────
#
#  DIMENSIONS (24)
#  ───────────────
#  Identity / Time    : report_date, data_source                         → 2
#  App                : app_id, app_name, platform, mobile_os_version,
#                       gma_sdk_version, app_version_name               → 6
#  Ad Unit            : ad_unit_id, ad_unit_name, ad_format, ad_type    → 4
#  Geo                : country_code, country_name                      → 2
#  Restriction        : serving_restriction                             → 1
#  Mediation          : ad_source_id, ad_source_name,
#                       ad_source_instance_id, ad_source_instance_name,
#                       mediation_group_id, mediation_group_name        → 6
#  Campaign (v1beta)  : campaign_id, campaign_name, ad_id, ad_name,
#                       placement_id, placement_name, placement_platform → 7 (but placement_platform overlaps → 3 net unique dims)
#  TOTAL DIMENSIONS   : 24
#
#  METRICS (14)
#  ────────────
#  Publisher core     : impressions, clicks, ctr,
#                       estimated_earnings_micros, ecpm_micros          → 5
#  Publisher fill     : ad_requests, matched_requests,
#                       fill_rate, match_rate, show_rate                → 5
#  Publisher mediation: observed_ecpm_micros                            → 1
#  Advertiser         : installs, spend_micros, cpi_micros, interactions → 4 (but spend is campaign)
#  (interactions is campaign only)
#  TOTAL METRICS      : 14
#
#  META (4)
#  ────────
#  sync_timestamp, run_id, row_hash, data_source (already in dims)
#  Net new meta       : sync_timestamp, run_id, row_hash               → 3
#
#  GRAND TOTAL        : 24 dims + 14 metrics + 4 meta = 42 columns

UNIFIED_FACT_SCHEMA = [
    # ── Meta / Identity ──────────────────────────────────────────────────────
    bigquery.SchemaField("report_date",              "DATE",      description="Date of the ad performance data. Partition key."),
    bigquery.SchemaField("data_source",              "STRING",    description="admob_network | admob_network_adtype | admob_mediation | admob_campaign"),
    bigquery.SchemaField("run_id",                   "STRING",    description="Links to admob_sync_log for audit trail."),
    bigquery.SchemaField("sync_timestamp",           "TIMESTAMP", description="UTC timestamp when this row was written to BigQuery."),

    # ── App Dimensions (6) ───────────────────────────────────────────────────
    bigquery.SchemaField("app_id",                   "STRING",    description="AdMob app ID e.g. ca-app-pub-xxx~yyy"),
    bigquery.SchemaField("app_name",                 "STRING",    description="Display name of the app."),
    bigquery.SchemaField("platform",                 "STRING",    description="iOS or Android."),
    bigquery.SchemaField("mobile_os_version",        "STRING",    description="Device OS version e.g. iOS 17.0, Android 14."),
    bigquery.SchemaField("gma_sdk_version",          "STRING",    description="Google Mobile Ads SDK version."),
    bigquery.SchemaField("app_version_name",         "STRING",    description="App version string e.g. 3.2.1."),

    # ── Ad Unit Dimensions (4) ───────────────────────────────────────────────
    bigquery.SchemaField("ad_unit_id",               "STRING",    description="Ad unit ID."),
    bigquery.SchemaField("ad_unit_name",             "STRING",    description="Display name of the ad unit."),
    bigquery.SchemaField("ad_format",                "STRING",    description="Banner | Interstitial | Rewarded | Native | AppOpen | MREC etc."),
    bigquery.SchemaField("ad_type",                  "STRING",    description="TEXT | IMAGE | VIDEO | PLAYABLE | INTERACTIVE. Only in admob_network_adtype rows."),

    # ── Geo Dimensions (2) ──────────────────────────────────────────────────
    bigquery.SchemaField("country_code",             "STRING",    description="ISO 3166-1 alpha-2 e.g. US, PK, IN, AE."),
    bigquery.SchemaField("country_name",             "STRING",    description="Full country name."),

    # ── Restriction (1) ─────────────────────────────────────────────────────
    bigquery.SchemaField("serving_restriction",      "STRING",    description="Restricted | No restriction. Affects revenue significantly."),

    # ── Mediation Dimensions (6) ─────────────────────────────────────────────
    bigquery.SchemaField("ad_source_id",             "STRING",    description="Mediation network ID. Only in admob_mediation rows."),
    bigquery.SchemaField("ad_source_name",           "STRING",    description="e.g. Meta Audience Network, AppLovin, DT Exchange."),
    bigquery.SchemaField("ad_source_instance_id",    "STRING",    description="Specific instance ID of the mediation network."),
    bigquery.SchemaField("ad_source_instance_name",  "STRING",    description="Instance display name."),
    bigquery.SchemaField("mediation_group_id",       "STRING",    description="Mediation group ID."),
    bigquery.SchemaField("mediation_group_name",     "STRING",    description="Mediation group display name."),

    # ── Campaign Dimensions (5) — v1beta only ────────────────────────────────
    bigquery.SchemaField("campaign_id",              "STRING",    description="Campaign ID. Only in admob_campaign rows."),
    bigquery.SchemaField("campaign_name",            "STRING",    description="Campaign display name."),
    bigquery.SchemaField("ad_id",                    "STRING",    description="Ad creative ID."),
    bigquery.SchemaField("ad_name",                  "STRING",    description="Ad creative display name."),
    bigquery.SchemaField("placement_id",             "STRING",    description="Placement ID (campaign report)."),
    bigquery.SchemaField("placement_name",           "STRING",    description="Placement display name."),

    # ── Publisher Metrics — Core (5) ─────────────────────────────────────────
    bigquery.SchemaField("impressions",              "INT64",     description="Ad impressions shown to users."),
    bigquery.SchemaField("clicks",                   "INT64",     description="Total clicks on ads."),
    bigquery.SchemaField("ctr",                      "FLOAT64",   description="Click-through rate = clicks / impressions. Range 0.0–1.0."),
    bigquery.SchemaField("estimated_earnings_micros","INT64",     description="Estimated publisher revenue in micros. Divide by 1,000,000 for USD."),
    bigquery.SchemaField("ecpm_micros",              "FLOAT64",   description="Effective CPM in micros = estimated_earnings / impressions * 1000."),

    # ── Publisher Metrics — Fill (5) ─────────────────────────────────────────
    bigquery.SchemaField("ad_requests",              "INT64",     description="Ad requests sent to AdMob. Not in admob_network_adtype rows."),
    bigquery.SchemaField("matched_requests",         "INT64",     description="Requests that came back with an ad."),
    bigquery.SchemaField("fill_rate",                "FLOAT64",   description="matched_requests / ad_requests. Derived field."),
    bigquery.SchemaField("match_rate",               "FLOAT64",   description="AdMob match rate. Not in admob_network_adtype rows."),
    bigquery.SchemaField("show_rate",                "FLOAT64",   description="% of matched ads actually shown = impressions / matched_requests."),

    # ── Publisher Metrics — Mediation (1) ────────────────────────────────────
    bigquery.SchemaField("observed_ecpm_micros",     "FLOAT64",   description="eCPM reported by the winning mediation network. Only in admob_mediation rows."),

    # ── Advertiser Metrics — Campaign (3) ────────────────────────────────────
    bigquery.SchemaField("installs",                 "INT64",     description="App installs. In network (campaign beta) and mediation reports."),
    bigquery.SchemaField("spend_micros",             "INT64",     description="Campaign spend in micros. Only in admob_campaign rows."),
    bigquery.SchemaField("cpi_micros",               "FLOAT64",   description="Cost per install in micros. Only in admob_campaign rows."),
    bigquery.SchemaField("interactions",             "INT64",     description="Total interactions. Only in admob_campaign rows."),
]

# ─── Account dim ─────────────────────────────────────────────────────────────
ACCOUNT_SCHEMA = [
    bigquery.SchemaField("account_resource_name", "STRING"),
    bigquery.SchemaField("publisher_id",           "STRING"),
    bigquery.SchemaField("reporting_time_zone",    "STRING"),
    bigquery.SchemaField("currency_code",          "STRING"),
    bigquery.SchemaField("sync_timestamp",         "TIMESTAMP"),
]

# ─── Apps dim ────────────────────────────────────────────────────────────────
APPS_SCHEMA = [
    bigquery.SchemaField("app_resource_name",  "STRING"),
    bigquery.SchemaField("app_id",             "STRING"),
    bigquery.SchemaField("platform",           "STRING"),
    bigquery.SchemaField("manual_display_name","STRING"),
    bigquery.SchemaField("store_app_id",       "STRING"),
    bigquery.SchemaField("store_display_name", "STRING"),
    bigquery.SchemaField("app_approval_state", "STRING"),
    bigquery.SchemaField("sync_timestamp",     "TIMESTAMP"),
]

# ─── Ad units dim ────────────────────────────────────────────────────────────
AD_UNITS_SCHEMA = [
    bigquery.SchemaField("ad_unit_resource_name", "STRING"),
    bigquery.SchemaField("ad_unit_id",            "STRING"),
    bigquery.SchemaField("app_id",                "STRING"),
    bigquery.SchemaField("ad_unit_display_name",  "STRING"),
    bigquery.SchemaField("ad_format",             "STRING"),
    bigquery.SchemaField("ad_types",              "STRING", mode="REPEATED"),
    bigquery.SchemaField("sync_timestamp",        "TIMESTAMP"),
]

# ─── Sync log ────────────────────────────────────────────────────────────────
SYNC_LOG_SCHEMA = [
    bigquery.SchemaField("run_id",            "STRING"),
    bigquery.SchemaField("run_type",          "STRING"),
    bigquery.SchemaField("start_date",        "DATE"),
    bigquery.SchemaField("end_date",          "DATE"),
    bigquery.SchemaField("status",            "STRING"),
    bigquery.SchemaField("network_rows",      "INT64"),
    bigquery.SchemaField("network_adtype_rows","INT64"),
    bigquery.SchemaField("mediation_rows",    "INT64"),
    bigquery.SchemaField("campaign_rows",     "INT64"),
    bigquery.SchemaField("total_rows",        "INT64"),
    bigquery.SchemaField("error_message",     "STRING"),
    bigquery.SchemaField("duration_seconds",  "FLOAT64"),
    bigquery.SchemaField("sync_timestamp",    "TIMESTAMP"),
]


# =============================================================================
# VALIDATION
# =============================================================================

def validate_config() -> bool:
    required = {
        "GCP_PROJECT_ID":       PROJECT_ID,
        "BQ_DATASET_ID":        DATASET_ID,
        "OAUTH_CLIENT_ID":      CLIENT_ID,
        "OAUTH_CLIENT_SECRET":  CLIENT_SECRET,
        "OAUTH_REFRESH_TOKEN":  REFRESH_TOKEN,
        "GCP_CREDENTIALS_JSON": BQ_CREDENTIALS_JSON,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"ERROR: Missing environment variables: {', '.join(missing)}")
        return False
    return True


# =============================================================================
# AUTH
# =============================================================================

def get_admob_credentials() -> Credentials:
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=[
            "https://www.googleapis.com/auth/admob.readonly",
            "https://www.googleapis.com/auth/admob.report",
        ],
    )
    creds.refresh(Request())
    return creds


def get_bq_client() -> bigquery.Client:
    info  = json.loads(BQ_CREDENTIALS_JSON)
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    return bigquery.Client(project=PROJECT_ID, credentials=creds, location=BQ_LOCATION)


def get_v1(creds):
    return build("admob", "v1",     credentials=creds, cache_discovery=False)

def get_v1beta(creds):
    return build("admob", "v1beta", credentials=creds, cache_discovery=False)


# =============================================================================
# RETRY
# =============================================================================

def with_retry(fn, label="call"):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except HttpError as e:
            if e.resp.status < 500 and e.resp.status != 429:
                raise
            last_err = e
        except Exception as e:
            last_err = e
        wait = RETRY_BACKOFF * attempt
        print(f"  [{label}] attempt {attempt}/{MAX_RETRIES} failed — retrying in {wait}s …")
        time.sleep(wait)
    raise last_err


# =============================================================================
# HELPERS
# =============================================================================

def utc_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()

def run_id_now() -> str:
    return datetime.utcnow().strftime("%Y%m%d%H%M%S")

def to_api_date(d: date) -> Dict[str, int]:
    return {"year": d.year, "month": d.month, "day": d.day}

def dim_val(dims, key):
    return dims.get(key, {}).get("value")

def dim_lbl(dims, key):
    return dims.get(key, {}).get("displayLabel") or dims.get(key, {}).get("value")

def metric_val(m: Optional[Dict]) -> Optional[Any]:
    if not m:
        return None
    for k in ("microsValue", "integerValue"):
        if k in m and m[k] not in (None, ""):
            return int(m[k])
    for k in ("doubleValue", "decimalValue"):
        if k in m and m[k] not in (None, ""):
            return float(m[k])
    if "value" in m and m["value"] not in (None, ""):
        raw = m["value"]
        try:
            return float(raw) if "." in str(raw) else int(raw)
        except Exception:
            return raw
    return None

def parse_date(dims) -> Optional[str]:
    raw = dim_val(dims, "DATE")
    if not raw or len(raw) != 8:
        return None
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"

def make_hash(*parts) -> str:
    combined = "|".join(str(p or "") for p in parts)
    return hashlib.md5(combined.encode()).hexdigest()

def paginate(callable_, items_key: str) -> List[Dict]:
    results, page_token = [], None
    while True:
        resp = with_retry(
            lambda pt=page_token: callable_(pageToken=pt).execute() if pt else callable_().execute()
        )
        results.extend(resp.get(items_key, []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return results

def safe_fill_rate(matched, requests):
    try:
        if requests and int(requests) > 0:
            return round(int(matched) / int(requests), 6)
    except Exception:
        pass
    return None

def safe_ecpm(earnings_micros, impressions):
    try:
        if impressions and int(impressions) > 0:
            return round(int(earnings_micros) / int(impressions) * 1000, 2)
    except Exception:
        pass
    return None


# =============================================================================
# BIGQUERY OPS
# =============================================================================

def ensure_dataset(bq: bigquery.Client):
    ds_id = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        bq.get_dataset(ds_id)
        print(f"  Dataset exists: {ds_id}")
    except Exception:
        ds = bigquery.Dataset(ds_id)
        ds.location = BQ_LOCATION
        bq.create_dataset(ds)
        print(f"  Created dataset: {ds_id}")


def ensure_table(bq: bigquery.Client, name: str, schema, is_fact=False):
    tid = f"{PROJECT_ID}.{DATASET_ID}.{name}"
    try:
        bq.get_table(tid)
        print(f"  Table exists: {name}")
    except Exception:
        t = bigquery.Table(tid, schema=schema)
        if is_fact:
            t.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="report_date"
            )
            t.clustering_fields = ["data_source", "app_id", "country_code", "ad_format"]
        bq.create_table(t)
        print(f"  Created table: {name}")


def load_rows(bq: bigquery.Client, table: str, schema, rows: List[Dict],
              disposition=bigquery.WriteDisposition.WRITE_APPEND) -> int:
    if not rows:
        print(f"  No rows for {table}")
        return 0
    tid = f"{PROJECT_ID}.{DATASET_ID}.{table}"
    cfg = bigquery.LoadJobConfig(schema=schema, write_disposition=disposition)

    def _load():
        job = bq.load_table_from_json(rows, tid, job_config=cfg)
        job.result()
        return len(rows)

    n = with_retry(_load, label=table)
    print(f"  Loaded {n:,} rows → {table}")
    return n


def delete_range(bq: bigquery.Client, table: str, start: date, end: date):
    tid = f"{PROJECT_ID}.{DATASET_ID}.{table}"
    bq.query(f"DELETE FROM `{tid}` WHERE report_date BETWEEN '{start}' AND '{end}'").result()
    print(f"  Deleted {table}: {start} → {end}")


def write_log(bq: bigquery.Client, run_id: str, run_type: str,
              start: date, end: date, status: str, totals: Dict,
              error: Optional[str], duration: float):
    row = [{
        "run_id":              run_id,
        "run_type":            run_type,
        "start_date":          str(start),
        "end_date":            str(end),
        "status":              status,
        "network_rows":        totals.get("network", 0),
        "network_adtype_rows": totals.get("network_adtype", 0),
        "mediation_rows":      totals.get("mediation", 0),
        "campaign_rows":       totals.get("campaign", 0),
        "total_rows":          sum(totals.values()),
        "error_message":       error,
        "duration_seconds":    round(duration, 2),
        "sync_timestamp":      utc_now(),
    }]
    try:
        load_rows(bq, LOG_TABLE, SYNC_LOG_SCHEMA, row)
    except Exception as e:
        print(f"  WARNING: sync_log write failed: {e}")


# =============================================================================
# DIMENSION TABLE SYNCS
# =============================================================================

def sync_dims(v1, bq: bigquery.Client, account: str):
    ts = utc_now()

    # Account
    acc = with_retry(lambda: v1.accounts().get(name=account).execute())
    account_rows = [{
        "account_resource_name": acc.get("name"),
        "publisher_id":          acc.get("publisherId"),
        "reporting_time_zone":   acc.get("reportingTimeZone"),
        "currency_code":         acc.get("currencyCode"),
        "sync_timestamp":        ts,
    }]
    load_rows(bq, DIM_ACCOUNT, ACCOUNT_SCHEMA, account_rows,
              disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    # Apps
    apps = paginate(
        lambda pageToken=None: v1.accounts().apps().list(parent=account, pageToken=pageToken),
        "apps"
    )
    app_rows = []
    for a in apps:
        mi = a.get("manualAppInfo", {})
        li = a.get("linkedAppInfo", {})
        app_rows.append({
            "app_resource_name":  a.get("name"),
            "app_id":             a.get("appId"),
            "platform":           a.get("platform"),
            "manual_display_name":mi.get("displayName"),
            "store_app_id":       li.get("appStoreId"),
            "store_display_name": li.get("displayName"),
            "app_approval_state": a.get("appApprovalState"),
            "sync_timestamp":     ts,
        })
    load_rows(bq, DIM_APPS, APPS_SCHEMA, app_rows,
              disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    # Ad Units
    units = paginate(
        lambda pageToken=None: v1.accounts().adUnits().list(parent=account, pageToken=pageToken),
        "adUnits"
    )
    unit_rows = [{
        "ad_unit_resource_name": u.get("name"),
        "ad_unit_id":            u.get("adUnitId"),
        "app_id":                u.get("appId"),
        "ad_unit_display_name":  u.get("displayName"),
        "ad_format":             u.get("adFormat"),
        "ad_types":              u.get("adTypes", []),
        "sync_timestamp":        ts,
    } for u in units]
    load_rows(bq, DIM_AD_UNITS, AD_UNITS_SCHEMA, unit_rows,
              disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)


# =============================================================================
# REPORT REQUEST BUILDERS
# =============================================================================

def _base_spec(start: date, end: date) -> Dict:
    return {
        "dateRange": {"startDate": to_api_date(start), "endDate": to_api_date(end)},
        "localizationSettings": {"currencyCode": ADMOB_CURRENCY},
    }

NETWORK_DIMS = [
    "DATE", "APP", "AD_UNIT", "COUNTRY", "FORMAT", "PLATFORM",
    "MOBILE_OS_VERSION", "GMA_SDK_VERSION", "APP_VERSION_NAME", "SERVING_RESTRICTION",
]

def network_request(start: date, end: date) -> Dict:
    spec = _base_spec(start, end)
    spec["dimensions"] = NETWORK_DIMS
    spec["metrics"]    = [
        "AD_REQUESTS", "MATCHED_REQUESTS", "MATCH_RATE",
        "IMPRESSIONS", "CLICKS", "IMPRESSION_CTR",
        "IMPRESSION_RPM", "ESTIMATED_EARNINGS", "SHOW_RATE",
    ]
    return {"reportSpec": spec}

def network_adtype_request(start: date, end: date) -> Dict:
    spec = _base_spec(start, end)
    spec["dimensions"] = NETWORK_DIMS + ["AD_TYPE"]
    spec["metrics"]    = [
        "MATCHED_REQUESTS", "IMPRESSIONS", "CLICKS",
        "IMPRESSION_CTR", "ESTIMATED_EARNINGS", "SHOW_RATE",
    ]
    return {"reportSpec": spec}

def mediation_request(start: date, end: date) -> Dict:
    spec = _base_spec(start, end)
    spec["dimensions"] = [
        "DATE", "APP", "AD_UNIT", "AD_SOURCE", "AD_SOURCE_INSTANCE",
        "MEDIATION_GROUP", "COUNTRY", "FORMAT", "PLATFORM",
        "MOBILE_OS_VERSION", "GMA_SDK_VERSION", "APP_VERSION_NAME", "SERVING_RESTRICTION",
    ]
    spec["metrics"] = [
        "AD_REQUESTS", "MATCHED_REQUESTS", "MATCH_RATE",
        "IMPRESSIONS", "CLICKS", "IMPRESSION_CTR",
        "ESTIMATED_EARNINGS", "OBSERVED_ECPM",
    ]
    return {"reportSpec": spec}

def campaign_request(start: date, end: date) -> Dict:
    spec = _base_spec(start, end)
    spec["dimensions"] = [
        "DATE", "CAMPAIGN_ID", "CAMPAIGN_NAME", "AD_ID", "AD_NAME",
        "PLACEMENT_ID", "PLACEMENT_NAME", "PLACEMENT_PLATFORM", "COUNTRY", "FORMAT",
    ]
    spec["metrics"] = [
        "IMPRESSIONS", "CLICKS", "CLICK_THROUGH_RATE",
        "INSTALLS", "ESTIMATED_COST", "AVERAGE_CPI", "INTERACTIONS",
    ]
    return {"reportSpec": spec}


# =============================================================================
# REPORT FETCHERS
# =============================================================================

def fetch_report(v1, account: str, body: Dict) -> List[Dict]:
    return with_retry(
        lambda: v1.accounts().networkReport().generate(parent=account, body=body).execute()
    )

def fetch_mediation(v1, account: str, body: Dict) -> List[Dict]:
    return with_retry(
        lambda: v1.accounts().mediationReport().generate(parent=account, body=body).execute()
    )

def fetch_campaign(v1beta, account: str, body: Dict) -> List[Dict]:
    return with_retry(
        lambda: v1beta.accounts().campaignReport().generate(parent=account, body=body).execute()
    )


# =============================================================================
# ROW PARSERS  →  unified schema
# =============================================================================

def _base_row(dims, source: str, ts: str, run_id: str) -> Dict:
    return {
        "report_date":         parse_date(dims),
        "data_source":         source,
        "run_id":              run_id,
        "sync_timestamp":      ts,
        "app_id":              dim_val(dims, "APP"),
        "app_name":            dim_lbl(dims, "APP"),
        "platform":            dim_lbl(dims, "PLATFORM"),
        "mobile_os_version":   dim_lbl(dims, "MOBILE_OS_VERSION"),
        "gma_sdk_version":     dim_lbl(dims, "GMA_SDK_VERSION"),
        "app_version_name":    dim_lbl(dims, "APP_VERSION_NAME"),
        "ad_unit_id":          dim_val(dims, "AD_UNIT"),
        "ad_unit_name":        dim_lbl(dims, "AD_UNIT"),
        "ad_format":           dim_lbl(dims, "FORMAT"),
        "country_code":        dim_val(dims, "COUNTRY"),
        "country_name":        dim_lbl(dims, "COUNTRY"),
        "serving_restriction": dim_lbl(dims, "SERVING_RESTRICTION"),
        # mediation dims — NULL for non-mediation rows
        "ad_source_id":              None,
        "ad_source_name":            None,
        "ad_source_instance_id":     None,
        "ad_source_instance_name":   None,
        "mediation_group_id":        None,
        "mediation_group_name":      None,
        # campaign dims — NULL for non-campaign rows
        "ad_type":        None,
        "campaign_id":    None,
        "campaign_name":  None,
        "ad_id":          None,
        "ad_name":        None,
        "placement_id":   None,
        "placement_name": None,
        # all metrics NULL by default
        "impressions":              None,
        "clicks":                   None,
        "ctr":                      None,
        "estimated_earnings_micros":None,
        "ecpm_micros":              None,
        "ad_requests":              None,
        "matched_requests":         None,
        "fill_rate":                None,
        "match_rate":               None,
        "show_rate":                None,
        "observed_ecpm_micros":     None,
        "installs":                 None,
        "spend_micros":             None,
        "cpi_micros":               None,
        "interactions":             None,
    }


def parse_network(report: List[Dict], run_id: str) -> List[Dict]:
    ts   = utc_now()
    rows = []
    for item in report:
        row_data = item.get("row")
        if not row_data:
            continue
        dims = row_data.get("dimensionValues", {})
        mets = row_data.get("metricValues", {})
        row  = _base_row(dims, "admob_network", ts, run_id)
        if not row["report_date"]:
            continue

        impressions   = metric_val(mets.get("IMPRESSIONS"))
        earnings      = metric_val(mets.get("ESTIMATED_EARNINGS"))
        ad_requests   = metric_val(mets.get("AD_REQUESTS"))
        matched       = metric_val(mets.get("MATCHED_REQUESTS"))

        row.update({
            "impressions":               impressions,
            "clicks":                    metric_val(mets.get("CLICKS")),
            "ctr":                       metric_val(mets.get("IMPRESSION_CTR")),
            "estimated_earnings_micros": earnings,
            "ecpm_micros":               safe_ecpm(earnings, impressions),
            "ad_requests":               ad_requests,
            "matched_requests":          matched,
            "fill_rate":                 safe_fill_rate(matched, ad_requests),
            "match_rate":                metric_val(mets.get("MATCH_RATE")),
            "show_rate":                 metric_val(mets.get("SHOW_RATE")),
            # IMPRESSION_RPM is already per-mille so store directly
            "ecpm_micros":               metric_val(mets.get("IMPRESSION_RPM")) or safe_ecpm(earnings, impressions),
        })
        rows.append(row)
    return rows


def parse_network_adtype(report: List[Dict], run_id: str) -> List[Dict]:
    ts   = utc_now()
    rows = []
    for item in report:
        row_data = item.get("row")
        if not row_data:
            continue
        dims = row_data.get("dimensionValues", {})
        mets = row_data.get("metricValues", {})
        row  = _base_row(dims, "admob_network_adtype", ts, run_id)
        if not row["report_date"]:
            continue

        impressions = metric_val(mets.get("IMPRESSIONS"))
        earnings    = metric_val(mets.get("ESTIMATED_EARNINGS"))
        matched     = metric_val(mets.get("MATCHED_REQUESTS"))

        row.update({
            "ad_type":                   dim_lbl(dims, "AD_TYPE"),
            "impressions":               impressions,
            "clicks":                    metric_val(mets.get("CLICKS")),
            "ctr":                       metric_val(mets.get("IMPRESSION_CTR")),
            "estimated_earnings_micros": earnings,
            "ecpm_micros":               safe_ecpm(earnings, impressions),
            "matched_requests":          matched,
            "show_rate":                 metric_val(mets.get("SHOW_RATE")),
            # AD_REQUESTS, MATCH_RATE, IMPRESSION_RPM are NOT available with AD_TYPE
        })
        rows.append(row)
    return rows


def parse_mediation(report: List[Dict], run_id: str) -> List[Dict]:
    ts   = utc_now()
    rows = []
    for item in report:
        row_data = item.get("row")
        if not row_data:
            continue
        dims = row_data.get("dimensionValues", {})
        mets = row_data.get("metricValues", {})
        row  = _base_row(dims, "admob_mediation", ts, run_id)
        if not row["report_date"]:
            continue

        impressions = metric_val(mets.get("IMPRESSIONS"))
        earnings    = metric_val(mets.get("ESTIMATED_EARNINGS"))
        ad_requests = metric_val(mets.get("AD_REQUESTS"))
        matched     = metric_val(mets.get("MATCHED_REQUESTS"))

        row.update({
            "ad_source_id":              dim_val(dims, "AD_SOURCE"),
            "ad_source_name":            dim_lbl(dims, "AD_SOURCE"),
            "ad_source_instance_id":     dim_val(dims, "AD_SOURCE_INSTANCE"),
            "ad_source_instance_name":   dim_lbl(dims, "AD_SOURCE_INSTANCE"),
            "mediation_group_id":        dim_val(dims, "MEDIATION_GROUP"),
            "mediation_group_name":      dim_lbl(dims, "MEDIATION_GROUP"),
            "impressions":               impressions,
            "clicks":                    metric_val(mets.get("CLICKS")),
            "ctr":                       metric_val(mets.get("IMPRESSION_CTR")),
            "estimated_earnings_micros": earnings,
            "ecpm_micros":               safe_ecpm(earnings, impressions),
            "ad_requests":               ad_requests,
            "matched_requests":          matched,
            "fill_rate":                 safe_fill_rate(matched, ad_requests),
            "match_rate":                metric_val(mets.get("MATCH_RATE")),
            "observed_ecpm_micros":      metric_val(mets.get("OBSERVED_ECPM")),
        })
        rows.append(row)
    return rows


def parse_campaign(report: List[Dict], run_id: str) -> List[Dict]:
    ts   = utc_now()
    rows = []
    for item in report:
        row_data = item.get("row")
        if not row_data:
            continue
        dims = row_data.get("dimensionValues", {})
        mets = row_data.get("metricValues", {})

        report_date = parse_date(dims)
        if not report_date:
            continue

        rows.append({
            "report_date":               report_date,
            "data_source":               "admob_campaign",
            "run_id":                    run_id,
            "sync_timestamp":            ts,
            # app dims are NOT in campaign report
            "app_id":                    None,
            "app_name":                  None,
            "platform":                  dim_lbl(dims, "PLACEMENT_PLATFORM"),
            "mobile_os_version":         None,
            "gma_sdk_version":           None,
            "app_version_name":          None,
            "ad_unit_id":                dim_val(dims, "AD_ID"),
            "ad_unit_name":              dim_lbl(dims, "AD_NAME"),
            "ad_format":                 dim_lbl(dims, "FORMAT"),
            "ad_type":                   None,
            "country_code":              dim_val(dims, "COUNTRY"),
            "country_name":              dim_lbl(dims, "COUNTRY"),
            "serving_restriction":       None,
            "ad_source_id":              None,
            "ad_source_name":            None,
            "ad_source_instance_id":     None,
            "ad_source_instance_name":   None,
            "mediation_group_id":        None,
            "mediation_group_name":      None,
            "campaign_id":               dim_val(dims, "CAMPAIGN_ID"),
            "campaign_name":             dim_lbl(dims, "CAMPAIGN_NAME"),
            "ad_id":                     dim_val(dims, "AD_ID"),
            "ad_name":                   dim_lbl(dims, "AD_NAME"),
            "placement_id":              dim_val(dims, "PLACEMENT_ID"),
            "placement_name":            dim_lbl(dims, "PLACEMENT_NAME"),
            "impressions":               metric_val(mets.get("IMPRESSIONS")),
            "clicks":                    metric_val(mets.get("CLICKS")),
            "ctr":                       metric_val(mets.get("CLICK_THROUGH_RATE")),
            "estimated_earnings_micros": None,
            "ecpm_micros":               None,
            "ad_requests":               None,
            "matched_requests":          None,
            "fill_rate":                 None,
            "match_rate":                None,
            "show_rate":                 None,
            "observed_ecpm_micros":      None,
            "installs":                  metric_val(mets.get("INSTALLS")),
            "spend_micros":              metric_val(mets.get("ESTIMATED_COST")),
            "cpi_micros":                metric_val(mets.get("AVERAGE_CPI")),
            "interactions":              metric_val(mets.get("INTERACTIONS")),
        })
    return rows


# =============================================================================
# ACCOUNT NAME
# =============================================================================

def get_account_name(v1) -> str:
    if ADMOB_PUBLISHER_ID:
        raw = ADMOB_PUBLISHER_ID.strip()
        return raw if raw.startswith("accounts/") else f"accounts/{raw}"
    resp = with_retry(lambda: v1.accounts().list().execute())
    accounts = resp.get("account", []) or resp.get("accounts", [])
    if not accounts:
        raise ValueError("No AdMob accounts found.")
    return accounts[0]["name"]


# =============================================================================
# ENSURE ALL TABLES
# =============================================================================

def ensure_all_tables(bq: bigquery.Client, include_campaign: bool):
    print("Ensuring tables exist …")
    ensure_dataset(bq)
    ensure_table(bq, FACT_TABLE,   UNIFIED_FACT_SCHEMA, is_fact=True)
    ensure_table(bq, DIM_ACCOUNT,  ACCOUNT_SCHEMA)
    ensure_table(bq, DIM_APPS,     APPS_SCHEMA)
    ensure_table(bq, DIM_AD_UNITS, AD_UNITS_SCHEMA)
    ensure_table(bq, LOG_TABLE,    SYNC_LOG_SCHEMA)


# =============================================================================
# SYNC ONE RANGE
# =============================================================================

def sync_range(v1, v1beta, bq: bigquery.Client, account: str,
               start: date, end: date, include_campaign: bool,
               run_id: str) -> Dict[str, int]:

    totals = {"network": 0, "network_adtype": 0, "mediation": 0, "campaign": 0}

    # Wipe existing data for the date range first (idempotent)
    delete_range(bq, FACT_TABLE, start, end)

    # 1. Network report
    print("  Fetching network report …")
    net_report = fetch_report(v1, account, network_request(start, end))
    net_rows   = parse_network(net_report, run_id)
    totals["network"] = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, net_rows)

    # 2. Network report — AD_TYPE split
    print("  Fetching network report (ad type split) …")
    nat_report = fetch_report(v1, account, network_adtype_request(start, end))
    nat_rows   = parse_network_adtype(nat_report, run_id)
    totals["network_adtype"] = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, nat_rows)

    # 3. Mediation report
    print("  Fetching mediation report …")
    med_report = fetch_mediation(v1, account, mediation_request(start, end))
    med_rows   = parse_mediation(med_report, run_id)
    totals["mediation"] = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, med_rows)

    # 4. Campaign report (optional, v1beta)
    if include_campaign and v1beta:
        print("  Fetching campaign report (v1beta) …")
        try:
            cam_report = fetch_campaign(v1beta, account, campaign_request(start, end))
            cam_rows   = parse_campaign(cam_report, run_id)
            totals["campaign"] = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, cam_rows)
        except HttpError as e:
            print(f"  WARNING: Campaign report skipped — {e}")

    return totals


# =============================================================================
# MAIN SYNC ENTRY POINTS
# =============================================================================

def sync(days_back: int = 3, include_campaign: bool = False):
    rid        = run_id_now()
    t0         = time.time()
    end_date   = datetime.utcnow().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back - 1)

    print(f"\n=== AdMob Unified Sync | run_id={rid} ===")
    print(f"  Date range : {start_date} → {end_date}")
    print(f"  Days back  : {days_back}")

    creds  = get_admob_credentials()
    v1     = get_v1(creds)
    v1beta = get_v1beta(creds) if include_campaign else None
    bq     = get_bq_client()
    account = get_account_name(v1)
    print(f"  Account    : {account}")

    ensure_all_tables(bq, include_campaign)

    totals = {"network": 0, "network_adtype": 0, "mediation": 0, "campaign": 0}
    error  = None
    status = "SUCCESS"

    try:
        print("\nSyncing dimension tables …")
        sync_dims(v1, bq, account)

        print("\nSyncing fact table …")
        totals = sync_range(v1, v1beta, bq, account, start_date, end_date, include_campaign, rid)

    except Exception as e:
        status = "FAILED"
        error  = str(e)
        raise

    finally:
        write_log(bq, rid, "sync", start_date, end_date, status, totals, error, time.time() - t0)

    print(f"\n=== Sync complete ===")
    print(json.dumps(totals, indent=2))


def backfill(start_str: str, end_str: str, chunk: int = 7, include_campaign: bool = False):
    rid        = run_id_now()
    t0         = time.time()
    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date   = datetime.strptime(end_str,   "%Y-%m-%d").date()

    if include_campaign and chunk > 30:
        print("Campaign report max 30 days per request — setting chunk to 30.")
        chunk = 30

    print(f"\n=== AdMob Unified Backfill | run_id={rid} ===")
    print(f"  Date range : {start_date} → {end_date}")
    print(f"  Chunk size : {chunk} days")

    creds   = get_admob_credentials()
    v1      = get_v1(creds)
    v1beta  = get_v1beta(creds) if include_campaign else None
    bq      = get_bq_client()
    account = get_account_name(v1)
    print(f"  Account    : {account}")

    ensure_all_tables(bq, include_campaign)
    sync_dims(v1, bq, account)

    grand  = {"network": 0, "network_adtype": 0, "mediation": 0, "campaign": 0}
    error  = None
    status = "SUCCESS"
    cur    = start_date

    try:
        while cur <= end_date:
            chunk_end = min(cur + timedelta(days=chunk - 1), end_date)
            print(f"\nChunk: {cur} → {chunk_end}")
            t = sync_range(v1, v1beta, bq, account, cur, chunk_end, include_campaign, rid)
            for k in grand:
                grand[k] += t.get(k, 0)
            cur = chunk_end + timedelta(days=1)

    except Exception as e:
        status = "FAILED"
        error  = str(e)
        raise

    finally:
        write_log(bq, rid, "backfill", start_date, end_date, status, grand, error, time.time() - t0)

    print(f"\n=== Backfill complete ===")
    print(json.dumps(grand, indent=2))


# =============================================================================
# CLI
# =============================================================================

def main():
    p = argparse.ArgumentParser(
        description="AdMob → BigQuery unified sync (single table)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Daily sync (last 3 days):
    python admob_unified_sync.py

  Custom days:
    python admob_unified_sync.py --days 7

  With campaign report:
    python admob_unified_sync.py --enable-campaign

  Backfill:
    python admob_unified_sync.py --backfill-start 2025-01-01 --backfill-end 2025-03-31

  Backfill with custom chunk:
    python admob_unified_sync.py --backfill-start 2025-01-01 --backfill-end 2025-03-31 --chunk 14
        """
    )
    p.add_argument("--days",            type=int, default=3,  help="Days back for daily sync (default 3)")
    p.add_argument("--backfill-start",  type=str,             help="YYYY-MM-DD")
    p.add_argument("--backfill-end",    type=str,             help="YYYY-MM-DD")
    p.add_argument("--chunk",           type=int, default=7,  help="Chunk size for backfill (default 7)")
    p.add_argument("--enable-campaign", action="store_true",  help="Enable v1beta campaign report")
    args = p.parse_args()

    if not validate_config():
        sys.exit(1)

    include_campaign = ENABLE_CAMPAIGN or args.enable_campaign

    try:
        if args.backfill_start and args.backfill_end:
            backfill(args.backfill_start, args.backfill_end, args.chunk, include_campaign)
        else:
            sync(args.days, include_campaign)
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
