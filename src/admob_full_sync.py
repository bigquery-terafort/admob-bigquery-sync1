"""
AdMob → BigQuery  (Single Unified Table) — FINAL VERSION

Key Improvements
────────────────
1. Token auto-refreshes itself — never expires mid-run
2. Per-chunk token refresh — fresh token for every chunk
3. Timeout increased to 180 seconds
4. Mediation 403 handled gracefully
5. Retry backoff improved
6. All money fields correct FLOAT/INT types
7. Default chunk = 1 day — safest for large backfills

All AdMob data sources → ONE table: admob_unified_fact
  admob_network        → Network Report  (no AD_TYPE)
  admob_network_adtype → Network Report  (with AD_TYPE)
  admob_mediation      → Mediation Report
  admob_campaign       → Campaign Report (v1beta, optional)

Money fields: INT64 MICROS — divide by 1,000,000 for USD
Partition  : report_date (DAY)
Cluster    : data_source, app_id, country_code, ad_format
"""

import os
import sys
import json
import time
import argparse
import socket
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import bigquery
from google.oauth2 import service_account

# Global timeout
socket.setdefaulttimeout(180)


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

FACT_TABLE   = "admob_unified_fact"
DIM_ACCOUNT  = "admob_account_dim"
DIM_APPS     = "admob_apps_dim"
DIM_AD_UNITS = "admob_ad_units_dim"
LOG_TABLE    = "admob_sync_log"

MAX_RETRIES   = 4
RETRY_BACKOFF = 8


# =============================================================================
# BIGQUERY SCHEMAS
# =============================================================================

UNIFIED_FACT_SCHEMA = [
    bigquery.SchemaField("report_date",               "DATE"),
    bigquery.SchemaField("data_source",               "STRING"),
    bigquery.SchemaField("run_id",                    "STRING"),
    bigquery.SchemaField("sync_timestamp",            "TIMESTAMP"),
    bigquery.SchemaField("app_id",                    "STRING"),
    bigquery.SchemaField("app_name",                  "STRING"),
    bigquery.SchemaField("platform",                  "STRING"),
    bigquery.SchemaField("mobile_os_version",         "STRING"),
    bigquery.SchemaField("gma_sdk_version",           "STRING"),
    bigquery.SchemaField("app_version_name",          "STRING"),
    bigquery.SchemaField("ad_unit_id",                "STRING"),
    bigquery.SchemaField("ad_unit_name",              "STRING"),
    bigquery.SchemaField("ad_format",                 "STRING"),
    bigquery.SchemaField("ad_type",                   "STRING"),
    bigquery.SchemaField("country_code",              "STRING"),
    bigquery.SchemaField("country_name",              "STRING"),
    bigquery.SchemaField("serving_restriction",       "STRING"),
    bigquery.SchemaField("ad_source_id",              "STRING"),
    bigquery.SchemaField("ad_source_name",            "STRING"),
    bigquery.SchemaField("ad_source_instance_id",     "STRING"),
    bigquery.SchemaField("ad_source_instance_name",   "STRING"),
    bigquery.SchemaField("mediation_group_id",        "STRING"),
    bigquery.SchemaField("mediation_group_name",      "STRING"),
    bigquery.SchemaField("campaign_id",               "STRING"),
    bigquery.SchemaField("campaign_name",             "STRING"),
    bigquery.SchemaField("ad_id",                     "STRING"),
    bigquery.SchemaField("ad_name",                   "STRING"),
    bigquery.SchemaField("placement_id",              "STRING"),
    bigquery.SchemaField("placement_name",            "STRING"),
    bigquery.SchemaField("impressions",               "INT64"),
    bigquery.SchemaField("clicks",                    "INT64"),
    bigquery.SchemaField("ctr",                       "FLOAT64"),
    bigquery.SchemaField("estimated_earnings_micros", "INT64"),
    bigquery.SchemaField("ecpm_micros",               "FLOAT64"),
    bigquery.SchemaField("ad_requests",               "INT64"),
    bigquery.SchemaField("matched_requests",          "INT64"),
    bigquery.SchemaField("fill_rate",                 "FLOAT64"),
    bigquery.SchemaField("match_rate",                "FLOAT64"),
    bigquery.SchemaField("show_rate",                 "FLOAT64"),
    bigquery.SchemaField("observed_ecpm_micros",      "FLOAT64"),
    bigquery.SchemaField("installs",                  "INT64"),
    bigquery.SchemaField("spend_micros",              "INT64"),
    bigquery.SchemaField("cpi_micros",                "FLOAT64"),
    bigquery.SchemaField("interactions",              "INT64"),
]

ACCOUNT_SCHEMA = [
    bigquery.SchemaField("account_resource_name", "STRING"),
    bigquery.SchemaField("publisher_id",          "STRING"),
    bigquery.SchemaField("reporting_time_zone",   "STRING"),
    bigquery.SchemaField("currency_code",         "STRING"),
    bigquery.SchemaField("sync_timestamp",        "TIMESTAMP"),
]

APPS_SCHEMA = [
    bigquery.SchemaField("app_resource_name",   "STRING"),
    bigquery.SchemaField("app_id",              "STRING"),
    bigquery.SchemaField("platform",            "STRING"),
    bigquery.SchemaField("manual_display_name", "STRING"),
    bigquery.SchemaField("store_app_id",        "STRING"),
    bigquery.SchemaField("store_display_name",  "STRING"),
    bigquery.SchemaField("app_approval_state",  "STRING"),
    bigquery.SchemaField("sync_timestamp",      "TIMESTAMP"),
]

AD_UNITS_SCHEMA = [
    bigquery.SchemaField("ad_unit_resource_name", "STRING"),
    bigquery.SchemaField("ad_unit_id",            "STRING"),
    bigquery.SchemaField("app_id",                "STRING"),
    bigquery.SchemaField("ad_unit_display_name",  "STRING"),
    bigquery.SchemaField("ad_format",             "STRING"),
    bigquery.SchemaField("ad_types",              "STRING", mode="REPEATED"),
    bigquery.SchemaField("sync_timestamp",        "TIMESTAMP"),
]

SYNC_LOG_SCHEMA = [
    bigquery.SchemaField("run_id",             "STRING"),
    bigquery.SchemaField("run_type",           "STRING"),
    bigquery.SchemaField("start_date",         "DATE"),
    bigquery.SchemaField("end_date",           "DATE"),
    bigquery.SchemaField("status",             "STRING"),
    bigquery.SchemaField("network_rows",       "INT64"),
    bigquery.SchemaField("network_adtype_rows","INT64"),
    bigquery.SchemaField("mediation_rows",     "INT64"),
    bigquery.SchemaField("campaign_rows",      "INT64"),
    bigquery.SchemaField("total_rows",         "INT64"),
    bigquery.SchemaField("error_message",      "STRING"),
    bigquery.SchemaField("duration_seconds",   "FLOAT64"),
    bigquery.SchemaField("sync_timestamp",     "TIMESTAMP"),
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
        print(f"ERROR: Missing env vars: {', '.join(missing)}")
        return False
    return True


# =============================================================================
# AUTH — Fresh token every time
# =============================================================================

def get_fresh_credentials() -> Credentials:
    """Always returns a fresh valid token. Auto-refreshes every call."""
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
    print(f"  Token refreshed ✅")
    return creds


def get_v1(creds):
    return build("admob", "v1", credentials=creds, cache_discovery=False)

def get_v1beta(creds):
    return build("admob", "v1beta", credentials=creds, cache_discovery=False)

def get_bq_client() -> bigquery.Client:
    info  = json.loads(BQ_CREDENTIALS_JSON)
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    return bigquery.Client(project=PROJECT_ID, credentials=creds, location=BQ_LOCATION)


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


def write_log(bq, run_id, run_type, start, end, status, totals, error, duration):
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
# DIMENSION SYNC
# =============================================================================

def sync_dims(v1, bq: bigquery.Client, account: str):
    ts = utc_now()

    acc = with_retry(lambda: v1.accounts().get(name=account).execute())
    load_rows(bq, DIM_ACCOUNT, ACCOUNT_SCHEMA, [{
        "account_resource_name": acc.get("name"),
        "publisher_id":          acc.get("publisherId"),
        "reporting_time_zone":   acc.get("reportingTimeZone"),
        "currency_code":         acc.get("currencyCode"),
        "sync_timestamp":        ts,
    }], disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    apps = paginate(
        lambda pageToken=None: v1.accounts().apps().list(parent=account, pageToken=pageToken),
        "apps"
    )
    app_rows = []
    for a in apps:
        mi = a.get("manualAppInfo", {})
        li = a.get("linkedAppInfo", {})
        app_rows.append({
            "app_resource_name":   a.get("name"),
            "app_id":              a.get("appId"),
            "platform":            a.get("platform"),
            "manual_display_name": mi.get("displayName"),
            "store_app_id":        li.get("appStoreId"),
            "store_display_name":  li.get("displayName"),
            "app_approval_state":  a.get("appApprovalState"),
            "sync_timestamp":      ts,
        })
    load_rows(bq, DIM_APPS, APPS_SCHEMA, app_rows,
              disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

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
# REPORT BUILDERS
# =============================================================================

def _base_spec(start: date, end: date) -> Dict:
    return {
        "dateRange": {"startDate": to_api_date(start), "endDate": to_api_date(end)},
        "localizationSettings": {"currencyCode": ADMOB_CURRENCY},
    }

def network_request(start, end):
    spec = _base_spec(start, end)
    spec["dimensions"] = ["DATE","APP","AD_UNIT","COUNTRY","FORMAT","PLATFORM",
                          "MOBILE_OS_VERSION","GMA_SDK_VERSION","APP_VERSION_NAME","SERVING_RESTRICTION"]
    spec["metrics"]    = ["AD_REQUESTS","MATCHED_REQUESTS","MATCH_RATE","IMPRESSIONS",
                          "CLICKS","IMPRESSION_CTR","IMPRESSION_RPM","ESTIMATED_EARNINGS","SHOW_RATE"]
    return {"reportSpec": spec}

def network_adtype_request(start, end):
    spec = _base_spec(start, end)
    spec["dimensions"] = ["DATE","APP","AD_UNIT","AD_TYPE","COUNTRY","FORMAT","PLATFORM",
                          "MOBILE_OS_VERSION","GMA_SDK_VERSION","APP_VERSION_NAME","SERVING_RESTRICTION"]
    spec["metrics"]    = ["MATCHED_REQUESTS","IMPRESSIONS","CLICKS",
                          "IMPRESSION_CTR","ESTIMATED_EARNINGS","SHOW_RATE"]
    return {"reportSpec": spec}

def mediation_request(start, end):
    spec = _base_spec(start, end)
    spec["dimensions"] = ["DATE","APP","AD_UNIT","AD_SOURCE","AD_SOURCE_INSTANCE",
                          "MEDIATION_GROUP","COUNTRY","FORMAT","PLATFORM",
                          "MOBILE_OS_VERSION","GMA_SDK_VERSION","APP_VERSION_NAME","SERVING_RESTRICTION"]
    spec["metrics"]    = ["AD_REQUESTS","MATCHED_REQUESTS","MATCH_RATE","IMPRESSIONS",
                          "CLICKS","IMPRESSION_CTR","ESTIMATED_EARNINGS","OBSERVED_ECPM"]
    return {"reportSpec": spec}

def campaign_request(start, end):
    spec = _base_spec(start, end)
    spec["dimensions"] = ["DATE","CAMPAIGN_ID","CAMPAIGN_NAME","AD_ID","AD_NAME",
                          "PLACEMENT_ID","PLACEMENT_NAME","PLACEMENT_PLATFORM","COUNTRY","FORMAT"]
    spec["metrics"]    = ["IMPRESSIONS","CLICKS","CLICK_THROUGH_RATE",
                          "INSTALLS","ESTIMATED_COST","AVERAGE_CPI","INTERACTIONS"]
    return {"reportSpec": spec}


# =============================================================================
# REPORT FETCHERS
# =============================================================================

def fetch_report(v1, account, body):
    return with_retry(
        lambda: v1.accounts().networkReport().generate(parent=account, body=body).execute()
    )

def fetch_mediation(v1, account, body):
    return with_retry(
        lambda: v1.accounts().mediationReport().generate(parent=account, body=body).execute()
    )

def fetch_campaign(v1beta, account, body):
    return with_retry(
        lambda: v1beta.accounts().campaignReport().generate(parent=account, body=body).execute()
    )


# =============================================================================
# ROW PARSERS
# =============================================================================

def _empty_row(source: str, ts: str, run_id: str) -> Dict:
    return {
        "data_source": source, "run_id": run_id, "sync_timestamp": ts,
        "app_id": None, "app_name": None, "platform": None,
        "mobile_os_version": None, "gma_sdk_version": None, "app_version_name": None,
        "ad_unit_id": None, "ad_unit_name": None, "ad_format": None, "ad_type": None,
        "country_code": None, "country_name": None, "serving_restriction": None,
        "ad_source_id": None, "ad_source_name": None,
        "ad_source_instance_id": None, "ad_source_instance_name": None,
        "mediation_group_id": None, "mediation_group_name": None,
        "campaign_id": None, "campaign_name": None,
        "ad_id": None, "ad_name": None, "placement_id": None, "placement_name": None,
        "impressions": None, "clicks": None, "ctr": None,
        "estimated_earnings_micros": None, "ecpm_micros": None,
        "ad_requests": None, "matched_requests": None, "fill_rate": None,
        "match_rate": None, "show_rate": None, "observed_ecpm_micros": None,
        "installs": None, "spend_micros": None, "cpi_micros": None, "interactions": None,
    }

def _base_dims(row, dims):
    row.update({
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
    })


def parse_network(report, run_id):
    ts, rows = utc_now(), []
    for item in report:
        rd = item.get("row")
        if not rd: continue
        dims, mets = rd.get("dimensionValues", {}), rd.get("metricValues", {})
        dt = parse_date(dims)
        if not dt: continue
        row = _empty_row("admob_network", ts, run_id)
        row["report_date"] = dt
        _base_dims(row, dims)
        imp  = metric_val(mets.get("IMPRESSIONS"))
        earn = metric_val(mets.get("ESTIMATED_EARNINGS"))
        req  = metric_val(mets.get("AD_REQUESTS"))
        mat  = metric_val(mets.get("MATCHED_REQUESTS"))
        row.update({
            "impressions":               imp,
            "clicks":                    metric_val(mets.get("CLICKS")),
            "ctr":                       metric_val(mets.get("IMPRESSION_CTR")),
            "estimated_earnings_micros": earn,
            "ecpm_micros":               metric_val(mets.get("IMPRESSION_RPM")) or safe_ecpm(earn, imp),
            "ad_requests":               req,
            "matched_requests":          mat,
            "fill_rate":                 safe_fill_rate(mat, req),
            "match_rate":                metric_val(mets.get("MATCH_RATE")),
            "show_rate":                 metric_val(mets.get("SHOW_RATE")),
        })
        rows.append(row)
    return rows


def parse_network_adtype(report, run_id):
    ts, rows = utc_now(), []
    for item in report:
        rd = item.get("row")
        if not rd: continue
        dims, mets = rd.get("dimensionValues", {}), rd.get("metricValues", {})
        dt = parse_date(dims)
        if not dt: continue
        row = _empty_row("admob_network_adtype", ts, run_id)
        row["report_date"] = dt
        _base_dims(row, dims)
        imp  = metric_val(mets.get("IMPRESSIONS"))
        earn = metric_val(mets.get("ESTIMATED_EARNINGS"))
        mat  = metric_val(mets.get("MATCHED_REQUESTS"))
        row.update({
            "ad_type":                   dim_lbl(dims, "AD_TYPE"),
            "impressions":               imp,
            "clicks":                    metric_val(mets.get("CLICKS")),
            "ctr":                       metric_val(mets.get("IMPRESSION_CTR")),
            "estimated_earnings_micros": earn,
            "ecpm_micros":               safe_ecpm(earn, imp),
            "matched_requests":          mat,
            "show_rate":                 metric_val(mets.get("SHOW_RATE")),
        })
        rows.append(row)
    return rows


def parse_mediation(report, run_id):
    ts, rows = utc_now(), []
    for item in report:
        rd = item.get("row")
        if not rd: continue
        dims, mets = rd.get("dimensionValues", {}), rd.get("metricValues", {})
        dt = parse_date(dims)
        if not dt: continue
        row = _empty_row("admob_mediation", ts, run_id)
        row["report_date"] = dt
        _base_dims(row, dims)
        imp  = metric_val(mets.get("IMPRESSIONS"))
        earn = metric_val(mets.get("ESTIMATED_EARNINGS"))
        req  = metric_val(mets.get("AD_REQUESTS"))
        mat  = metric_val(mets.get("MATCHED_REQUESTS"))
        row.update({
            "ad_source_id":              dim_val(dims, "AD_SOURCE"),
            "ad_source_name":            dim_lbl(dims, "AD_SOURCE"),
            "ad_source_instance_id":     dim_val(dims, "AD_SOURCE_INSTANCE"),
            "ad_source_instance_name":   dim_lbl(dims, "AD_SOURCE_INSTANCE"),
            "mediation_group_id":        dim_val(dims, "MEDIATION_GROUP"),
            "mediation_group_name":      dim_lbl(dims, "MEDIATION_GROUP"),
            "impressions":               imp,
            "clicks":                    metric_val(mets.get("CLICKS")),
            "ctr":                       metric_val(mets.get("IMPRESSION_CTR")),
            "estimated_earnings_micros": earn,
            "ecpm_micros":               safe_ecpm(earn, imp),
            "ad_requests":               req,
            "matched_requests":          mat,
            "fill_rate":                 safe_fill_rate(mat, req),
            "match_rate":                metric_val(mets.get("MATCH_RATE")),
            "observed_ecpm_micros":      metric_val(mets.get("OBSERVED_ECPM")),
        })
        rows.append(row)
    return rows


def parse_campaign(report, run_id):
    ts, rows = utc_now(), []
    for item in report:
        rd = item.get("row")
        if not rd: continue
        dims, mets = rd.get("dimensionValues", {}), rd.get("metricValues", {})
        dt = parse_date(dims)
        if not dt: continue
        row = _empty_row("admob_campaign", ts, run_id)
        row["report_date"] = dt
        row.update({
            "platform":     dim_lbl(dims, "PLACEMENT_PLATFORM"),
            "ad_unit_id":   dim_val(dims, "AD_ID"),
            "ad_unit_name": dim_lbl(dims, "AD_NAME"),
            "ad_format":    dim_lbl(dims, "FORMAT"),
            "country_code": dim_val(dims, "COUNTRY"),
            "country_name": dim_lbl(dims, "COUNTRY"),
            "campaign_id":  dim_val(dims, "CAMPAIGN_ID"),
            "campaign_name":dim_lbl(dims, "CAMPAIGN_NAME"),
            "ad_id":        dim_val(dims, "AD_ID"),
            "ad_name":      dim_lbl(dims, "AD_NAME"),
            "placement_id": dim_val(dims, "PLACEMENT_ID"),
            "placement_name":dim_lbl(dims, "PLACEMENT_NAME"),
            "impressions":  metric_val(mets.get("IMPRESSIONS")),
            "clicks":       metric_val(mets.get("CLICKS")),
            "ctr":          metric_val(mets.get("CLICK_THROUGH_RATE")),
            "installs":     metric_val(mets.get("INSTALLS")),
            "spend_micros": metric_val(mets.get("ESTIMATED_COST")),
            "cpi_micros":   metric_val(mets.get("AVERAGE_CPI")),
            "interactions": metric_val(mets.get("INTERACTIONS")),
        })
        rows.append(row)
    return rows


# =============================================================================
# ACCOUNT
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
# ENSURE TABLES
# =============================================================================

def ensure_all_tables(bq: bigquery.Client):
    print("Ensuring tables …")
    ensure_dataset(bq)
    ensure_table(bq, FACT_TABLE,   UNIFIED_FACT_SCHEMA, is_fact=True)
    ensure_table(bq, DIM_ACCOUNT,  ACCOUNT_SCHEMA)
    ensure_table(bq, DIM_APPS,     APPS_SCHEMA)
    ensure_table(bq, DIM_AD_UNITS, AD_UNITS_SCHEMA)
    ensure_table(bq, LOG_TABLE,    SYNC_LOG_SCHEMA)


# =============================================================================
# SYNC ONE CHUNK — Fresh token every time!
# =============================================================================

def sync_range(account: str, bq: bigquery.Client,
               start: date, end: date,
               include_campaign: bool, run_id: str) -> Dict[str, int]:

    totals = {"network": 0, "network_adtype": 0, "mediation": 0, "campaign": 0}

    # Fresh token for every single chunk
    creds  = get_fresh_credentials()
    v1     = get_v1(creds)
    v1beta = get_v1beta(creds) if include_campaign else None

    delete_range(bq, FACT_TABLE, start, end)

    # 1. Network
    print("  Fetching network report …")
    net_rows = parse_network(
        fetch_report(v1, account, network_request(start, end)), run_id)
    totals["network"] = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, net_rows)

    # 2. Network adtype
    print("  Fetching network adtype report …")
    nat_rows = parse_network_adtype(
        fetch_report(v1, account, network_adtype_request(start, end)), run_id)
    totals["network_adtype"] = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, nat_rows)

    # 3. Mediation
    try:
        print("  Fetching mediation report …")
        med_rows = parse_mediation(
            fetch_mediation(v1, account, mediation_request(start, end)), run_id)
        totals["mediation"] = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, med_rows)
    except HttpError as e:
        if e.resp.status == 403:
            print("  WARNING: Mediation skipped — 403.")
        else:
            raise

    # 4. Campaign (optional)
    if include_campaign and v1beta:
        print("  Fetching campaign report …")
        try:
            cam_rows = parse_campaign(
                fetch_campaign(v1beta, account, campaign_request(start, end)), run_id)
            totals["campaign"] = load_rows(bq, FACT_TABLE, UNIFIED_FACT_SCHEMA, cam_rows)
        except HttpError as e:
            print(f"  WARNING: Campaign skipped — {e}")

    return totals


# =============================================================================
# MAIN SYNC
# =============================================================================

def sync(days_back: int = 3, include_campaign: bool = False):
    rid        = run_id_now()
    t0         = time.time()
    end_date   = datetime.utcnow().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back - 1)

    print(f"\n=== AdMob Unified Sync | run_id={rid} ===")
    print(f"  Date range : {start_date} → {end_date}")

    creds   = get_fresh_credentials()
    v1      = get_v1(creds)
    bq      = get_bq_client()
    account = get_account_name(v1)
    print(f"  Account    : {account}")

    ensure_all_tables(bq)

    totals = {"network": 0, "network_adtype": 0, "mediation": 0, "campaign": 0}
    error, status = None, "SUCCESS"

    try:
        print("\nSyncing dimensions …")
        sync_dims(v1, bq, account)

        print("\nSyncing facts …")
        totals = sync_range(account, bq, start_date, end_date, include_campaign, rid)

    except Exception as e:
        status, error = "FAILED", str(e)
        raise
    finally:
        write_log(bq, rid, "sync", start_date, end_date, status, totals, error, time.time()-t0)

    print(f"\n=== Sync complete ===")
    print(json.dumps(totals, indent=2))


# =============================================================================
# BACKFILL — Fresh token per chunk!
# =============================================================================

def backfill(start_str: str, end_str: str, chunk: int = 1, include_campaign: bool = False):
    rid        = run_id_now()
    t0         = time.time()
    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date   = datetime.strptime(end_str,   "%Y-%m-%d").date()

    if include_campaign and chunk > 30:
        chunk = 30

    print(f"\n=== AdMob Backfill | run_id={rid} ===")
    print(f"  Date range : {start_date} → {end_date}")
    print(f"  Chunk size : {chunk} day(s)")

    creds   = get_fresh_credentials()
    v1      = get_v1(creds)
    bq      = get_bq_client()
    account = get_account_name(v1)
    print(f"  Account    : {account}")

    ensure_all_tables(bq)
    sync_dims(v1, bq, account)

    grand  = {"network": 0, "network_adtype": 0, "mediation": 0, "campaign": 0}
    error, status = None, "SUCCESS"
    cur    = start_date

    try:
        while cur <= end_date:
            chunk_end = min(cur + timedelta(days=chunk - 1), end_date)
            print(f"\nChunk: {cur} → {chunk_end}")

            # Fresh token for EVERY chunk — this is the key fix!
            t = sync_range(account, bq, cur, chunk_end, include_campaign, rid)

            for k in grand:
                grand[k] += t.get(k, 0)

            cur = chunk_end + timedelta(days=1)
            time.sleep(2)  # Small pause between chunks

    except Exception as e:
        status, error = "FAILED", str(e)
        raise
    finally:
        write_log(bq, rid, "backfill", start_date, end_date, status, grand, error, time.time()-t0)

    print(f"\n=== Backfill complete ===")
    print(json.dumps(grand, indent=2))


# =============================================================================
# CLI
# =============================================================================

def main():
    p = argparse.ArgumentParser(description="AdMob → BigQuery unified sync")
    p.add_argument("--days",             type=int, default=3)
    p.add_argument("--backfill-start",   type=str)
    p.add_argument("--backfill-end",     type=str)
    p.add_argument("--chunk",            type=int, default=1)
    p.add_argument("--chunk-days",       type=int, default=1)
    p.add_argument("--enable-campaign",  action="store_true")
    p.add_argument("--enable-campaign-beta", action="store_true")
    args = p.parse_args()

    if not validate_config():
        sys.exit(1)

    include_campaign = ENABLE_CAMPAIGN or args.enable_campaign or args.enable_campaign_beta
    chunk = args.chunk or args.chunk_days or 1

    try:
        if args.backfill_start and args.backfill_end:
            backfill(args.backfill_start, args.backfill_end, chunk, include_campaign)
        else:
            sync(args.days, include_campaign)
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
