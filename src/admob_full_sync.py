"""
AdMob Full Sync -> BigQuery

What this syncs
---------------
Stable v1:
- Account metadata
- Apps inventory
- Ad units inventory
- Network report (split into two safe report calls)
- Mediation report

Optional v1beta:
- Campaign report

Notes
-----
1) Network report is split because AD_TYPE is incompatible with:
   - AD_REQUESTS
   - MATCH_RATE
   - IMPRESSION_RPM

2) This script syncs DATE-grain facts by default.
   WEEK / MONTH can be derived in BigQuery from report_date.

3) Campaign report is optional and beta.
"""

import os
import sys
import json
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

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "admob_raw")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

ADMOB_PUBLISHER_ID = os.environ.get("ADMOB_PUBLISHER_ID", "").strip()
ADMOB_REPORT_CURRENCY = os.environ.get("ADMOB_REPORT_CURRENCY", "USD")
ENABLE_ADMOB_BETA_CAMPAIGN = os.environ.get("ENABLE_ADMOB_BETA_CAMPAIGN", "false").lower() == "true"

CLIENT_ID = os.environ.get("OAUTH_CLIENT_ID")
CLIENT_SECRET = os.environ.get("OAUTH_CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("OAUTH_REFRESH_TOKEN")
BQ_CREDENTIALS_JSON = os.environ.get("GCP_CREDENTIALS_JSON")

# Fact tables
TABLE_NETWORK = "admob_network_fact"
TABLE_NETWORK_AD_TYPE = "admob_network_ad_type_fact"
TABLE_MEDIATION = "admob_mediation_fact"
TABLE_CAMPAIGN = "admob_campaign_fact"

# Dimension tables
TABLE_ACCOUNT = "admob_account_dim"
TABLE_APPS = "admob_apps_dim"
TABLE_AD_UNITS = "admob_ad_units_dim"


# =============================================================================
# BIGQUERY SCHEMAS
# =============================================================================

ACCOUNT_SCHEMA = [
    bigquery.SchemaField("account_resource_name", "STRING"),
    bigquery.SchemaField("publisher_id", "STRING"),
    bigquery.SchemaField("reporting_time_zone", "STRING"),
    bigquery.SchemaField("currency_code", "STRING"),
    bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
]

APPS_SCHEMA = [
    bigquery.SchemaField("app_resource_name", "STRING"),
    bigquery.SchemaField("app_id", "STRING"),
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("manual_display_name", "STRING"),
    bigquery.SchemaField("store_app_id", "STRING"),
    bigquery.SchemaField("store_display_name", "STRING"),
    bigquery.SchemaField("app_approval_state", "STRING"),
    bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
]

AD_UNITS_SCHEMA = [
    bigquery.SchemaField("ad_unit_resource_name", "STRING"),
    bigquery.SchemaField("ad_unit_id", "STRING"),
    bigquery.SchemaField("app_id", "STRING"),
    bigquery.SchemaField("ad_unit_display_name", "STRING"),
    bigquery.SchemaField("ad_format", "STRING"),
    bigquery.SchemaField("ad_types", "STRING", mode="REPEATED"),
    bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
]

NETWORK_SCHEMA = [
    bigquery.SchemaField("report_date", "DATE"),
    bigquery.SchemaField("app_id", "STRING"),
    bigquery.SchemaField("app_name", "STRING"),
    bigquery.SchemaField("ad_unit_id", "STRING"),
    bigquery.SchemaField("ad_unit_name", "STRING"),
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("country_name", "STRING"),
    bigquery.SchemaField("ad_format", "STRING"),
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("mobile_os_version", "STRING"),
    bigquery.SchemaField("gma_sdk_version", "STRING"),
    bigquery.SchemaField("app_version_name", "STRING"),
    bigquery.SchemaField("serving_restriction", "STRING"),
    bigquery.SchemaField("ad_requests", "INTEGER"),
    bigquery.SchemaField("clicks", "INTEGER"),
    bigquery.SchemaField("estimated_earnings_micros", "INTEGER"),
    bigquery.SchemaField("impressions", "INTEGER"),
    bigquery.SchemaField("impression_ctr", "FLOAT"),
    bigquery.SchemaField("impression_rpm_micros", "INTEGER"),
    bigquery.SchemaField("matched_requests", "INTEGER"),
    bigquery.SchemaField("match_rate", "FLOAT"),
    bigquery.SchemaField("show_rate", "FLOAT"),
    bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
]

NETWORK_AD_TYPE_SCHEMA = [
    bigquery.SchemaField("report_date", "DATE"),
    bigquery.SchemaField("app_id", "STRING"),
    bigquery.SchemaField("app_name", "STRING"),
    bigquery.SchemaField("ad_unit_id", "STRING"),
    bigquery.SchemaField("ad_unit_name", "STRING"),
    bigquery.SchemaField("ad_type", "STRING"),
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("country_name", "STRING"),
    bigquery.SchemaField("ad_format", "STRING"),
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("mobile_os_version", "STRING"),
    bigquery.SchemaField("gma_sdk_version", "STRING"),
    bigquery.SchemaField("app_version_name", "STRING"),
    bigquery.SchemaField("serving_restriction", "STRING"),
    bigquery.SchemaField("clicks", "INTEGER"),
    bigquery.SchemaField("estimated_earnings_micros", "INTEGER"),
    bigquery.SchemaField("impressions", "INTEGER"),
    bigquery.SchemaField("impression_ctr", "FLOAT"),
    bigquery.SchemaField("matched_requests", "INTEGER"),
    bigquery.SchemaField("show_rate", "FLOAT"),
    bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
]

MEDIATION_SCHEMA = [
    bigquery.SchemaField("report_date", "DATE"),
    bigquery.SchemaField("app_id", "STRING"),
    bigquery.SchemaField("app_name", "STRING"),
    bigquery.SchemaField("ad_unit_id", "STRING"),
    bigquery.SchemaField("ad_unit_name", "STRING"),
    bigquery.SchemaField("ad_source_id", "STRING"),
    bigquery.SchemaField("ad_source_name", "STRING"),
    bigquery.SchemaField("ad_source_instance_id", "STRING"),
    bigquery.SchemaField("ad_source_instance_name", "STRING"),
    bigquery.SchemaField("mediation_group_id", "STRING"),
    bigquery.SchemaField("mediation_group_name", "STRING"),
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("country_name", "STRING"),
    bigquery.SchemaField("ad_format", "STRING"),
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("mobile_os_version", "STRING"),
    bigquery.SchemaField("gma_sdk_version", "STRING"),
    bigquery.SchemaField("app_version_name", "STRING"),
    bigquery.SchemaField("serving_restriction", "STRING"),
    bigquery.SchemaField("ad_requests", "INTEGER"),
    bigquery.SchemaField("clicks", "INTEGER"),
    bigquery.SchemaField("estimated_earnings_micros", "INTEGER"),
    bigquery.SchemaField("impressions", "INTEGER"),
    bigquery.SchemaField("impression_ctr", "FLOAT"),
    bigquery.SchemaField("matched_requests", "INTEGER"),
    bigquery.SchemaField("match_rate", "FLOAT"),
    bigquery.SchemaField("observed_ecpm_micros", "INTEGER"),
    bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
]

CAMPAIGN_SCHEMA = [
    bigquery.SchemaField("report_date", "DATE"),
    bigquery.SchemaField("campaign_id", "STRING"),
    bigquery.SchemaField("campaign_name", "STRING"),
    bigquery.SchemaField("ad_id", "STRING"),
    bigquery.SchemaField("ad_name", "STRING"),
    bigquery.SchemaField("placement_id", "STRING"),
    bigquery.SchemaField("placement_name", "STRING"),
    bigquery.SchemaField("placement_platform", "STRING"),
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("country_name", "STRING"),
    bigquery.SchemaField("ad_format", "STRING"),
    bigquery.SchemaField("impressions", "INTEGER"),
    bigquery.SchemaField("clicks", "INTEGER"),
    bigquery.SchemaField("click_through_rate", "FLOAT"),
    bigquery.SchemaField("installs", "INTEGER"),
    bigquery.SchemaField("estimated_cost_micros", "INTEGER"),
    bigquery.SchemaField("average_cpi_micros", "INTEGER"),
    bigquery.SchemaField("interactions", "INTEGER"),
    bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
]


# =============================================================================
# VALIDATION
# =============================================================================

def validate_config() -> bool:
    required = {
        "GCP_PROJECT_ID": PROJECT_ID,
        "BQ_DATASET_ID": DATASET_ID,
        "OAUTH_CLIENT_ID": CLIENT_ID,
        "OAUTH_CLIENT_SECRET": CLIENT_SECRET,
        "OAUTH_REFRESH_TOKEN": REFRESH_TOKEN,
        "GCP_CREDENTIALS_JSON": BQ_CREDENTIALS_JSON,
    }

    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        return False

    return True


# =============================================================================
# AUTH
# =============================================================================

def get_admob_credentials() -> Credentials:
    credentials = Credentials(
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
    credentials.refresh(Request())
    return credentials


def get_bigquery_client() -> bigquery.Client:
    credentials_info = json.loads(BQ_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials, location=BQ_LOCATION)


def get_admob_service_v1(credentials: Credentials):
    return build("admob", "v1", credentials=credentials, cache_discovery=False)


def get_admob_service_v1beta(credentials: Credentials):
    return build("admob", "v1beta", credentials=credentials, cache_discovery=False)


# =============================================================================
# HELPERS
# =============================================================================

def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()


def normalize_account_name(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("accounts/"):
        return raw
    return f"accounts/{raw}"


def discover_account_name(service_v1) -> str:
    response = service_v1.accounts().list().execute()
    accounts = response.get("account", []) or response.get("accounts", [])
    if not accounts:
        raise ValueError("No AdMob accounts found for the provided OAuth credentials.")
    return accounts[0]["name"]


def get_account_name(service_v1) -> str:
    if ADMOB_PUBLISHER_ID:
        return normalize_account_name(ADMOB_PUBLISHER_ID)
    return discover_account_name(service_v1)


def to_api_date(d: date) -> Dict[str, int]:
    return {
        "year": d.year,
        "month": d.month,
        "day": d.day,
    }


def parse_metric_value(metric_dict: Optional[Dict[str, Any]]) -> Optional[Any]:
    if not metric_dict:
        return None

    if "microsValue" in metric_dict and metric_dict["microsValue"] not in (None, ""):
        return int(metric_dict["microsValue"])

    if "integerValue" in metric_dict and metric_dict["integerValue"] not in (None, ""):
        return int(metric_dict["integerValue"])

    if "doubleValue" in metric_dict and metric_dict["doubleValue"] not in (None, ""):
        return float(metric_dict["doubleValue"])

    if "decimalValue" in metric_dict and metric_dict["decimalValue"] not in (None, ""):
        return float(metric_dict["decimalValue"])

    if "value" in metric_dict and metric_dict["value"] not in (None, ""):
        raw = metric_dict["value"]
        try:
            if "." in str(raw):
                return float(raw)
            return int(raw)
        except Exception:
            return raw

    return None


def dim_value(dims: Dict[str, Any], key: str) -> Optional[str]:
    return dims.get(key, {}).get("value")


def dim_label(dims: Dict[str, Any], key: str) -> Optional[str]:
    return dims.get(key, {}).get("displayLabel") or dims.get(key, {}).get("value")


def parse_report_date(dims: Dict[str, Any]) -> Optional[str]:
    raw = dim_value(dims, "DATE")
    if not raw or len(raw) != 8:
        return None
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"


def paginate_list(method_callable, items_key: str) -> List[Dict[str, Any]]:
    results = []
    page_token = None

    while True:
        if page_token:
            response = method_callable(pageToken=page_token).execute()
        else:
            response = method_callable().execute()

        items = response.get(items_key, [])
        results.extend(items)

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return results


# =============================================================================
# BIGQUERY OPERATIONS
# =============================================================================

def ensure_dataset_exists(client: bigquery.Client) -> None:
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset exists: {dataset_id}")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = BQ_LOCATION
        client.create_dataset(dataset)
        print(f"Created dataset: {dataset_id}")


def ensure_table_exists(client: bigquery.Client, table_name: str, schema: List[bigquery.SchemaField]) -> None:
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    try:
        client.get_table(table_id)
        print(f"Table exists: {table_name}")
    except Exception:
        table = bigquery.Table(table_id, schema=schema)

        if table_name in {TABLE_NETWORK, TABLE_NETWORK_AD_TYPE, TABLE_MEDIATION, TABLE_CAMPAIGN}:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="report_date"
            )

        client.create_table(table)
        print(f"Created table: {table_name}")


def load_json_rows(
    client: bigquery.Client,
    table_name: str,
    schema: List[bigquery.SchemaField],
    rows: List[Dict[str, Any]],
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
) -> int:
    if not rows:
        print(f"No rows to load for {table_name}")
        return 0

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition,
    )
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(rows)} rows into {table_name}")
    return len(rows)


def delete_date_range(client: bigquery.Client, table_name: str, start_date: date, end_date: date) -> None:
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    query = f"""
        DELETE FROM `{table_id}`
        WHERE report_date BETWEEN '{start_date}' AND '{end_date}'
    """
    client.query(query).result()
    print(f"Deleted existing rows from {table_name}: {start_date} to {end_date}")


# =============================================================================
# METADATA SYNC
# =============================================================================

def fetch_account_dim(service_v1, account_name: str) -> List[Dict[str, Any]]:
    account = service_v1.accounts().get(name=account_name).execute()
    sync_time = utc_now_iso()

    return [{
        "account_resource_name": account.get("name"),
        "publisher_id": account.get("publisherId"),
        "reporting_time_zone": account.get("reportingTimeZone"),
        "currency_code": account.get("currencyCode"),
        "sync_timestamp": sync_time,
    }]


def fetch_apps_dim(service_v1, account_name: str) -> List[Dict[str, Any]]:
    response = service_v1.accounts().apps()
    items = paginate_list(
        method_callable=lambda pageToken=None: response.list(parent=account_name, pageToken=pageToken),
        items_key="apps"
    )

    sync_time = utc_now_iso()
    rows = []

    for app in items:
        manual_info = app.get("manualAppInfo", {})
        linked_info = app.get("linkedAppInfo", {})

        rows.append({
            "app_resource_name": app.get("name"),
            "app_id": app.get("appId"),
            "platform": app.get("platform"),
            "manual_display_name": manual_info.get("displayName"),
            "store_app_id": linked_info.get("appStoreId"),
            "store_display_name": linked_info.get("displayName"),
            "app_approval_state": app.get("appApprovalState"),
            "sync_timestamp": sync_time,
        })

    return rows


def fetch_ad_units_dim(service_v1, account_name: str) -> List[Dict[str, Any]]:
    response = service_v1.accounts().adUnits()
    items = paginate_list(
        method_callable=lambda pageToken=None: response.list(parent=account_name, pageToken=pageToken),
        items_key="adUnits"
    )

    sync_time = utc_now_iso()
    rows = []

    for unit in items:
        rows.append({
            "ad_unit_resource_name": unit.get("name"),
            "ad_unit_id": unit.get("adUnitId"),
            "app_id": unit.get("appId"),
            "ad_unit_display_name": unit.get("displayName"),
            "ad_format": unit.get("adFormat"),
            "ad_types": unit.get("adTypes", []),
            "sync_timestamp": sync_time,
        })

    return rows


# =============================================================================
# REPORT REQUEST BUILDERS
# =============================================================================

def build_network_request(
    start_date: date,
    end_date: date,
    include_ad_type: bool,
) -> Dict[str, Any]:
    if include_ad_type:
        dimensions = [
            "DATE",
            "APP",
            "AD_UNIT",
            "AD_TYPE",
            "COUNTRY",
            "FORMAT",
            "PLATFORM",
            "MOBILE_OS_VERSION",
            "GMA_SDK_VERSION",
            "APP_VERSION_NAME",
            "SERVING_RESTRICTION",
        ]
        metrics = [
            "CLICKS",
            "ESTIMATED_EARNINGS",
            "IMPRESSIONS",
            "IMPRESSION_CTR",
            "MATCHED_REQUESTS",
            "SHOW_RATE",
        ]
    else:
        dimensions = [
            "DATE",
            "APP",
            "AD_UNIT",
            "COUNTRY",
            "FORMAT",
            "PLATFORM",
            "MOBILE_OS_VERSION",
            "GMA_SDK_VERSION",
            "APP_VERSION_NAME",
            "SERVING_RESTRICTION",
        ]
        metrics = [
            "AD_REQUESTS",
            "CLICKS",
            "ESTIMATED_EARNINGS",
            "IMPRESSIONS",
            "IMPRESSION_CTR",
            "IMPRESSION_RPM",
            "MATCHED_REQUESTS",
            "MATCH_RATE",
            "SHOW_RATE",
        ]

    return {
        "reportSpec": {
            "dateRange": {
                "startDate": to_api_date(start_date),
                "endDate": to_api_date(end_date),
            },
            "dimensions": dimensions,
            "metrics": metrics,
            "localizationSettings": {
                "currencyCode": ADMOB_REPORT_CURRENCY,
            },
        }
    }


def build_mediation_request(start_date: date, end_date: date) -> Dict[str, Any]:
    return {
        "reportSpec": {
            "dateRange": {
                "startDate": to_api_date(start_date),
                "endDate": to_api_date(end_date),
            },
            "dimensions": [
                "DATE",
                "APP",
                "AD_UNIT",
                "AD_SOURCE",
                "AD_SOURCE_INSTANCE",
                "MEDIATION_GROUP",
                "COUNTRY",
                "FORMAT",
                "PLATFORM",
                "MOBILE_OS_VERSION",
                "GMA_SDK_VERSION",
                "APP_VERSION_NAME",
                "SERVING_RESTRICTION",
            ],
            "metrics": [
                "AD_REQUESTS",
                "CLICKS",
                "ESTIMATED_EARNINGS",
                "IMPRESSIONS",
                "IMPRESSION_CTR",
                "MATCHED_REQUESTS",
                "MATCH_RATE",
                "OBSERVED_ECPM",
            ],
            "localizationSettings": {
                "currencyCode": ADMOB_REPORT_CURRENCY,
            },
        }
    }


def build_campaign_request(start_date: date, end_date: date) -> Dict[str, Any]:
    return {
        "reportSpec": {
            "dateRange": {
                "startDate": to_api_date(start_date),
                "endDate": to_api_date(end_date),
            },
            "dimensions": [
                "DATE",
                "CAMPAIGN_ID",
                "CAMPAIGN_NAME",
                "AD_ID",
                "AD_NAME",
                "PLACEMENT_ID",
                "PLACEMENT_NAME",
                "PLACEMENT_PLATFORM",
                "COUNTRY",
                "FORMAT",
            ],
            "metrics": [
                "IMPRESSIONS",
                "CLICKS",
                "CLICK_THROUGH_RATE",
                "INSTALLS",
                "ESTIMATED_COST",
                "AVERAGE_CPI",
                "INTERACTIONS",
            ],
            "localizationSettings": {
                "currencyCode": ADMOB_REPORT_CURRENCY,
            },
        }
    }


# =============================================================================
# REPORT FETCHERS
# =============================================================================

def fetch_network_report(service_v1, account_name: str, start_date: date, end_date: date, include_ad_type: bool):
    body = build_network_request(start_date, end_date, include_ad_type)
    return service_v1.accounts().networkReport().generate(parent=account_name, body=body).execute()


def fetch_mediation_report(service_v1, account_name: str, start_date: date, end_date: date):
    body = build_mediation_request(start_date, end_date)
    return service_v1.accounts().mediationReport().generate(parent=account_name, body=body).execute()


def fetch_campaign_report(service_v1beta, account_name: str, start_date: date, end_date: date):
    body = build_campaign_request(start_date, end_date)
    return service_v1beta.accounts().campaignReport().generate(parent=account_name, body=body).execute()


# =============================================================================
# REPORT PARSERS
# =============================================================================

def parse_network_rows(report: List[Dict[str, Any]], include_ad_type: bool) -> List[Dict[str, Any]]:
    rows = []
    sync_time = utc_now_iso()

    for item in report:
        row = item.get("row")
        if not row:
            continue

        dims = row.get("dimensionValues", {})
        mets = row.get("metricValues", {})
        report_date = parse_report_date(dims)
        if not report_date:
            continue

        base = {
            "report_date": report_date,
            "app_id": dim_value(dims, "APP"),
            "app_name": dim_label(dims, "APP"),
            "ad_unit_id": dim_value(dims, "AD_UNIT"),
            "ad_unit_name": dim_label(dims, "AD_UNIT"),
            "country_code": dim_value(dims, "COUNTRY"),
            "country_name": dim_label(dims, "COUNTRY"),
            "ad_format": dim_label(dims, "FORMAT"),
            "platform": dim_label(dims, "PLATFORM"),
            "mobile_os_version": dim_label(dims, "MOBILE_OS_VERSION"),
            "gma_sdk_version": dim_label(dims, "GMA_SDK_VERSION"),
            "app_version_name": dim_label(dims, "APP_VERSION_NAME"),
            "serving_restriction": dim_label(dims, "SERVING_RESTRICTION"),
            "sync_timestamp": sync_time,
        }

        if include_ad_type:
            base["ad_type"] = dim_label(dims, "AD_TYPE")
            base["clicks"] = parse_metric_value(mets.get("CLICKS")) or 0
            base["estimated_earnings_micros"] = parse_metric_value(mets.get("ESTIMATED_EARNINGS")) or 0
            base["impressions"] = parse_metric_value(mets.get("IMPRESSIONS")) or 0
            base["impression_ctr"] = parse_metric_value(mets.get("IMPRESSION_CTR"))
            base["matched_requests"] = parse_metric_value(mets.get("MATCHED_REQUESTS")) or 0
            base["show_rate"] = parse_metric_value(mets.get("SHOW_RATE"))
        else:
            base["ad_requests"] = parse_metric_value(mets.get("AD_REQUESTS")) or 0
            base["clicks"] = parse_metric_value(mets.get("CLICKS")) or 0
            base["estimated_earnings_micros"] = parse_metric_value(mets.get("ESTIMATED_EARNINGS")) or 0
            base["impressions"] = parse_metric_value(mets.get("IMPRESSIONS")) or 0
            base["impression_ctr"] = parse_metric_value(mets.get("IMPRESSION_CTR"))
            base["impression_rpm_micros"] = parse_metric_value(mets.get("IMPRESSION_RPM"))
            base["matched_requests"] = parse_metric_value(mets.get("MATCHED_REQUESTS")) or 0
            base["match_rate"] = parse_metric_value(mets.get("MATCH_RATE"))
            base["show_rate"] = parse_metric_value(mets.get("SHOW_RATE"))

        rows.append(base)

    return rows


def parse_mediation_rows(report: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    sync_time = utc_now_iso()

    for item in report:
        row = item.get("row")
        if not row:
            continue

        dims = row.get("dimensionValues", {})
        mets = row.get("metricValues", {})
        report_date = parse_report_date(dims)
        if not report_date:
            continue

        rows.append({
            "report_date": report_date,
            "app_id": dim_value(dims, "APP"),
            "app_name": dim_label(dims, "APP"),
            "ad_unit_id": dim_value(dims, "AD_UNIT"),
            "ad_unit_name": dim_label(dims, "AD_UNIT"),
            "ad_source_id": dim_value(dims, "AD_SOURCE"),
            "ad_source_name": dim_label(dims, "AD_SOURCE"),
            "ad_source_instance_id": dim_value(dims, "AD_SOURCE_INSTANCE"),
            "ad_source_instance_name": dim_label(dims, "AD_SOURCE_INSTANCE"),
            "mediation_group_id": dim_value(dims, "MEDIATION_GROUP"),
            "mediation_group_name": dim_label(dims, "MEDIATION_GROUP"),
            "country_code": dim_value(dims, "COUNTRY"),
            "country_name": dim_label(dims, "COUNTRY"),
            "ad_format": dim_label(dims, "FORMAT"),
            "platform": dim_label(dims, "PLATFORM"),
            "mobile_os_version": dim_label(dims, "MOBILE_OS_VERSION"),
            "gma_sdk_version": dim_label(dims, "GMA_SDK_VERSION"),
            "app_version_name": dim_label(dims, "APP_VERSION_NAME"),
            "serving_restriction": dim_label(dims, "SERVING_RESTRICTION"),
            "ad_requests": parse_metric_value(mets.get("AD_REQUESTS")) or 0,
            "clicks": parse_metric_value(mets.get("CLICKS")) or 0,
            "estimated_earnings_micros": parse_metric_value(mets.get("ESTIMATED_EARNINGS")) or 0,
            "impressions": parse_metric_value(mets.get("IMPRESSIONS")) or 0,
            "impression_ctr": parse_metric_value(mets.get("IMPRESSION_CTR")),
            "matched_requests": parse_metric_value(mets.get("MATCHED_REQUESTS")) or 0,
            "match_rate": parse_metric_value(mets.get("MATCH_RATE")),
            "observed_ecpm_micros": parse_metric_value(mets.get("OBSERVED_ECPM")),
            "sync_timestamp": sync_time,
        })

    return rows


def parse_campaign_rows(report: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    sync_time = utc_now_iso()

    for item in report:
        row = item.get("row")
        if not row:
            continue

        dims = row.get("dimensionValues", {})
        mets = row.get("metricValues", {})
        report_date = parse_report_date(dims)
        if not report_date:
            continue

        rows.append({
            "report_date": report_date,
            "campaign_id": dim_value(dims, "CAMPAIGN_ID"),
            "campaign_name": dim_label(dims, "CAMPAIGN_NAME"),
            "ad_id": dim_value(dims, "AD_ID"),
            "ad_name": dim_label(dims, "AD_NAME"),
            "placement_id": dim_value(dims, "PLACEMENT_ID"),
            "placement_name": dim_label(dims, "PLACEMENT_NAME"),
            "placement_platform": dim_label(dims, "PLACEMENT_PLATFORM"),
            "country_code": dim_value(dims, "COUNTRY"),
            "country_name": dim_label(dims, "COUNTRY"),
            "ad_format": dim_label(dims, "FORMAT"),
            "impressions": parse_metric_value(mets.get("IMPRESSIONS")) or 0,
            "clicks": parse_metric_value(mets.get("CLICKS")) or 0,
            "click_through_rate": parse_metric_value(mets.get("CLICK_THROUGH_RATE")),
            "installs": parse_metric_value(mets.get("INSTALLS")) or 0,
            "estimated_cost_micros": parse_metric_value(mets.get("ESTIMATED_COST")),
            "average_cpi_micros": parse_metric_value(mets.get("AVERAGE_CPI")),
            "interactions": parse_metric_value(mets.get("INTERACTIONS")) or 0,
            "sync_timestamp": sync_time,
        })

    return rows


# =============================================================================
# SYNC RUNNERS
# =============================================================================

def ensure_all_tables(client: bigquery.Client, include_campaign: bool) -> None:
    ensure_dataset_exists(client)
    ensure_table_exists(client, TABLE_ACCOUNT, ACCOUNT_SCHEMA)
    ensure_table_exists(client, TABLE_APPS, APPS_SCHEMA)
    ensure_table_exists(client, TABLE_AD_UNITS, AD_UNITS_SCHEMA)
    ensure_table_exists(client, TABLE_NETWORK, NETWORK_SCHEMA)
    ensure_table_exists(client, TABLE_NETWORK_AD_TYPE, NETWORK_AD_TYPE_SCHEMA)
    ensure_table_exists(client, TABLE_MEDIATION, MEDIATION_SCHEMA)

    if include_campaign:
        ensure_table_exists(client, TABLE_CAMPAIGN, CAMPAIGN_SCHEMA)


def sync_dimensions(service_v1, bq: bigquery.Client, account_name: str) -> None:
    account_rows = fetch_account_dim(service_v1, account_name)
    apps_rows = fetch_apps_dim(service_v1, account_name)
    ad_unit_rows = fetch_ad_units_dim(service_v1, account_name)

    load_json_rows(bq, TABLE_ACCOUNT, ACCOUNT_SCHEMA, account_rows, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    load_json_rows(bq, TABLE_APPS, APPS_SCHEMA, apps_rows, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    load_json_rows(bq, TABLE_AD_UNITS, AD_UNITS_SCHEMA, ad_unit_rows, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)


def sync_reports_for_range(
    service_v1,
    service_v1beta,
    bq: bigquery.Client,
    account_name: str,
    start_date: date,
    end_date: date,
    include_campaign: bool,
) -> Dict[str, int]:
    totals = {
        "network": 0,
        "network_ad_type": 0,
        "mediation": 0,
        "campaign": 0,
    }

    # Delete existing facts in range
    delete_date_range(bq, TABLE_NETWORK, start_date, end_date)
    delete_date_range(bq, TABLE_NETWORK_AD_TYPE, start_date, end_date)
    delete_date_range(bq, TABLE_MEDIATION, start_date, end_date)
    if include_campaign:
        delete_date_range(bq, TABLE_CAMPAIGN, start_date, end_date)

    # Network main
    print("Fetching network report...")
    network_report = fetch_network_report(service_v1, account_name, start_date, end_date, include_ad_type=False)
    network_rows = parse_network_rows(network_report, include_ad_type=False)
    totals["network"] = load_json_rows(bq, TABLE_NETWORK, NETWORK_SCHEMA, network_rows)

    # Network by ad type
    print("Fetching network report (ad type split)...")
    network_ad_type_report = fetch_network_report(service_v1, account_name, start_date, end_date, include_ad_type=True)
    network_ad_type_rows = parse_network_rows(network_ad_type_report, include_ad_type=True)
    totals["network_ad_type"] = load_json_rows(bq, TABLE_NETWORK_AD_TYPE, NETWORK_AD_TYPE_SCHEMA, network_ad_type_rows)

    # Mediation
    print("Fetching mediation report...")
    mediation_report = fetch_mediation_report(service_v1, account_name, start_date, end_date)
    mediation_rows = parse_mediation_rows(mediation_report)
    totals["mediation"] = load_json_rows(bq, TABLE_MEDIATION, MEDIATION_SCHEMA, mediation_rows)

    # Campaign beta optional
    if include_campaign:
        print("Fetching campaign report (beta)...")
        try:
            campaign_report = fetch_campaign_report(service_v1beta, account_name, start_date, end_date)
            campaign_rows = parse_campaign_rows(campaign_report)
            totals["campaign"] = load_json_rows(bq, TABLE_CAMPAIGN, CAMPAIGN_SCHEMA, campaign_rows)
        except HttpError as e:
            print(f"WARNING: Campaign report failed, skipping beta sync. Details: {e}")

    return totals


def sync(days_back: int = 2, include_campaign: bool = False) -> None:
    print(f"=== AdMob full sync (last {days_back} days) ===")

    end_date = datetime.utcnow().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back - 1)

    print(f"Date range: {start_date} -> {end_date}")

    admob_creds = get_admob_credentials()
    service_v1 = get_admob_service_v1(admob_creds)
    service_v1beta = get_admob_service_v1beta(admob_creds) if include_campaign else None
    bq = get_bigquery_client()

    account_name = get_account_name(service_v1)
    print(f"Using account: {account_name}")

    ensure_all_tables(bq, include_campaign=include_campaign)

    print("Syncing dimension tables...")
    sync_dimensions(service_v1, bq, account_name)

    print("Syncing report tables...")
    totals = sync_reports_for_range(
        service_v1=service_v1,
        service_v1beta=service_v1beta,
        bq=bq,
        account_name=account_name,
        start_date=start_date,
        end_date=end_date,
        include_campaign=include_campaign,
    )

    print("=== Sync complete ===")
    print(json.dumps(totals, indent=2))


def backfill(start_date_str: str, end_date_str: str, chunk_days: int = 7, include_campaign: bool = False) -> None:
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    print(f"=== AdMob backfill ===")
    print(f"Date range: {start_date} -> {end_date}")
    print(f"Chunk size: {chunk_days} days")

    if include_campaign and chunk_days > 30:
        print("Campaign report beta supports max 30 days per request. Reducing chunk_days to 30.")
        chunk_days = 30

    admob_creds = get_admob_credentials()
    service_v1 = get_admob_service_v1(admob_creds)
    service_v1beta = get_admob_service_v1beta(admob_creds) if include_campaign else None
    bq = get_bigquery_client()

    account_name = get_account_name(service_v1)
    print(f"Using account: {account_name}")

    ensure_all_tables(bq, include_campaign=include_campaign)

    print("Refreshing dimension tables...")
    sync_dimensions(service_v1, bq, account_name)

    current_start = start_date
    grand_totals = {
        "network": 0,
        "network_ad_type": 0,
        "mediation": 0,
        "campaign": 0,
    }

    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=chunk_days - 1), end_date)
        print(f"\nProcessing chunk: {current_start} -> {current_end}")

        totals = sync_reports_for_range(
            service_v1=service_v1,
            service_v1beta=service_v1beta,
            bq=bq,
            account_name=account_name,
            start_date=current_start,
            end_date=current_end,
            include_campaign=include_campaign,
        )

        for k in grand_totals:
            grand_totals[k] += totals.get(k, 0)

        current_start = current_end + timedelta(days=1)

    print("\n=== Backfill complete ===")
    print(json.dumps(grand_totals, indent=2))


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Full AdMob -> BigQuery sync",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Daily sync last 2 days:
    python src/admob_full_sync.py

  Sync last 7 days:
    python src/admob_full_sync.py --days 7

  Enable beta campaign report:
    python src/admob_full_sync.py --enable-campaign-beta

  Backfill:
    python src/admob_full_sync.py --backfill-start 2025-01-01 --backfill-end 2025-03-31

  Backfill with custom chunk size:
    python src/admob_full_sync.py --backfill-start 2025-01-01 --backfill-end 2025-03-31 --chunk-days 14
        """,
    )

    parser.add_argument("--days", type=int, default=2, help="Number of days to sync, default 2")
    parser.add_argument("--backfill-start", type=str, help="YYYY-MM-DD")
    parser.add_argument("--backfill-end", type=str, help="YYYY-MM-DD")
    parser.add_argument("--chunk-days", type=int, default=7, help="Backfill chunk size, default 7")
    parser.add_argument(
        "--enable-campaign-beta",
        action="store_true",
        help="Enable optional v1beta campaign report sync",
    )

    args = parser.parse_args()

    if not validate_config():
        sys.exit(1)

    include_campaign = ENABLE_ADMOB_BETA_CAMPAIGN or args.enable_campaign_beta

    try:
        if args.backfill_start and args.backfill_end:
            backfill(
                start_date_str=args.backfill_start,
                end_date_str=args.backfill_end,
                chunk_days=args.chunk_days,
                include_campaign=include_campaign,
            )
        else:
            sync(days_back=args.days, include_campaign=include_campaign)
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
