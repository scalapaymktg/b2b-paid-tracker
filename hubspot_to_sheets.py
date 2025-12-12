#!/usr/bin/env python3
"""
HubSpot API to Google Sheets - Weekly Qualified Pipeline Report
Fetches deals from HubSpot and updates Google Sheets
"""

import requests
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import os

# Load .env file for local execution
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configuration - from environment variables or defaults (for local execution)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_ACCOUNT_FILE = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS',
    os.path.join(SCRIPT_DIR, 'b2b-paid-tracker-2c1969b03f31.json'))
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1twwTpmJK1hiZVL0NuvpGJFN3G3enkwe510-i8ChwCKg')
SHEET_NAME = 'HubSpot API Test'

# HubSpot config - from environment variables (required for GitHub Actions, optional for local with .env)
HUBSPOT_TOKEN = os.environ.get('HUBSPOT_TOKEN', "")
HUBSPOT_URL = "https://api.hubspot.com/crm/v3/objects/deals/search"

# Pipeline IDs
PIPELINE_IDS = ["77766861", "75805933"]  # Sales - Pipeline, Marketing - Inbound automated

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Mapping ID -> Label for Pipeline and Deal Stage
PIPELINE_MAP = {
    "77766861": "Sales - Pipeline",
    "75805933": "Marketing - Inbound automated Micro/Small Pipeline",
    "127897798": "Partner - Pipeline",
    "258360802": "Account Management - Churn Pipeline",
    "1347411134": "Partnership - Pipeline",
    "208540610": "Account Management - Pipeline"
}

STAGE_MAP = {
    "184232166": "SQL",
    "184232167": "Discovery meetings",
    "2705937612": "NBM Pending Review",
    "184232168": "Business Meeting",
    "184232169": "Negotiation",
    "184220904": "Onboarding Initiated",
    "184220905": "Onboarding Completed",
    "184232171": "Won",
    "184232172": "Closed lost",
    "181259987": "Inbound Created",
    "181259988": "Proposal sent",
    "181259989": "Registered",
    "720800761": "KYC Pending Approval",
    "181259990": "Onboarding Completed",
    "2019815647": "Integration Completed",
    "181259992": "Won",
    "181259993": "Closed lost",
    "256106455": "Target",
    "256106456": "In discussion",
    "256106457": "Live & Engaged",
    "256106458": "Live & Sleeping",
    "256106459": "Not interested",
    "428007611": "Churn Risk",
    "428007613": "Negotiation",
    "428007616": "Churn",
    "428007617": "Retained",
    "2586233042": "Inbound Created",
    "1834011865": "Proposal sent",
    "1834011866": "KYC Pending Approval",
    "2019816637": "Onboarding Completed",
    "2021900503": "Integration Completed",
    "1834011870": "Won",
    "1834011871": "Closed Lost",
    "363460847": "Target",
    "363460848": "Discovery meetings",
    "363460849": "Business Meeting",
    "363460850": "Validation & Negotiation",
    "362959344": "Final proposal to EB",
    "362959345": "Contract signed",
    "363460851": "Won",
    "363460852": "Closed lost",
    "363503572": "Terminated"
}

# Column mapping: HubSpot property -> Sheet column name
COLUMN_MAPPING = {
    "deal_qualification_date": "Deal qualification date",
    "legal_entity_country_region": "Legal Entity - Country/Region",
    "amount": "Amount",
    "ttv_all_time": "TTV All Time",
    "conversion_touch__utm_medium": "Conversion Touch: UTM Medium",
    "conversion_touch__utm_source": "Conversion Touch: UTM Source",
    "conversion_touch__utm_content": "Conversion Touch: UTM Content",
    "conversion_touch__utm_campaign": "Conversion Touch: UTM Campaign",
    "conversion_touch__referral_source": "Conversion Touch: Referral Source",
    "conversion_touch__aggregate_source": "Conversion Touch: Aggregate Source",
    "store_type": "Store type",
    "pipeline": "Pipeline",
    "dealstage": "Deal Stage",
    "hs_object_id": "Deal ID"
}


def get_last_week_dates():
    """Get last week Monday-Sunday dates as timestamps in milliseconds"""
    today = datetime.now()
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)
    # For HubSpot LT filter, we need the day after
    end_date = last_sunday + timedelta(days=1)

    start_ts = int(last_monday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    end_ts = int(end_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)

    return start_ts, end_ts, last_monday.strftime('%Y-%m-%d'), last_sunday.strftime('%Y-%m-%d')


def fetch_all_deals(start_ts, end_ts):
    """Fetch all deals from HubSpot with pagination"""
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json"
    }

    payload_template = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "generic_source",
                        "operator": "IN",
                        "values": ["Marketing - Interactions & Inbound requests"]
                    },
                    {
                        "propertyName": "pipeline",
                        "operator": "IN",
                        "values": PIPELINE_IDS
                    },
                    {
                        "propertyName": "deal_qualification_date",
                        "operator": "GTE",
                        "value": str(start_ts)
                    },
                    {
                        "propertyName": "deal_qualification_date",
                        "operator": "LT",
                        "value": str(end_ts)
                    },
                    {
                        "propertyName": "conversion_touch__aggregate_source",
                        "operator": "IN",
                        "values": ["Paid Search", "Paid Social"]
                    }
                ]
            }
        ],
        "properties": list(COLUMN_MAPPING.keys()),
        "limit": 100
    }

    all_results = []
    after = None

    while True:
        payload = payload_template.copy()
        if after:
            payload["after"] = after

        response = requests.post(HUBSPOT_URL, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        all_results.extend(results)

        paging = data.get("paging", {})
        next_after = paging.get("next", {}).get("after")

        if not next_after:
            break
        after = next_after

    return all_results


def process_deals(deals):
    """Process deals into rows for Google Sheets"""
    headers = list(COLUMN_MAPPING.values())
    rows = []

    for deal in deals:
        props = deal.get("properties", {})
        row = []
        for prop_name in COLUMN_MAPPING.keys():
            value = props.get(prop_name, "")
            if value is None:
                value = "(No value)"

            # Map pipeline ID to label
            if prop_name == "pipeline" and value in PIPELINE_MAP:
                value = PIPELINE_MAP[value]

            # Map dealstage ID to label
            if prop_name == "dealstage" and value in STAGE_MAP:
                value = STAGE_MAP[value]

            row.append(value)
        rows.append(row)

    # Sort by deal_qualification_date
    rows.sort(key=lambda x: x[0] if x[0] else "")

    return headers, rows


def apply_formatting(spreadsheet, worksheet):
    """Apply formatting to the sheet"""
    sheet_id = worksheet.id
    body = {
        "requests": [
            # Header bold
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold"
            }},
            # Amount column (C) - Currency
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 2, "endColumnIndex": 3},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "€#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # TTV All Time column (D) - Currency
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 3, "endColumnIndex": 4},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "€#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }}
        ]
    }
    spreadsheet.batch_update(body)


def update_google_sheets(headers, rows):
    """Update Google Sheets with data"""
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    try:
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=20)

    worksheet.update(range_name='A1', values=[headers] + rows)
    apply_formatting(spreadsheet, worksheet)

    return len(rows)


def main():
    print(f"[{datetime.now()}] Starting HubSpot to Google Sheets sync...")

    # Get date range
    start_ts, end_ts, start_date, end_date = get_last_week_dates()
    print(f"  Date range: {start_date} to {end_date}")

    # Fetch deals
    deals = fetch_all_deals(start_ts, end_ts)
    print(f"  Fetched {len(deals)} deals from HubSpot")

    if not deals:
        print("  No deals found for this period")
        return

    # Process data
    headers, rows = process_deals(deals)

    # Update Google Sheets
    row_count = update_google_sheets(headers, rows)
    print(f"  Updated Google Sheets with {row_count} rows")

    print(f"[{datetime.now()}] Done!")


if __name__ == "__main__":
    main()
