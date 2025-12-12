#!/usr/bin/env python3
"""
Meta API to Google Sheets - Weekly Report
Runs every Monday at 2:00 AM to fetch Meta Ads data and update Google Sheets
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
SHEET_NAME = 'Meta API Test'

# Meta API credentials - from environment variables (required for GitHub Actions, optional for local with .env)
ACCESS_TOKEN = os.environ.get('META_ACCESS_TOKEN', "")
AD_ACCOUNT_ID = os.environ.get('META_AD_ACCOUNT_ID', "")

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


def get_last_week_dates():
    """Get last week Monday-Sunday dates"""
    today = datetime.now()
    # Find last Monday
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)
    return last_monday.strftime('%Y-%m-%d'), last_sunday.strftime('%Y-%m-%d')


def fetch_meta_data(since, until):
    """Fetch campaign insights from Meta API"""
    url = f"https://graph.facebook.com/v21.0/{AD_ACCOUNT_ID}/insights"
    params = {
        "access_token": ACCESS_TOKEN,
        "level": "campaign",
        "fields": ",".join([
            "campaign_id", "campaign_name", "impressions", "reach", "frequency",
            "spend", "cpm", "account_currency", "actions", "cost_per_action_type",
            "date_start", "date_stop"
        ]),
        "time_increment": 7,
        "time_range": json.dumps({"since": since, "until": until}),
        "limit": 500
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get("data", [])


def process_data(data):
    """Process Meta API data into rows for Google Sheets"""
    headers = [
        "Campaign name", "Week", "Impressions", "Reach", "Frequency", "Currency",
        "Amount spent (EUR)", "CPM", "ER", "Hook Rate", "Landing Page Views",
        "Leads", "Cost per Lead (EUR)", "Reporting starts", "Reporting ends"
    ]

    rows = []
    for item in data:
        leads = landing_page_views = engagement = video_views = cost_per_lead = 0

        if "actions" in item:
            for action in item["actions"]:
                if action["action_type"] == "lead":
                    leads = float(action["value"])
                elif action["action_type"] == "landing_page_view":
                    landing_page_views = float(action["value"])
                elif action["action_type"] == "post_engagement":
                    engagement = float(action["value"])
                elif action["action_type"] == "video_view":
                    video_views = float(action["value"])

        if "cost_per_action_type" in item:
            for cost in item["cost_per_action_type"]:
                if cost["action_type"] == "lead":
                    cost_per_lead = float(cost["value"])

        impressions = float(item.get("impressions", 0))
        er = (engagement / impressions) if impressions > 0 else 0
        hook_rate = (video_views / impressions) if impressions > 0 else 0

        row = [
            item.get("campaign_name"),
            f"{item.get('date_start')} - {item.get('date_stop')}",
            int(impressions),
            int(item.get("reach", 0)),
            round(float(item.get("frequency", 0)), 2),
            item.get("account_currency", "EUR"),
            round(float(item.get("spend", 0)), 2),
            round(float(item.get("cpm", 0)), 2),
            round(er, 4),
            round(hook_rate, 4),
            int(landing_page_views),
            int(leads) if leads > 0 else "",
            round(cost_per_lead, 2) if cost_per_lead > 0 else "",
            item.get("date_start"),
            item.get("date_stop")
        ]
        rows.append(row)

    rows.sort(key=lambda x: (x[0] or "", x[1]))
    return headers, rows


def apply_formatting(spreadsheet, worksheet):
    """Apply currency and percentage formatting"""
    sheet_id = worksheet.id
    body = {
        "requests": [
            # Amount spent (EUR) - Currency
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 6, "endColumnIndex": 7},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "€#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # CPM - Currency
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 7, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "€#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # Cost per Lead - Currency
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 12, "endColumnIndex": 13},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "€#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # ER - Percentage
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 8, "endColumnIndex": 9},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # Hook Rate - Percentage
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 9, "endColumnIndex": 10},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # Header bold
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold"
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
        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=100, cols=20)

    worksheet.update(range_name='A1', values=[headers] + rows)
    apply_formatting(spreadsheet, worksheet)

    return len(rows)


def main():
    print(f"[{datetime.now()}] Starting Meta API to Google Sheets sync...")

    # Get date range (last week Mon-Sun)
    since, until = get_last_week_dates()
    print(f"  Date range: {since} to {until}")

    # Fetch data
    data = fetch_meta_data(since, until)
    print(f"  Fetched {len(data)} records from Meta API")

    # Process data
    headers, rows = process_data(data)

    # Update Google Sheets
    row_count = update_google_sheets(headers, rows)
    print(f"  Updated Google Sheets with {row_count} rows")

    print(f"[{datetime.now()}] Done!")


if __name__ == "__main__":
    main()
