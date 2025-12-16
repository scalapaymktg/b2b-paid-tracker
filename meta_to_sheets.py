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


def is_prefix_match(str1, str2):
    """Returns True if one string is a prefix of the other."""
    if not str1 or not str2:
        return False
    return str1.startswith(str2) or str2.startswith(str1)


def find_matching_hubspot_campaign(campaign_name, hubspot_campaigns):
    """Find the best matching HubSpot UTM campaign using prefix logic."""
    if not campaign_name:
        return ""
    matches = [hs for hs in hubspot_campaigns if is_prefix_match(campaign_name, hs)]
    if not matches:
        return ""
    return max(matches, key=len)


def load_hubspot_campaigns(gc, spreadsheet_id):
    """Load unique HubSpot UTM campaign names from the HubSpot API Test sheet"""
    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        hs_sheet = spreadsheet.worksheet("HubSpot API Test")
        data = hs_sheet.get("H2:H")  # Column H = UTM Campaign
        campaigns = list(set([row[0] for row in data if row and row[0] and row[0] != "(No value)"]))
        return campaigns
    except Exception as e:
        print(f"  Warning: Could not load HubSpot campaigns: {e}")
        return []


def get_date_range():
    """Get date range from July 6, 2025 to today"""
    since = "2025-07-06"
    until = datetime.now().strftime('%Y-%m-%d')
    return since, until


def get_week_start(date_str):
    """Get the Monday of the week for a given date (ISO week: Mon-Sun)"""
    date = datetime.strptime(date_str, '%Y-%m-%d')
    monday = date - timedelta(days=date.weekday())
    return monday.strftime('%Y-%m-%d')


def fetch_meta_data_daily(since, until):
    """Fetch campaign insights from Meta API with daily breakdown"""
    url = f"https://graph.facebook.com/v21.0/{AD_ACCOUNT_ID}/insights"
    params = {
        "access_token": ACCESS_TOKEN,
        "level": "campaign",
        "fields": ",".join([
            "campaign_id", "campaign_name", "impressions", "reach",
            "spend", "cpm", "account_currency", "actions", "cost_per_action_type",
            "date_start", "date_stop"
        ]),
        "time_increment": 1,  # Daily breakdown
        "time_range": json.dumps({"since": since, "until": until}),
        "limit": 500
    }

    all_data = []
    next_url = url
    is_first = True

    while next_url:
        if is_first:
            response = requests.get(url, params=params)
            is_first = False
        else:
            response = requests.get(next_url)

        response.raise_for_status()
        result = response.json()
        all_data.extend(result.get("data", []))
        next_url = result.get("paging", {}).get("next")

    return all_data


def aggregate_daily_to_weekly(daily_data):
    """Aggregate daily campaign data into weekly (Mon-Sun) buckets"""
    from collections import defaultdict

    # Group by (campaign_name, week_start)
    weekly_data = defaultdict(lambda: {
        "campaign_id": "",
        "campaign_name": "",
        "impressions": 0,
        "reach": 0,
        "spend": 0.0,
        "account_currency": "EUR",
        "leads": 0,
        "landing_page_views": 0,
        "engagement": 0,
        "video_views": 0,
        "cost_per_lead_total": 0.0,
        "lead_count_for_cpl": 0,
        "week_start": "",
        "week_end": ""
    })

    for item in daily_data:
        date_str = item.get("date_start", "")
        if not date_str:
            continue

        week_start = get_week_start(date_str)
        week_end = (datetime.strptime(week_start, '%Y-%m-%d') + timedelta(days=6)).strftime('%Y-%m-%d')
        campaign_name = item.get("campaign_name", "")
        key = (campaign_name, week_start)

        weekly_data[key]["campaign_id"] = item.get("campaign_id", "")
        weekly_data[key]["campaign_name"] = campaign_name
        weekly_data[key]["impressions"] += int(item.get("impressions", 0))
        weekly_data[key]["reach"] += int(item.get("reach", 0))
        weekly_data[key]["spend"] += float(item.get("spend", 0))
        weekly_data[key]["account_currency"] = item.get("account_currency", "EUR")
        weekly_data[key]["week_start"] = week_start
        weekly_data[key]["week_end"] = week_end

        # Extract actions
        if "actions" in item:
            for action in item["actions"]:
                action_type = action["action_type"]
                value = float(action["value"])
                if action_type == "lead":
                    weekly_data[key]["leads"] += value
                elif action_type == "landing_page_view":
                    weekly_data[key]["landing_page_views"] += value
                elif action_type == "post_engagement":
                    weekly_data[key]["engagement"] += value
                elif action_type == "video_view":
                    weekly_data[key]["video_views"] += value

        # Track cost per lead for weighted average
        if "cost_per_action_type" in item:
            for cost in item["cost_per_action_type"]:
                if cost["action_type"] == "lead":
                    weekly_data[key]["cost_per_lead_total"] += float(cost["value"]) * float(item.get("actions", [{}])[0].get("value", 1) if item.get("actions") else 1)
                    weekly_data[key]["lead_count_for_cpl"] += 1

    return list(weekly_data.values())


def process_weekly_data(weekly_data, hubspot_campaigns):
    """Process aggregated weekly data into rows for Google Sheets (columns A:P, Q+ has formulas)"""
    headers = [
        "Campaign name", "Week", "Impressions", "Reach", "Frequency", "Currency",
        "Amount spent (EUR)", "CPM", "ER", "Hook Rate", "Landing Page Views",
        "Leads", "Cost per Lead (EUR)", "Reporting starts", "Reporting ends",
        "Matched HubSpot Campaign"  # Column P
    ]  # 16 columns: A-P

    rows = []
    for item in weekly_data:
        impressions = item["impressions"]
        reach = item["reach"]
        spend = item["spend"]
        leads = item["leads"]
        landing_page_views = item["landing_page_views"]
        engagement = item["engagement"]
        video_views = item["video_views"]

        # Skip if no impressions
        if impressions == 0:
            continue

        # Calculate derived metrics
        frequency = impressions / reach if reach > 0 else 0
        cpm = (spend / impressions * 1000) if impressions > 0 else 0
        er = engagement / impressions if impressions > 0 else 0
        hook_rate = video_views / impressions if impressions > 0 else 0
        cost_per_lead = spend / leads if leads > 0 else 0

        campaign_name = item["campaign_name"]
        matched_campaign = find_matching_hubspot_campaign(campaign_name, hubspot_campaigns)

        row = [
            campaign_name,
            f"{item['week_start']} - {item['week_end']}",
            int(impressions),
            int(reach),
            round(frequency, 2),
            item["account_currency"],
            round(spend, 2),
            round(cpm, 2),
            round(er, 4),
            round(hook_rate, 4),
            int(landing_page_views),
            int(leads) if leads > 0 else "",
            round(cost_per_lead, 2) if cost_per_lead > 0 else "",
            item["week_start"],
            item["week_end"],
            matched_campaign  # Column P
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
    """Update Google Sheets with data - ONLY columns A:P, preserving Q+ (formulas)"""
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    try:
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        # Clear ONLY columns A:P (16 columns), not the entire sheet
        # This preserves formulas in column Q onwards
        current_row_count = worksheet.row_count
        if current_row_count > 0:
            # Clear columns A:P by writing empty values
            empty_range = [[""] * 16 for _ in range(current_row_count)]
            worksheet.update(range_name='A1:P' + str(current_row_count), values=empty_range)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=500, cols=30)

    # Write new data to columns A:P only
    all_data = [headers] + rows
    worksheet.update(range_name='A1:P' + str(len(all_data)), values=all_data)
    apply_formatting(spreadsheet, worksheet)

    return len(rows)


def main():
    print(f"[{datetime.now()}] Starting Meta API to Google Sheets sync...")

    # Get date range (July 6, 2025 to today)
    since, until = get_date_range()
    print(f"  Date range: {since} to {until}")

    # Fetch daily data
    print("  Fetching daily data from Meta API...")
    daily_data = fetch_meta_data_daily(since, until)
    print(f"  Fetched {len(daily_data)} daily records from Meta API")

    # Aggregate daily data into weekly buckets
    print("  Aggregating daily data into weekly buckets (Mon-Sun)...")
    weekly_data = aggregate_daily_to_weekly(daily_data)
    print(f"  Aggregated into {len(weekly_data)} campaign-week combinations")

    # Load HubSpot campaigns for matching
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    hubspot_campaigns = load_hubspot_campaigns(gc, SPREADSHEET_ID)
    print(f"  Loaded {len(hubspot_campaigns)} unique HubSpot campaigns for matching")

    # Process weekly data with matching (columns A:P, Q+ has formulas in sheet)
    headers, rows = process_weekly_data(weekly_data, hubspot_campaigns)

    # Update Google Sheets
    row_count = update_google_sheets(headers, rows)
    print(f"  Updated Google Sheets with {row_count} rows")

    print(f"[{datetime.now()}] Done!")


if __name__ == "__main__":
    main()
