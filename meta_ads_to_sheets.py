#!/usr/bin/env python3
"""
Meta API (Ad-level) to Google Sheets - Weekly Report
Fetches ad-level insights from Meta Ads and updates Google Sheets
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
SHEET_NAME = 'Meta Ads API Test'

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


def fetch_ad_insights_daily_chunk(since, until):
    """Fetch ad-level insights for a specific date range"""
    url = f"https://graph.facebook.com/v21.0/{AD_ACCOUNT_ID}/insights"

    params = {
        "access_token": ACCESS_TOKEN,
        "level": "ad",
        "fields": ",".join([
            "campaign_name",
            "ad_id",
            "ad_name",
            "impressions",
            "reach",
            "spend",
            "actions",
            "cost_per_action_type",
            "outbound_clicks",
            "video_p25_watched_actions",
            "video_p50_watched_actions",
            "video_p75_watched_actions",
            "date_start",
            "date_stop"
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

        data = result.get("data", [])
        all_data.extend(data)

        next_url = result.get("paging", {}).get("next")

    return all_data


def fetch_ad_insights_daily(since, until):
    """Fetch ad-level insights in monthly chunks to avoid API timeouts"""
    from dateutil.relativedelta import relativedelta

    start_date = datetime.strptime(since, '%Y-%m-%d')
    end_date = datetime.strptime(until, '%Y-%m-%d')

    all_data = []

    # Fetch in monthly chunks
    current_start = start_date
    while current_start < end_date:
        # End of current chunk (end of month or until date, whichever is earlier)
        current_end = min(current_start + relativedelta(months=1) - timedelta(days=1), end_date)

        chunk_since = current_start.strftime('%Y-%m-%d')
        chunk_until = current_end.strftime('%Y-%m-%d')

        print(f"    Fetching {chunk_since} to {chunk_until}...")

        try:
            chunk_data = fetch_ad_insights_daily_chunk(chunk_since, chunk_until)
            all_data.extend(chunk_data)
            print(f"      Got {len(chunk_data)} records")
        except Exception as e:
            print(f"      Error: {e}")

        # Move to next month
        current_start = current_end + timedelta(days=1)

    return all_data


def aggregate_ads_daily_to_weekly(daily_data):
    """Aggregate daily ad data into weekly (Mon-Sun) buckets"""
    from collections import defaultdict

    # Group by (ad_id, week_start)
    weekly_data = defaultdict(lambda: {
        "campaign_name": "",
        "ad_id": "",
        "ad_name": "",
        "impressions": 0,
        "reach": 0,
        "spend": 0.0,
        "leads": 0,
        "landing_page_views": 0,
        "engagement": 0,
        "outbound_clicks": 0,
        "video_p25": 0,
        "video_p50": 0,
        "video_p75": 0,
        "week_start": "",
        "week_end": ""
    })

    for item in daily_data:
        date_str = item.get("date_start", "")
        if not date_str:
            continue

        week_start = get_week_start(date_str)
        week_end = (datetime.strptime(week_start, '%Y-%m-%d') + timedelta(days=6)).strftime('%Y-%m-%d')
        ad_id = item.get("ad_id", "")
        key = (ad_id, week_start)

        weekly_data[key]["campaign_name"] = item.get("campaign_name", "")
        weekly_data[key]["ad_id"] = ad_id
        weekly_data[key]["ad_name"] = item.get("ad_name", "")
        weekly_data[key]["impressions"] += int(item.get("impressions", 0))
        weekly_data[key]["reach"] += int(item.get("reach", 0))
        weekly_data[key]["spend"] += float(item.get("spend", 0))
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

        # Outbound clicks
        if "outbound_clicks" in item:
            for click in item["outbound_clicks"]:
                weekly_data[key]["outbound_clicks"] += float(click.get("value", 0))

        # Video metrics
        if "video_p25_watched_actions" in item:
            for action in item["video_p25_watched_actions"]:
                weekly_data[key]["video_p25"] += float(action.get("value", 0))
        if "video_p50_watched_actions" in item:
            for action in item["video_p50_watched_actions"]:
                weekly_data[key]["video_p50"] += float(action.get("value", 0))
        if "video_p75_watched_actions" in item:
            for action in item["video_p75_watched_actions"]:
                weekly_data[key]["video_p75"] += float(action.get("value", 0))

    return list(weekly_data.values())


def process_weekly_ad_data(weekly_data, hubspot_campaigns):
    """Process aggregated weekly ad data into rows for Google Sheets (columns A-Z, AA+ has formulas)"""
    # Headers matching Excel structure (26 columns: A-Z)
    headers = [
        "Campaign name",           # A
        "Week",                    # B
        "Ad name",                 # C
        "Impressions",             # D
        "Reach",                   # E
        "Frequency",               # F
        "Amount spent (EUR)",      # G
        "Result type",             # H
        "Results",                 # I
        "Cost per result",         # J
        "CR",                      # K
        "CPM (cost per 1,000 impressions)",  # L
        "Outbound clicks",         # M
        "Outbound CTR (click-through rate)", # N
        "Cost per outbound click", # O
        "ER",                      # P
        "Hook Rate",               # Q
        "Hold Rate",               # R
        "Traffic Loss Rate",       # S
        "Website landing page views",  # T
        "Leads",                   # U
        "Cost per Lead (EUR)",     # V
        "Preview link",            # W
        "Reporting starts",        # X
        "Reporting ends",          # Y
        "Matched HubSpot Campaign" # Z
    ]

    rows = []
    for item in weekly_data:
        impressions = item["impressions"]
        reach = item["reach"]
        spend = item["spend"]
        leads = item["leads"]
        landing_page_views = item["landing_page_views"]
        engagement = item["engagement"]
        outbound_clicks = item["outbound_clicks"]
        video_p25 = item["video_p25"]
        video_p50 = item["video_p50"]
        video_p75 = item["video_p75"]

        # Skip ads with no impressions
        if impressions == 0:
            continue

        # Calculate derived metrics
        frequency = impressions / reach if reach > 0 else 0
        cpm = (spend / impressions * 1000) if impressions > 0 else 0
        er = engagement / impressions if impressions > 0 else 0
        hook_rate = video_p25 / impressions if impressions > 0 else 0
        hold_rate = video_p50 / video_p25 if video_p25 > 0 else 0
        traffic_loss_rate = 1 - (video_p75 / video_p25) if video_p25 > 0 else 0
        cr = leads / landing_page_views if landing_page_views > 0 else 0
        cost_per_lead = spend / leads if leads > 0 else 0
        outbound_ctr = outbound_clicks / impressions if impressions > 0 else 0
        cost_per_outbound_click = spend / outbound_clicks if outbound_clicks > 0 else 0

        # Result type and cost per result
        result_type = "Lead" if leads > 0 else ""
        results = leads
        cost_per_result = cost_per_lead

        # Preview link
        ad_id = item["ad_id"]
        preview_link = f"https://www.facebook.com/ads/manager/preview/{ad_id}" if ad_id else ""

        # Match campaign to HubSpot
        campaign_name = item["campaign_name"]
        matched_campaign = find_matching_hubspot_campaign(campaign_name, hubspot_campaigns)

        row = [
            campaign_name,                                   # A: Campaign name
            f"{item['week_start']} - {item['week_end']}",    # B: Week
            item["ad_name"],                                 # C: Ad name
            int(impressions),                                # D: Impressions
            int(reach),                                      # E: Reach
            round(frequency, 2),                             # F: Frequency
            round(spend, 2),                                 # G: Amount spent (EUR)
            result_type,                                     # H: Result type
            int(results) if results > 0 else "",             # I: Results
            round(cost_per_result, 2) if cost_per_result > 0 else "",  # J: Cost per result
            round(cr, 4),                                    # K: CR
            round(cpm, 2),                                   # L: CPM
            int(outbound_clicks) if outbound_clicks > 0 else "",  # M: Outbound clicks
            round(outbound_ctr, 4) if outbound_ctr > 0 else "",  # N: Outbound CTR
            round(cost_per_outbound_click, 2) if cost_per_outbound_click > 0 else "",  # O: Cost per outbound click
            round(er, 4),                                    # P: ER
            round(hook_rate, 4),                             # Q: Hook Rate
            round(hold_rate, 4) if video_p25 > 0 else "",    # R: Hold Rate
            round(traffic_loss_rate, 4) if video_p25 > 0 else "",  # S: Traffic Loss Rate
            int(landing_page_views) if landing_page_views > 0 else "",  # T: Website landing page views
            int(leads) if leads > 0 else "",                 # U: Leads
            round(cost_per_lead, 2) if cost_per_lead > 0 else "",  # V: Cost per Lead (EUR)
            preview_link,                                    # W: Preview link
            item["week_start"],                              # X: Reporting starts
            item["week_end"],                                # Y: Reporting ends
            matched_campaign                                 # Z: Matched HubSpot Campaign
        ]
        rows.append(row)

    # Sort by campaign, ad name, week
    rows.sort(key=lambda x: (x[0] or "", x[2] or "", x[1] or ""))

    return headers, rows


def apply_formatting(spreadsheet, worksheet):
    """Apply currency and percentage formatting based on new column structure (A-Y)"""
    sheet_id = worksheet.id
    body = {
        "requests": [
            # Header bold (row 1)
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold"
            }},
            # G: Amount spent (EUR) - Currency (index 6)
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 6, "endColumnIndex": 7},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "€#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # J: Cost per result - Currency (index 9)
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 9, "endColumnIndex": 10},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "€#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # K: CR - Percentage (index 10)
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 10, "endColumnIndex": 11},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # L: CPM - Currency (index 11)
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 11, "endColumnIndex": 12},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "€#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # N: Outbound CTR - Percentage (index 13)
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 13, "endColumnIndex": 14},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # O: Cost per outbound click - Currency (index 14)
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 14, "endColumnIndex": 15},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "€#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # P: ER - Percentage (index 15)
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 15, "endColumnIndex": 16},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # Q: Hook Rate - Percentage (index 16)
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 16, "endColumnIndex": 17},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # R: Hold Rate - Percentage (index 17)
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 17, "endColumnIndex": 18},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # S: Traffic Loss Rate - Percentage (index 18)
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 18, "endColumnIndex": 19},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # V: Cost per Lead (EUR) - Currency (index 21)
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 21, "endColumnIndex": 22},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "€#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
        ]
    }
    spreadsheet.batch_update(body)


def update_google_sheets(headers, rows):
    """Update Google Sheets with data - ONLY columns A:Z, preserving AA+ (formulas)"""
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    try:
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        # Clear ONLY columns A:Z (26 columns), not the entire sheet
        # This preserves formulas in column AA onwards
        current_row_count = worksheet.row_count
        if current_row_count > 0:
            # Clear columns A:Z by writing empty values
            empty_range = [[""] * 26 for _ in range(current_row_count)]
            worksheet.update(range_name='A1:Z' + str(current_row_count), values=empty_range)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=2000, cols=50)

    # Write new data to columns A:Z only
    all_data = [headers] + rows
    worksheet.update(range_name='A1:Z' + str(len(all_data)), values=all_data)
    apply_formatting(spreadsheet, worksheet)

    return len(rows)


def main():
    print(f"[{datetime.now()}] Starting Meta Ads API to Google Sheets sync...")

    # Get date range (July 6, 2025 to today)
    since, until = get_date_range()
    print(f"  Date range: {since} to {until}")

    # Fetch daily data
    print("  Fetching daily ad data from Meta API...")
    daily_data = fetch_ad_insights_daily(since, until)
    print(f"  Fetched {len(daily_data)} daily ad records from Meta API")

    # Aggregate daily data into weekly buckets
    print("  Aggregating daily data into weekly buckets (Mon-Sun)...")
    weekly_data = aggregate_ads_daily_to_weekly(daily_data)
    print(f"  Aggregated into {len(weekly_data)} ad-week combinations")

    # Load HubSpot campaigns for matching
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    hubspot_campaigns = load_hubspot_campaigns(gc, SPREADSHEET_ID)
    print(f"  Loaded {len(hubspot_campaigns)} unique HubSpot campaigns for matching")

    # Process weekly data with matching (columns A:Z, AA+ has formulas in sheet)
    headers, rows = process_weekly_ad_data(weekly_data, hubspot_campaigns)
    print(f"  Ads with delivery: {len(rows)}")

    if not rows:
        print("  No ads with delivery found")
        return

    # Update Google Sheets
    row_count = update_google_sheets(headers, rows)
    print(f"  Updated Google Sheets with {row_count} rows")

    print(f"[{datetime.now()}] Done!")


if __name__ == "__main__":
    main()
