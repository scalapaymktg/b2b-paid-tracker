#!/usr/bin/env python3
"""
Google Ads API to Google Sheets - Weekly Report
Fetches campaign data from Google Ads and updates Google Sheets with weekly aggregation
"""

import gspread
from google.oauth2.service_account import Credentials
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from datetime import datetime, timedelta
from collections import defaultdict
import os

# Load .env file for local execution
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configuration - from environment variables or defaults
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_ACCOUNT_FILE = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS',
    os.path.join(SCRIPT_DIR, 'b2b-paid-tracker-2c1969b03f31.json'))
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1twwTpmJK1hiZVL0NuvpGJFN3G3enkwe510-i8ChwCKg')
SHEET_NAME = 'Google Ads API Test'

# Google Ads API credentials
GOOGLE_ADS_CLIENT_ID = os.environ.get('GOOGLE_ADS_CLIENT_ID', '')
GOOGLE_ADS_CLIENT_SECRET = os.environ.get('GOOGLE_ADS_CLIENT_SECRET', '')
GOOGLE_ADS_REFRESH_TOKEN = os.environ.get('GOOGLE_ADS_REFRESH_TOKEN', '')
GOOGLE_ADS_DEVELOPER_TOKEN = os.environ.get('GOOGLE_ADS_DEVELOPER_TOKEN', '')
GOOGLE_ADS_MANAGER_CUSTOMER_ID = os.environ.get('GOOGLE_ADS_MANAGER_CUSTOMER_ID', '5071207713')
GOOGLE_ADS_CUSTOMER_ID = os.environ.get('GOOGLE_ADS_CUSTOMER_ID', '5415618431')

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


def get_google_ads_client():
    """Create Google Ads API client"""
    credentials = {
        "developer_token": GOOGLE_ADS_DEVELOPER_TOKEN,
        "client_id": GOOGLE_ADS_CLIENT_ID,
        "client_secret": GOOGLE_ADS_CLIENT_SECRET,
        "refresh_token": GOOGLE_ADS_REFRESH_TOKEN,
        "use_proto_plus": True,
        "login_customer_id": GOOGLE_ADS_MANAGER_CUSTOMER_ID
    }
    return GoogleAdsClient.load_from_dict(credentials)


def fetch_google_ads_data_daily(since, until):
    """Fetch daily campaign data from Google Ads API"""
    client = get_google_ads_client()
    ga_service = client.get_service("GoogleAdsService")

    query = f"""
        SELECT
            segments.date,
            campaign.name,
            customer.currency_code,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.ctr,
            metrics.average_cpc,
            metrics.average_cpm,
            metrics.search_impression_share,
            metrics.search_click_share
        FROM campaign
        WHERE segments.date >= '{since}'
          AND segments.date <= '{until}'
          AND campaign.status != 'REMOVED'
        ORDER BY segments.date, campaign.name
    """

    all_data = []

    try:
        response = ga_service.search_stream(
            customer_id=GOOGLE_ADS_CUSTOMER_ID,
            query=query
        )

        for batch in response:
            for row in batch.results:
                all_data.append({
                    "date": row.segments.date,
                    "campaign_name": row.campaign.name,
                    "currency_code": row.customer.currency_code,
                    "cost_micros": row.metrics.cost_micros,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "ctr": row.metrics.ctr,
                    "average_cpc": row.metrics.average_cpc,
                    "average_cpm": row.metrics.average_cpm,
                    "search_impression_share": row.metrics.search_impression_share,
                    "search_click_share": row.metrics.search_click_share
                })

    except GoogleAdsException as ex:
        print(f"  Error fetching Google Ads data:")
        for error in ex.failure.errors:
            print(f"    {error.message}")
        raise

    return all_data


def aggregate_daily_to_weekly(daily_data):
    """Aggregate daily campaign data into weekly (Mon-Sun) buckets"""
    weekly_data = defaultdict(lambda: {
        "campaign_name": "",
        "currency_code": "EUR",
        "cost_micros": 0,
        "impressions": 0,
        "clicks": 0,
        "search_impression_share_sum": 0.0,
        "search_impression_share_count": 0,
        "search_click_share_sum": 0.0,
        "search_click_share_count": 0,
        "week_start": "",
        "week_end": ""
    })

    for item in daily_data:
        date_str = item.get("date", "")
        if not date_str:
            continue

        week_start = get_week_start(date_str)
        week_end = (datetime.strptime(week_start, '%Y-%m-%d') + timedelta(days=6)).strftime('%Y-%m-%d')
        campaign_name = item.get("campaign_name", "")
        key = (campaign_name, week_start)

        weekly_data[key]["campaign_name"] = campaign_name
        weekly_data[key]["currency_code"] = item.get("currency_code", "EUR")
        weekly_data[key]["cost_micros"] += item.get("cost_micros", 0)
        weekly_data[key]["impressions"] += item.get("impressions", 0)
        weekly_data[key]["clicks"] += item.get("clicks", 0)
        weekly_data[key]["week_start"] = week_start
        weekly_data[key]["week_end"] = week_end

        # Track search impression share for averaging (only if valid)
        sis = item.get("search_impression_share")
        if sis is not None and sis > 0:
            weekly_data[key]["search_impression_share_sum"] += sis
            weekly_data[key]["search_impression_share_count"] += 1

        # Track search click share for averaging (only if valid)
        scs = item.get("search_click_share")
        if scs is not None and scs > 0:
            weekly_data[key]["search_click_share_sum"] += scs
            weekly_data[key]["search_click_share_count"] += 1

    return list(weekly_data.values())


def process_weekly_data(weekly_data, hubspot_campaigns):
    """Process aggregated weekly data into rows for Google Sheets (columns A:L, M+ has formulas)"""
    headers = [
        "Week",                    # A
        "Campaign",                # B
        "Currency code",           # C
        "Cost",                    # D
        "Impr.",                   # E
        "Avg. CPM",                # F
        "Clicks",                  # G
        "CTR",                     # H
        "Avg. CPC",                # I
        "Search impr. share",      # J
        "Click share",             # K
        "Matched HubSpot Campaign" # L
    ]  # 12 columns: A-L

    rows = []
    for item in weekly_data:
        impressions = item["impressions"]
        clicks = item["clicks"]
        cost_micros = item["cost_micros"]

        # Skip if no impressions
        if impressions == 0:
            continue

        # Convert cost from micros to actual currency
        cost = cost_micros / 1_000_000

        # Calculate derived metrics
        cpm = (cost / impressions * 1000) if impressions > 0 else 0
        ctr = clicks / impressions if impressions > 0 else 0
        cpc = cost / clicks if clicks > 0 else 0

        # Average search impression share and click share
        sis_count = item["search_impression_share_count"]
        search_impression_share = item["search_impression_share_sum"] / sis_count if sis_count > 0 else 0

        scs_count = item["search_click_share_count"]
        search_click_share = item["search_click_share_sum"] / scs_count if scs_count > 0 else 0

        # Match campaign to HubSpot
        campaign_name = item["campaign_name"]
        matched_campaign = find_matching_hubspot_campaign(campaign_name, hubspot_campaigns)

        row = [
            f"{item['week_start']} - {item['week_end']}",  # A: Week
            campaign_name,                                  # B: Campaign
            item["currency_code"],                          # C: Currency code
            round(cost, 2),                                 # D: Cost
            int(impressions),                               # E: Impr.
            round(cpm, 2),                                  # F: Avg. CPM
            int(clicks),                                    # G: Clicks
            round(ctr, 4),                                  # H: CTR
            round(cpc, 2) if clicks > 0 else "",            # I: Avg. CPC
            round(search_impression_share, 4) if search_impression_share > 0 else "",  # J: Search impr. share
            round(search_click_share, 4) if search_click_share > 0 else "",            # K: Click share
            matched_campaign                                # L: Matched HubSpot Campaign
        ]
        rows.append(row)

    # Sort by week, campaign
    rows.sort(key=lambda x: (x[0] or "", x[1] or ""))
    return headers, rows


def apply_formatting(spreadsheet, worksheet):
    """Apply currency and percentage formatting"""
    sheet_id = worksheet.id
    body = {
        "requests": [
            # Header bold
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold"
            }},
            # D: Cost - Currency
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 3, "endColumnIndex": 4},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "€#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # F: Avg. CPM - Currency
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 5, "endColumnIndex": 6},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "€#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # H: CTR - Percentage
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 7, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # I: Avg. CPC - Currency
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 8, "endColumnIndex": 9},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "€#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # J: Search impr. share - Percentage
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 9, "endColumnIndex": 10},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            # K: Click share - Percentage
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 10, "endColumnIndex": 11},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                "fields": "userEnteredFormat.numberFormat"
            }}
        ]
    }
    spreadsheet.batch_update(body)


def update_google_sheets(headers, rows):
    """Update Google Sheets with data - ONLY columns A:L, preserving M+ (formulas)"""
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    try:
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        # Clear ONLY columns A:L (12 columns), not the entire sheet
        # This preserves formulas in column M onwards
        current_row_count = worksheet.row_count
        if current_row_count > 0:
            # Clear columns A:L by writing empty values
            empty_range = [[""] * 12 for _ in range(current_row_count)]
            worksheet.update(range_name='A1:L' + str(current_row_count), values=empty_range)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=500, cols=30)

    # Write new data to columns A:L only
    all_data = [headers] + rows
    worksheet.update(range_name='A1:L' + str(len(all_data)), values=all_data)
    apply_formatting(spreadsheet, worksheet)

    return len(rows)


def main():
    print(f"[{datetime.now()}] Starting Google Ads API to Google Sheets sync...")

    # Get date range (July 6, 2025 to today)
    since, until = get_date_range()
    print(f"  Date range: {since} to {until}")

    # Fetch daily data
    print("  Fetching daily data from Google Ads API...")
    daily_data = fetch_google_ads_data_daily(since, until)
    print(f"  Fetched {len(daily_data)} daily records from Google Ads API")

    # Aggregate daily data into weekly buckets
    print("  Aggregating daily data into weekly buckets (Mon-Sun)...")
    weekly_data = aggregate_daily_to_weekly(daily_data)
    print(f"  Aggregated into {len(weekly_data)} campaign-week combinations")

    # Load HubSpot campaigns for matching
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    hubspot_campaigns = load_hubspot_campaigns(gc, SPREADSHEET_ID)
    print(f"  Loaded {len(hubspot_campaigns)} unique HubSpot campaigns for matching")

    # Process weekly data with matching (columns A:L, M+ has formulas in sheet)
    headers, rows = process_weekly_data(weekly_data, hubspot_campaigns)

    if not rows:
        print("  No campaigns with impressions found")
        return

    # Update Google Sheets
    row_count = update_google_sheets(headers, rows)
    print(f"  Updated Google Sheets with {row_count} rows")

    print(f"[{datetime.now()}] Done!")


if __name__ == "__main__":
    main()
