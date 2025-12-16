#!/usr/bin/env python3
"""
Brand Auction Report to Google Sheets
Runs every Monday at 06:30 to:
1. Search Gmail for "Your Google Ads report is ready: B2B Brand Auction Insight" emails
2. Extract the download link from the email
3. Download the CSV report
4. Upload to Google Sheets "brand auction test" tab
"""

import requests
import gspread
import csv
import io
import base64
import re
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import os

# Load .env file for local execution
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_ACCOUNT_FILE = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS',
    os.path.join(SCRIPT_DIR, 'b2b-paid-tracker-2c1969b03f31.json'))
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1twwTpmJK1hiZVL0NuvpGJFN3G3enkwe510-i8ChwCKg')
SHEET_NAME = 'brand auction test'

# Gmail OAuth credentials (uses dedicated Gmail refresh token)
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_ADS_CLIENT_ID', '')
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_ADS_CLIENT_SECRET', '')
# Use GOOGLE_GMAIL_REFRESH_TOKEN if available, otherwise fall back to GOOGLE_ADS_REFRESH_TOKEN
GOOGLE_REFRESH_TOKEN = os.environ.get('GOOGLE_GMAIL_REFRESH_TOKEN', os.environ.get('GOOGLE_ADS_REFRESH_TOKEN', ''))

SHEETS_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# Email search parameters
EMAIL_SUBJECT = "Your Google Ads report is ready: B2B Brand Auction Insight"


def get_gmail_service():
    """Create Gmail API service using OAuth credentials"""
    creds = Credentials(
        token=None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        scopes=GMAIL_SCOPES
    )

    # Refresh the token
    creds.refresh(Request())

    return build('gmail', 'v1', credentials=creds)


def search_email(service, hours_back=24):
    """Search for the report email from the last N hours"""
    # Calculate the date for the query
    after_date = datetime.now() - timedelta(hours=hours_back)
    after_timestamp = int(after_date.timestamp())

    # Build the search query
    query = f'subject:"{EMAIL_SUBJECT}" after:{after_timestamp}'

    print(f"  Searching for emails with query: {query}")

    try:
        results = service.users().messages().list(
            userId='me',
            q=query,
            maxResults=1
        ).execute()

        messages = results.get('messages', [])

        if not messages:
            print(f"  No emails found matching the criteria")
            return None

        return messages[0]['id']

    except Exception as e:
        print(f"  Error searching emails: {e}")
        return None


def get_email_content(service, message_id):
    """Get the email content and extract the download link"""
    try:
        message = service.users().messages().get(
            userId='me',
            id=message_id,
            format='full'
        ).execute()

        # Get the email body
        payload = message.get('payload', {})
        body_data = None

        # Check for multipart message
        if 'parts' in payload:
            for part in payload['parts']:
                if part.get('mimeType') == 'text/html':
                    body_data = part.get('body', {}).get('data')
                    break
                elif part.get('mimeType') == 'text/plain':
                    body_data = part.get('body', {}).get('data')
        else:
            body_data = payload.get('body', {}).get('data')

        if not body_data:
            print("  Could not extract email body")
            return None

        # Decode the body
        body = base64.urlsafe_b64decode(body_data).decode('utf-8')

        # Extract the download link (Google Ads report link)
        # The link typically looks like: https://c.gle/...
        link_pattern = r'https://c\.gle/[A-Za-z0-9_-]+'
        matches = re.findall(link_pattern, body)

        if matches:
            # Return the first (and likely only) link
            return matches[0]

        # Try alternative pattern for full Google links
        alt_pattern = r'https://[^\s<>"]+googleads[^\s<>"]*'
        alt_matches = re.findall(alt_pattern, body)

        if alt_matches:
            return alt_matches[0]

        print("  Could not find download link in email")
        print(f"  Email body preview: {body[:500]}...")
        return None

    except Exception as e:
        print(f"  Error getting email content: {e}")
        return None


def download_csv_report(url):
    """Download the CSV report from the given URL"""
    try:
        print(f"  Downloading report from: {url[:50]}...")

        # Follow redirects to get the actual CSV
        response = requests.get(url, allow_redirects=True, timeout=60)
        response.raise_for_status()

        # Check if we got a CSV or need to follow more redirects
        content_type = response.headers.get('Content-Type', '')

        if 'text/csv' in content_type or 'application/csv' in content_type:
            return response.text

        # If HTML, look for the actual download link
        if 'text/html' in content_type:
            # Try to find the actual CSV link in the response
            html = response.text
            csv_link_pattern = r'https://[^\s<>"]+\.csv[^\s<>"]*'
            csv_matches = re.findall(csv_link_pattern, html)

            if csv_matches:
                csv_response = requests.get(csv_matches[0], timeout=60)
                csv_response.raise_for_status()
                return csv_response.text

        # If we can't determine the type, try to parse as CSV anyway
        return response.text

    except Exception as e:
        print(f"  Error downloading CSV: {e}")
        return None


def parse_csv_data(csv_content):
    """Parse CSV content into rows for Google Sheets"""
    try:
        reader = csv.reader(io.StringIO(csv_content))
        rows = list(reader)

        if not rows:
            print("  CSV is empty")
            return None

        print(f"  Parsed {len(rows)} rows from CSV")
        return rows

    except Exception as e:
        print(f"  Error parsing CSV: {e}")
        return None


def update_google_sheets(rows):
    """Update Google Sheets with the CSV data"""
    creds = ServiceAccountCredentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SHEETS_SCOPES
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    try:
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        # Clear existing data
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        # Create the worksheet if it doesn't exist
        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=30)

    # Write all data
    if rows:
        worksheet.update(range_name='A1', values=rows)

        # Bold the header row
        sheet_id = worksheet.id
        body = {
            "requests": [
                {"repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold"
                }}
            ]
        }
        spreadsheet.batch_update(body)

    return len(rows) - 1  # Exclude header


def main():
    print(f"[{datetime.now()}] Starting Brand Auction Report sync...")

    # Step 1: Connect to Gmail
    print("  Connecting to Gmail API...")
    try:
        gmail_service = get_gmail_service()
        print("  Gmail connection successful")
    except Exception as e:
        print(f"  ERROR: Could not connect to Gmail: {e}")
        print("  Note: You may need to authorize Gmail access. Run the OAuth flow with gmail.readonly scope.")
        return

    # Step 2: Search for the email
    print("  Searching for report email...")
    message_id = search_email(gmail_service, hours_back=24)

    if not message_id:
        print("  No report email found in the last 24 hours")
        return

    print(f"  Found email with ID: {message_id}")

    # Step 3: Extract download link
    print("  Extracting download link...")
    download_link = get_email_content(gmail_service, message_id)

    if not download_link:
        print("  Could not extract download link from email")
        return

    print(f"  Download link found: {download_link[:60]}...")

    # Step 4: Download the CSV
    print("  Downloading CSV report...")
    csv_content = download_csv_report(download_link)

    if not csv_content:
        print("  Could not download CSV report")
        return

    # Step 5: Parse CSV
    print("  Parsing CSV data...")
    rows = parse_csv_data(csv_content)

    if not rows:
        print("  No data to upload")
        return

    # Step 6: Upload to Google Sheets
    print("  Uploading to Google Sheets...")
    row_count = update_google_sheets(rows)
    print(f"  Updated '{SHEET_NAME}' with {row_count} data rows")

    print(f"[{datetime.now()}] Done!")


if __name__ == "__main__":
    main()
