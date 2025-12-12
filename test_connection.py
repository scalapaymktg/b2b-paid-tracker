import gspread
from google.oauth2.service_account import Credentials

# Configurazione
SPREADSHEET_ID = "1twwTpmJK1hiZVL0NuvpGJFN3G3enkwe510-i8ChwCKg"
CREDENTIALS_FILE = "b2b-paid-tracker-2c1969b03f31.json"

# Scopes necessari per lettura/scrittura
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def main():
    print("Connessione a Google Sheets...")

    # Autenticazione
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)

    # Apri lo spreadsheet
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    print(f"Connesso a: {spreadsheet.title}")

    # Lista fogli esistenti
    print(f"Fogli esistenti: {[ws.title for ws in spreadsheet.worksheets()]}")

    # Crea foglio "Test" se non esiste
    try:
        test_sheet = spreadsheet.worksheet("Test")
        print("Il foglio 'Test' esiste già!")
    except gspread.exceptions.WorksheetNotFound:
        test_sheet = spreadsheet.add_worksheet(title="Test", rows=100, cols=20)
        print("Foglio 'Test' creato con successo!")

    # Scrivi qualcosa per verificare
    test_sheet.update_cell(1, 1, "Connessione OK!")
    test_sheet.update_cell(1, 2, "Scrittura funzionante!")
    print("Test completato! Controlla il foglio 'Test' nel tuo Google Sheet.")

if __name__ == "__main__":
    main()
