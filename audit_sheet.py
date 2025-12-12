import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = "1twwTpmJK1hiZVL0NuvpGJFN3G3enkwe510-i8ChwCKg"
CREDENTIALS_FILE = "b2b-paid-tracker-2c1969b03f31.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def main():
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    print(f"{'='*60}")
    print(f"AUDIT: {spreadsheet.title}")
    print(f"{'='*60}\n")

    # Elimina foglio Test se esiste
    try:
        test_sheet = spreadsheet.worksheet("Test")
        spreadsheet.del_worksheet(test_sheet)
        print("Foglio 'Test' eliminato.\n")
    except gspread.exceptions.WorksheetNotFound:
        print("Foglio 'Test' non trovato (già eliminato).\n")

    # Audit di tutti i fogli
    total_cells = 0
    total_data_cells = 0
    sheets_info = []

    print(f"{'Foglio':<20} {'Righe':<10} {'Colonne':<10} {'Celle Tot':<15} {'Con Dati':<15} {'% Uso':<10}")
    print("-" * 80)

    for ws in spreadsheet.worksheets():
        rows = ws.row_count
        cols = ws.col_count
        cells = rows * cols
        total_cells += cells

        # Conta celle con dati (può essere lento per fogli grandi)
        try:
            all_values = ws.get_all_values()
            data_rows = len(all_values)
            data_cols = max(len(row) for row in all_values) if all_values else 0
            non_empty = sum(1 for row in all_values for cell in row if cell.strip())
            total_data_cells += non_empty
            usage = (non_empty / cells * 100) if cells > 0 else 0

            sheets_info.append({
                'name': ws.title,
                'rows': rows,
                'cols': cols,
                'cells': cells,
                'data_rows': data_rows,
                'data_cols': data_cols,
                'non_empty': non_empty,
                'usage': usage
            })

            print(f"{ws.title:<20} {rows:<10} {cols:<10} {cells:<15,} {non_empty:<15,} {usage:<10.2f}%")
        except Exception as e:
            print(f"{ws.title:<20} {rows:<10} {cols:<10} {cells:<15,} {'ERRORE':<15} {str(e)[:20]}")

    print("-" * 80)
    print(f"\n{'='*60}")
    print("RIEPILOGO")
    print(f"{'='*60}")
    print(f"Celle totali allocate:     {total_cells:>15,}")
    print(f"Celle con dati:            {total_data_cells:>15,}")
    print(f"Limite Google Sheets:      {10_000_000:>15,}")
    print(f"Utilizzo del limite:       {total_cells/10_000_000*100:>14.2f}%")
    print(f"Spazio rimanente:          {10_000_000 - total_cells:>15,} celle")

    # Suggerimenti
    print(f"\n{'='*60}")
    print("SUGGERIMENTI PER OTTIMIZZAZIONE")
    print(f"{'='*60}")

    for s in sheets_info:
        if s['usage'] < 1 and s['cells'] > 100000:
            print(f"- '{s['name']}': {s['cells']:,} celle allocate ma solo {s['usage']:.2f}% usate")
            print(f"   Righe effettive: {s['data_rows']}, Colonne effettive: {s['data_cols']}")
            print(f"   Potresti ridurre a ~{s['data_rows']+100} righe x {s['data_cols']+5} colonne")

        if s['rows'] > 50000:
            print(f"- '{s['name']}': Ha {s['rows']:,} righe - potrebbe rallentare le formule")

if __name__ == "__main__":
    main()
