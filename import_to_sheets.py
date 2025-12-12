"""
B2B Paid Tracker - Import CSV/XLSX to Google Sheets
Legge file da una cartella e li scrive nei rispettivi fogli Google Sheets.
"""

import os
import glob
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

# === CONFIGURAZIONE ===

# Spreadsheet ID
SPREADSHEET_ID = "1twwTpmJK1hiZVL0NuvpGJFN3G3enkwe510-i8ChwCKg"

# Path al file credenziali (stesso folder dello script)
CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "b2b-paid-tracker-2c1969b03f31.json")

# Scopes per Google Sheets API
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Mapping: chiave di ricerca (contains) -> configurazione
# - sheet_name: nome del foglio in Google Sheets
# - max_col: ultima colonna da scrivere (es. "P" = 16 colonne)
# - skip_rows: righe da saltare nel file sorgente (metadata)
# - replace_comma: se True, sostituisce "," con "." nei valori numerici
FILE_MAPPING = {
    "qualified-pipeline": {
        "sheet_name": "qualified-pipeline-weekly-pai",
        "max_col": "P",  # 16 colonne
        "skip_rows": 0,
        "replace_comma": False,
        "subfolder_pattern": "hubspot-custom-report-qualified-pipeline-weekly-pai-*"  # cerca in sottocartella
    },
    "Ad-Weekly": {
        "sheet_name": "Ad-Weekly-B2B-Spend-and-Performanc-per-Ad",
        "max_col": "Y",  # 25 colonne
        "skip_rows": 0,
        "replace_comma": False
    },
    "All-time": {
        "sheet_name": "All-time-Spend",
        "max_col": "O",  # 15 colonne
        "skip_rows": 0,
        "replace_comma": False
    },
    "Google": {
        "sheet_name": "Google - Weekly Spend and Performance",
        "max_col": "K",  # 11 colonne
        "skip_rows": 2,  # 2 righe metadata
        "replace_comma": True
    },
    "Brand Auction": {
        "sheet_name": "B2B Brand Auction Insight",
        "max_col": "L",  # 12 colonne
        "skip_rows": 2,  # 2 righe metadata
        "replace_comma": True
    }
}


def col_letter_to_num(letter: str) -> int:
    """Converte lettera colonna in numero (A=1, B=2, ..., Z=26, AA=27, ...)"""
    result = 0
    for char in letter.upper():
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result


def find_matching_config(filename: str) -> tuple:
    """
    Trova la configurazione corrispondente al file basandosi su logica 'contains'.
    Ritorna (chiave, config) o (None, None) se non trovato.
    """
    for key, config in FILE_MAPPING.items():
        if key.lower() in filename.lower():
            return key, config
    return None, None


def read_file(filepath: str, skip_rows: int = 0) -> pd.DataFrame:
    """
    Legge un file CSV o XLSX e ritorna un DataFrame.
    - skip_rows: numero di righe da saltare all'inizio (metadata)
    """
    ext = os.path.splitext(filepath)[1].lower()

    if ext == ".csv":
        # Legge CSV saltando le righe metadata
        df = pd.read_csv(filepath, skiprows=skip_rows, dtype=str)
    elif ext in [".xlsx", ".xls"]:
        # Legge Excel saltando le righe metadata
        df = pd.read_excel(filepath, skiprows=skip_rows, dtype=str)
    else:
        raise ValueError(f"Formato file non supportato: {ext}")

    return df


def apply_comma_replacement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sostituisce le virgole con i punti nei valori numerici.
    Utile per convertire formato numerico europeo (1.234,56) in formato standard (1234.56).
    """
    def replace_comma(val):
        if pd.isna(val):
            return val
        val_str = str(val)
        # Se contiene sia punto che virgola (es. 1.234,56), rimuovi i punti e sostituisci virgola
        if '.' in val_str and ',' in val_str:
            return val_str.replace('.', '').replace(',', '.')
        # Se contiene solo virgola, sostituiscila con punto
        elif ',' in val_str:
            return val_str.replace(',', '.')
        return val_str

    return df.map(replace_comma)


def prepare_data_for_sheets(df: pd.DataFrame, max_col: str) -> list:
    """
    Prepara i dati per Google Sheets.
    - Limita alle colonne specificate
    - Converte NaN in stringa vuota
    - Ritorna lista di liste (righe)
    """
    max_col_num = col_letter_to_num(max_col)

    # Limita alle colonne specificate
    df_limited = df.iloc[:, :max_col_num]

    # Sostituisci NaN con stringa vuota
    df_limited = df_limited.fillna("")

    # Converti in lista di liste
    data = df_limited.values.tolist()

    return data


def write_to_sheet(spreadsheet, sheet_name: str, data: list, max_col: str):
    """
    Scrive i dati nel foglio specificato.
    - Preserva la riga 1 (header) del foglio
    - Sovrascrive dalla riga 2 in poi
    - Pulisce le righe extra se i nuovi dati sono meno
    """
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  ERRORE: Foglio '{sheet_name}' non trovato!")
        return False

    if not data:
        print(f"  ATTENZIONE: Nessun dato da scrivere per '{sheet_name}'")
        return False

    max_col_num = col_letter_to_num(max_col)

    # Range per i dati (dalla riga 2, colonna A fino a max_col)
    start_row = 2
    end_row = start_row + len(data) - 1
    range_notation = f"A{start_row}:{max_col}{end_row}"

    # Scrivi i dati
    worksheet.update(range_notation, data, value_input_option='RAW')

    # Pulisci eventuali righe extra (se i dati precedenti erano di più)
    # Ottieni il numero totale di righe attuali nel foglio
    current_rows = worksheet.row_count
    if end_row < current_rows:
        # Pulisci dalla riga successiva all'ultima scritta fino alla fine dei dati precedenti
        # Limitiamo a 10000 righe per sicurezza
        clear_end = min(current_rows, 10000)
        if end_row + 1 <= clear_end:
            clear_range = f"A{end_row + 1}:{max_col}{clear_end}"
            worksheet.batch_clear([clear_range])

    print(f"  Scritte {len(data)} righe nel range {range_notation}")
    return True


def process_files(source_folder: str, dry_run: bool = False):
    """
    Processa tutti i file CSV/XLSX nella cartella sorgente.

    Args:
        source_folder: cartella contenente i file da importare
        dry_run: se True, mostra solo cosa farebbe senza scrivere
    """
    # Trova tutti i file CSV e XLSX nella cartella principale
    csv_files = glob.glob(os.path.join(source_folder, "*.csv"))
    xlsx_files = glob.glob(os.path.join(source_folder, "*.xlsx"))
    xls_files = glob.glob(os.path.join(source_folder, "*.xls"))

    all_files = csv_files + xlsx_files + xls_files

    # Cerca anche nelle sottocartelle specificate nel mapping
    for key, config in FILE_MAPPING.items():
        if "subfolder_pattern" in config:
            # Cerca sottocartelle che matchano il pattern
            subfolders = glob.glob(os.path.join(source_folder, config["subfolder_pattern"]))
            for subfolder in subfolders:
                if os.path.isdir(subfolder):
                    # Cerca file CSV/XLSX nella sottocartella
                    sub_csv = glob.glob(os.path.join(subfolder, "*.csv"))
                    sub_xlsx = glob.glob(os.path.join(subfolder, "*.xlsx"))
                    sub_xls = glob.glob(os.path.join(subfolder, "*.xls"))
                    all_files.extend(sub_csv + sub_xlsx + sub_xls)

    if not all_files:
        print(f"Nessun file CSV/XLSX trovato in: {source_folder}")
        return

    print(f"Trovati {len(all_files)} file in {source_folder}")
    print("-" * 60)

    # Connessione a Google Sheets (solo se non dry_run)
    spreadsheet = None
    if not dry_run:
        credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        print(f"Connesso a: {spreadsheet.title}")
        print("-" * 60)

    # Processa ogni file
    processed = 0
    skipped = 0

    for filepath in all_files:
        filename = os.path.basename(filepath)
        key, config = find_matching_config(filename)

        if config is None:
            print(f"SKIP: {filename} (nessun mapping trovato)")
            skipped += 1
            continue

        print(f"\nProcesso: {filename}")
        print(f"  -> Foglio: {config['sheet_name']}")
        print(f"  -> Colonne: A-{config['max_col']}")

        if dry_run:
            print("  [DRY RUN - nessuna scrittura]")
            processed += 1
            continue

        try:
            # Leggi il file
            df = read_file(filepath, skip_rows=config['skip_rows'])
            print(f"  Lette {len(df)} righe dal file")

            # Applica sostituzione virgole se necessario
            if config['replace_comma']:
                df = apply_comma_replacement(df)
                print("  Applicata sostituzione , -> .")

            # Prepara i dati
            data = prepare_data_for_sheets(df, config['max_col'])

            # Scrivi nel foglio
            success = write_to_sheet(spreadsheet, config['sheet_name'], data, config['max_col'])

            if success:
                processed += 1
            else:
                skipped += 1

        except Exception as e:
            print(f"  ERRORE: {e}")
            skipped += 1

    print("\n" + "=" * 60)
    print(f"Completato: {processed} file processati, {skipped} saltati")


def main():
    """Entry point principale."""
    import argparse

    parser = argparse.ArgumentParser(description="Importa CSV/XLSX in Google Sheets")
    parser.add_argument(
        "source_folder",
        help="Cartella contenente i file CSV/XLSX da importare"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra cosa farebbe senza scrivere effettivamente"
    )

    args = parser.parse_args()

    # Verifica che la cartella esista
    if not os.path.isdir(args.source_folder):
        print(f"Errore: '{args.source_folder}' non è una cartella valida")
        return 1

    # Verifica che il file credenziali esista
    if not os.path.isfile(CREDENTIALS_FILE):
        print(f"Errore: File credenziali non trovato: {CREDENTIALS_FILE}")
        return 1

    process_files(args.source_folder, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    exit(main())
