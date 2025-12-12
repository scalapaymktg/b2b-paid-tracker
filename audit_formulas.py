import gspread
from google.oauth2.service_account import Credentials
from collections import Counter
import re

SPREADSHEET_ID = "1twwTpmJK1hiZVL0NuvpGJFN3G3enkwe510-i8ChwCKg"
CREDENTIALS_FILE = "b2b-paid-tracker-2c1969b03f31.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Formule note per essere pesanti
HEAVY_FORMULAS = [
    'IMPORTRANGE', 'QUERY', 'ARRAYFORMULA', 'VLOOKUP', 'HLOOKUP',
    'INDEX', 'MATCH', 'INDIRECT', 'OFFSET', 'SUMIF', 'SUMIFS',
    'COUNTIF', 'COUNTIFS', 'AVERAGEIF', 'AVERAGEIFS', 'FILTER',
    'SORT', 'UNIQUE', 'NOW', 'TODAY', 'RAND', 'RANDBETWEEN',
    'GOOGLEFINANCE', 'IMAGE', 'IMPORTXML', 'IMPORTHTML', 'IMPORTDATA',
    'REGEXMATCH', 'REGEXEXTRACT', 'REGEXREPLACE'
]

def extract_functions(formula):
    """Estrae tutte le funzioni usate in una formula"""
    if not formula or not formula.startswith('='):
        return []
    # Trova tutte le parole seguite da (
    functions = re.findall(r'([A-Z_]+)\s*\(', formula.upper())
    return functions

def main():
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    print(f"{'='*70}")
    print(f"AUDIT FORMULE: {spreadsheet.title}")
    print(f"{'='*70}\n")

    all_heavy_formulas = []
    global_function_count = Counter()

    for ws in spreadsheet.worksheets():
        print(f"\n{'─'*70}")
        print(f"Foglio: {ws.title}")
        print(f"{'─'*70}")

        try:
            # Ottieni tutte le formule
            formulas = ws.get(value_render_option='FORMULA')

            formula_count = 0
            function_count = Counter()
            heavy_examples = []

            for row_idx, row in enumerate(formulas, 1):
                for col_idx, cell in enumerate(row, 1):
                    if cell and str(cell).startswith('='):
                        formula_count += 1
                        functions = extract_functions(cell)
                        function_count.update(functions)
                        global_function_count.update(functions)

                        # Salva esempi di formule pesanti
                        for func in functions:
                            if func in HEAVY_FORMULAS:
                                col_letter = gspread.utils.rowcol_to_a1(row_idx, col_idx).rstrip('0123456789')
                                all_heavy_formulas.append({
                                    'sheet': ws.title,
                                    'cell': gspread.utils.rowcol_to_a1(row_idx, col_idx),
                                    'function': func,
                                    'formula': cell[:100] + ('...' if len(cell) > 100 else '')
                                })

            print(f"Totale formule: {formula_count}")

            if function_count:
                print(f"\nFunzioni usate:")
                for func, count in function_count.most_common(15):
                    marker = " ⚠️  PESANTE" if func in HEAVY_FORMULAS else ""
                    print(f"  {func:<20} {count:>5}x{marker}")

        except Exception as e:
            print(f"Errore: {e}")

    # Riepilogo globale
    print(f"\n{'='*70}")
    print("RIEPILOGO GLOBALE - FORMULE PESANTI")
    print(f"{'='*70}")

    heavy_count = Counter()
    for item in all_heavy_formulas:
        heavy_count[item['function']] += 1

    if heavy_count:
        print(f"\n{'Funzione':<20} {'Occorrenze':<15} {'Impatto'}")
        print("-" * 60)
        for func, count in heavy_count.most_common():
            if func in ['IMPORTRANGE', 'QUERY', 'GOOGLEFINANCE', 'IMPORTXML', 'IMPORTHTML', 'IMPORTDATA']:
                impact = "🔴 ALTO - chiamate esterne/complesse"
            elif func in ['NOW', 'TODAY', 'RAND', 'RANDBETWEEN']:
                impact = "🟠 MEDIO - ricalcolo continuo"
            elif func in ['ARRAYFORMULA', 'FILTER', 'SORT', 'UNIQUE']:
                impact = "🟡 MEDIO - elabora array"
            elif func in ['VLOOKUP', 'HLOOKUP', 'INDEX', 'MATCH', 'INDIRECT', 'OFFSET']:
                impact = "🟡 MEDIO - lookup su range"
            else:
                impact = "🟢 BASSO"
            print(f"{func:<20} {count:<15} {impact}")

    # Esempi delle formule più critiche
    print(f"\n{'='*70}")
    print("ESEMPI FORMULE CRITICHE (prime 20)")
    print(f"{'='*70}")

    critical_order = ['IMPORTRANGE', 'QUERY', 'ARRAYFORMULA', 'NOW', 'TODAY', 'INDIRECT', 'OFFSET']
    shown = 0
    for priority_func in critical_order:
        for item in all_heavy_formulas:
            if item['function'] == priority_func and shown < 20:
                print(f"\n[{item['sheet']}!{item['cell']}] {item['function']}")
                print(f"  {item['formula']}")
                shown += 1

    # Se non abbiamo riempito con le prioritarie, aggiungi altre
    if shown < 20:
        for item in all_heavy_formulas:
            if item['function'] not in critical_order and shown < 20:
                print(f"\n[{item['sheet']}!{item['cell']}] {item['function']}")
                print(f"  {item['formula']}")
                shown += 1

    # Suggerimenti finali
    print(f"\n{'='*70}")
    print("SUGGERIMENTI")
    print(f"{'='*70}")

    if 'IMPORTRANGE' in heavy_count:
        print(f"\n⚠️  IMPORTRANGE ({heavy_count['IMPORTRANGE']}x): Considera di copiare i dati una volta invece di linkarli")
    if 'QUERY' in heavy_count:
        print(f"\n⚠️  QUERY ({heavy_count['QUERY']}x): Molto potente ma lenta. Valuta di pre-elaborare i dati")
    if 'NOW' in heavy_count or 'TODAY' in heavy_count:
        total_volatile = heavy_count.get('NOW', 0) + heavy_count.get('TODAY', 0)
        print(f"\n⚠️  NOW/TODAY ({total_volatile}x): Causano ricalcolo continuo. Usa una cella singola e riferiscila")
    if 'ARRAYFORMULA' in heavy_count:
        print(f"\n⚠️  ARRAYFORMULA ({heavy_count['ARRAYFORMULA']}x): Su range grandi può rallentare molto")
    if 'INDIRECT' in heavy_count or 'OFFSET' in heavy_count:
        total = heavy_count.get('INDIRECT', 0) + heavy_count.get('OFFSET', 0)
        print(f"\n⚠️  INDIRECT/OFFSET ({total}x): Impediscono ottimizzazione. Usa riferimenti diretti se possibile")

if __name__ == "__main__":
    main()
