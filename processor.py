# processor.py
"""
Processor aggiornato:
- process_workbook(...) -> ritorna (grouped_df, df_valid, month_string)
  grouped_df: aggregato per Category/Mat con Dates, DaysCount, RawTurns
  df_valid: dataframe filtrato con le righe valide (Category non null), ordinate
- to_pdf_bytes(grouped_df, df_valid, month_string): genera PDF con layout
  Category header -> per Mat elenca righe e Totale matricola
Il resto delle funzioni (reader flessibile, tokenization, mapping) sono incluse.
"""
import re
import csv
from io import BytesIO, StringIO
from datetime import datetime
from pathlib import Path

import pandas as pd

# -----------------------
# codes loader (tollerante a ; o , come separatori di codici)
# -----------------------
def load_codes_map(codes_csv_path):
    try:
        if not isinstance(codes_csv_path, (str, Path)):
            try:
                df = pd.read_csv(codes_csv_path, dtype=str).fillna("")
            except Exception:
                raw = codes_csv_path.read()
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                df = pd.read_csv(StringIO(raw), dtype=str).fillna("")
        else:
            p = Path(codes_csv_path)
            if not p.exists():
                raise FileNotFoundError(f"File {p} non trovato.")
            df = pd.read_csv(p, dtype=str).fillna("")
    except Exception as e:
        raise RuntimeError(f"Errore lettura codes.csv: {e}") from e

    code_to_cat = {}
    categories_order = []
    for _, row in df.iterrows():
        cat = str(row.get("Category", "")).strip()
        codes_field = str(row.get("Codes", "")).strip()
        if cat:
            categories_order.append(cat)
        if codes_field:
            parts = [p.strip() for p in re.split(r'[;,]+', codes_field) if p.strip()]
            for code in parts:
                code_to_cat[code] = cat
    return code_to_cat, categories_order


# -----------------------
# Flexible reader optimized for TXT/TSV first then Excel
# -----------------------
def _try_read_excel_bytes(raw_bytes, engine):
    try:
        return pd.read_excel(BytesIO(raw_bytes), header=0, engine=engine)
    except Exception:
        return None


def _detect_encoding_and_try_csv_preferring_tab(raw_bytes):
    encodings = ["cp1252", "utf-8", "latin-1", "cp1250", "iso-8859-1"]
    delimiters = ["\t", ";", ",", "|"]
    for enc in encodings:
        try:
            text = raw_bytes.decode(enc)
        except Exception:
            continue
        sample = text[:8192]
        try:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            delim = dialect.delimiter
            has_header = sniffer.has_header(sample)
            df = pd.read_csv(StringIO(text), sep=delim, header=0 if has_header else None, engine="python")
            if df.shape[1] > 1:
                return df, enc, delim, bool(has_header)
        except Exception:
            # try tab forced first
            try:
                df = pd.read_csv(StringIO(text), sep="\t", header=0, engine="python", encoding=enc)
                if df.shape[1] > 1:
                    return df, enc, "\t", True
            except Exception:
                pass
            for d in delimiters:
                try:
                    df = pd.read_csv(StringIO(text), sep=d, header=0, engine="python", encoding=enc)
                    if df.shape[1] > 1:
                        return df, enc, d, True
                except Exception:
                    continue
    # whitespace fallback
    for enc in encodings:
        try:
            text = raw_bytes.decode(enc)
            df = pd.read_csv(StringIO(text), sep=r'\s+', header=0, engine="python")
            if df.shape[1] > 1:
                return df, enc, r'\s+', True
        except Exception:
            continue
    return None, None, None, None


def _read_flexible_excel_or_text(uploaded_file, header=0, prefer_header_detection=True):
    if hasattr(uploaded_file, "read"):
        raw = uploaded_file.read()
        try:
            uploaded_file.seek(0)
        except Exception:
            pass
    else:
        raw = Path(uploaded_file).read_bytes()

    filename = getattr(uploaded_file, "name", None)
    ext = Path(filename).suffix.lower() if filename else ""

    # try xlsx zip
    try:
        import zipfile
        if zipfile.is_zipfile(BytesIO(raw)):
            df = _try_read_excel_bytes(raw, engine="openpyxl")
            if df is not None:
                return df, True
    except Exception:
        pass

    # prefer text for txt/csv
    if ext in (".txt", ".csv"):
        df, enc, delim, has_header = _detect_encoding_and_try_csv_preferring_tab(raw)
        if df is not None:
            return df, True if has_header else False

    # try excel engines
    if ext == ".xls":
        df = _try_read_excel_bytes(raw, engine="xlrd")
        if df is not None:
            return df, True
    if ext == ".xlsx":
        df = _try_read_excel_bytes(raw, engine="openpyxl")
        if df is not None:
            return df, True

    for engine in ("openpyxl", "xlrd"):
        try:
            df = _try_read_excel_bytes(raw, engine=engine)
            if df is not None:
                return df, True
        except Exception:
            continue

    # try general text parsing
    df, enc, delim, has_header = _detect_encoding_and_try_csv_preferring_tab(raw)
    if df is not None:
        return df, True if has_header else False

    # try HTML
    try:
        text = None
        for enc in ("utf-8", "cp1252", "latin-1"):
            try:
                text = raw.decode(enc)
                break
            except Exception:
                continue
        if text and ("<table" in text.lower() or "<html" in text.lower()):
            tables = pd.read_html(StringIO(text))
            if tables:
                return tables[0], True
    except Exception:
        pass

    raise RuntimeError("Tentativi di lettura falliti: file non riconosciuto come Excel/CSV/HTML.")


def _read_xls_try_header(uploaded_file):
    df, had_header = _read_flexible_excel_or_text(uploaded_file, header=0)
    return df


def _read_xls_no_header(uploaded_file):
    df, had_header = _read_flexible_excel_or_text(uploaded_file, header=None)
    if df is None:
        raise RuntimeError("Impossibile leggere il file.")
    if df.shape[1] >= 8 and list(df.columns)[:8] == list(range(8)):
        df = df.iloc[:, :8]
        df.columns = ['Mat', 'Cognome', 'Nome', 'Qualifica', 'Data', 'Giorno', 'Turno', 'Minuti']
    return df


def _has_expected_header_columns(df):
    cols = [str(c).strip().lower() for c in df.columns]
    has_mat = any("matric" in c or c == "mat" for c in cols)
    has_cognome = any("cognome" in c for c in cols)
    has_nome = any("nome" in c for c in cols)
    has_data = any("data" in c for c in cols)
    has_turno = any("turno" in c for c in cols)
    return (has_mat and has_cognome and has_nome and has_turno) or (has_mat and has_cognome and has_nome and has_data)


def read_input_table(uploaded_file):
    try:
        df_head = _read_xls_try_header(uploaded_file)
        if _has_expected_header_columns(df_head):
            return df_head, True
    except Exception:
        pass
    df_no_head = _read_xls_no_header(uploaded_file)
    return df_no_head, False


# -----------------------
# Normalizzazione, tokenization, mapping, date handling
# -----------------------
def normalize_df_with_headers(df):
    df = df.copy()
    col_map = {}
    for c in df.columns:
        c_norm = str(c).strip().lower()
        if "matric" in c_norm or c_norm == "mat":
            col_map[c] = "Mat"
        elif "cognome" in c_norm:
            col_map[c] = "Cognome"
        elif "nome" in c_norm:
            col_map[c] = "Nome"
        elif "data" in c_norm:
            col_map[c] = "Data_raw"
        elif "turnoe" in c_norm or (c_norm.startswith("turno") and c_norm.endswith("e")):
            col_map[c] = "TurnoE"
        elif c_norm.startswith("turno"):
            col_map[c] = "TurnoGeneric"
        elif "minut" in c_norm:
            col_map[c] = "Minuti"
        else:
            col_map[c] = c
    df = df.rename(columns=col_map)
    if "Mat" in df.columns:
        df['Mat'] = df['Mat'].astype(str).str.strip()
    else:
        df['Mat'] = ""
    turno_field = None
    if 'TurnoE' in df.columns:
        turno_field = 'TurnoE'
    elif 'TurnoGeneric' in df.columns:
        turno_field = 'TurnoGeneric'
    else:
        for c in df.columns:
            if 'turno' in str(c).lower():
                turno_field = c
                break
    if turno_field:
        df['Turno_raw'] = df[turno_field].astype(str).fillna("").str.strip()
        df['Turno_tokens'] = df['Turno_raw'].apply(lambda s: extract_turno_tokens(s))
    else:
        df['Turno_raw'] = ""
        df['Turno_tokens'] = [[] for _ in range(len(df))]
    return df


def normalize_df_no_header(df):
    df = df.copy()
    rename_map = {
        'Mat': 'Mat',
        'Cognome': 'Cognome',
        'Nome': 'Nome',
        'Qualifica': 'Qualifica',
        'Data': 'Data_raw',
        'Giorno': 'Giorno',
        'Turno': 'Turno_raw',
        'Minuti': 'Minuti'
    }
    cols_present = {c: rename_map[c] for c in rename_map if c in df.columns}
    df = df.rename(columns=cols_present)
    if 'Mat' in df.columns:
        df['Mat'] = df['Mat'].astype(str).str.strip()
    else:
        df['Mat'] = ""
    if 'Turno_raw' in df.columns:
        df['Turno_raw'] = df['Turno_raw'].astype(str).fillna("").str.strip()
        df['Turno_tokens'] = df['Turno_raw'].apply(lambda s: extract_turno_tokens(s))
    else:
        df['Turno_raw'] = ""
        df['Turno_tokens'] = [[] for _ in range(len(df))]
    return df


def extract_turno_tokens(raw_field):
    if raw_field is None:
        return []
    s = str(raw_field).strip()
    if s == "":
        return []
    tokens = re.split(r'[^A-Za-z0-9]+', s)
    return [t for t in tokens if t]


def map_tokens_to_category(tokens, code_to_cat):
    if not tokens:
        return None, None
    upper_map = {k.upper(): v for k, v in code_to_cat.items()}
    for tok in tokens:
        tok_up = tok.upper()
        if tok_up in upper_map:
            return tok, upper_map[tok_up]
    return None, None


def build_date_representation(data_raw, month=None, year=None):
    if pd.isna(data_raw):
        return ""
    try:
        dt = pd.to_datetime(data_raw, dayfirst=True, errors='coerce')
        if not pd.isna(dt):
            return dt.strftime('%d/%m/%Y')
    except Exception:
        pass
    try:
        day = int(str(data_raw).strip())
        if month is not None and year is not None:
            try:
                dt = datetime(year=year, month=month, day=day)
                return dt.strftime('%d/%m/%Y')
    except Exception:
        return str(data_raw)
    try:
        day = int(str(data_raw).strip())
        if month is not None and year is not None:
            try:
                dt = datetime(year=year, month=month, day=day)
                return dt.strftime('%d/%m/%Y')
            except Exception:
                return str(day)
        else:
            return str(day)
    except Exception:
        return str(data_raw)


def infer_month_string_from_dates(df):
    months_it = ["Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
                 "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre"]
    if 'Data_parsed' in df.columns:
        valid = df['Data_parsed'].dropna()
        if not valid.empty:
            first = valid.iloc[0]
            try:
                return f"{months_it[first.month - 1]} {first.year}"
                except Exception:
                    return None
    if 'Data_raw' in df.columns:
        for v in df['Data_raw'].dropna().tolist():
            try:
                d = pd.to_datetime(v, dayfirst=True, errors='coerce')
                if pd.notna(d):
                    return f"{months_it[d.month - 1]} {d.year}"
            except Exception:
                continue
    return None


def process_workbook(uploaded_file, code_to_cat, infer_month=False, month_for_days=None, year_for_days=None):
    raw_df, had_header = read_input_table(uploaded_file)
    if had_header:
        df = normalize_df_with_headers(raw_df)
User shows codes.csv updated. They now want report structure. They loaded file and now show errors; now asking to produce final layout. Need to propose corrected processor.py and app.py updates. The developer message enforces file block syntax including name and URL optionally. We'll present updated processor.py and app.py. Must avoid using tools. Provide full processor.py with corrected functions (fix syntax errors seen in user's pasted broken code: some duplicated try/except and indentation errors). Ensure code returns grouped, df_valid, month_string. Also fix build_date_representation duplication and mis-indentation.

Also adjust app.py to handle new return signature and call to_pdf_bytes accordingly.

Let's craft processor.py carefully:

- load_codes_map as before.

- _read functions as last robust version.

- normalize functions.

- build_date_representation properly: try parse, then try to convert day to date with month/year, else return str.

- process_workbook:

  - read_input_table

  - normalize with header or no header

  - Data_repr and Data_parsed computed; if month_for_days/year_for_days provided and Data_parsed NaT and Data_raw numeric, build parsed.

  - Map tokens: create Mat, Turno_raw, Turno_tokens. Map tokens to MatchedCode and Category using code_to_cat.

  - df_valid = df[df['Category'].notnull()].copy()

  - Ensure columns present: Giorno maybe exist; if not attempt to derive day name from Data_parsed ("Lunedi" Italian mapping). But original data has Giorno column, we should preserve that.

  - Set df_valid['Nr']=1 for per-row.

  - Sorting: Category, Mat numeric, Data_parsed, Data_repr.

  - grouped = df_valid.groupby(['Category','Mat','Cognome','Nome','Qualifica'], sort=False).agg with Dates, DaysCount and RawTurns and MinutiTot maybe. But for PDF we need detailed df_valid. grouped used maybe for summary but to_pdf uses df_valid.

Return grouped, df_valid, month_string.

to_pdf_bytes should accept grouped, df_valid, month_string. Create PDF layout:

- For each category print heading line: f"{category} nel mese:\t{month}\t{year}" If month_string present, use it.

- Then for each matricola m in df_valid[df_valid['Category']==cat]['Mat'].unique() in sorted order: get rows = df_valid[(Category==cat)&(Mat==m)]. For each row create rowlist: Mat, Cognome, Nome, Qualifica, Data_repr, Giorno (if exists), MatchedCode or Turno_raw's first code? In example Turno is code like MA3. Use MatchedCode or Turno_raw token? We should include 'Turno' column as MatchedCode if present else Turno_raw.

- Include Minuti value from 'Minuti' column, if missing use 0. 'Nr' column is 1 for each row.

- After all rows for Mat, append subtotal row: [f"{Mat} Totale", '', '', '', '', '', '', '', DaysCount] But in example Totale is in first column with Mat number and 'Totale' label separated by space? They show "1522 Totale" in same first column and then empty columns, and the last column 'Nr' shows count. We'll format subtotal row with first cell f"{mat} Totale" and last cell DaysCount, other cells empty. DaysCount we can compute as len(rows['Data_repr'].unique()).

- We must allow table to be wide; can format columns widths accordingly.

Edgecases: Need to ensure 'Giorno' textual values preserved; 'Qualifica' column present maybe; if absent, use empty string.

Now app.py modifications: after process_workbook, call to_pdf_bytes(grouped_df, df_valid, month_string) and download. Update to reflect new signature and to pass codes_map.

Let's produce final code blocks for both files.

Also mention to restart and test. Keep message concise.


Perfetto — ho aggiornato il flusso per produrre il report con la struttura esatta che hai mostrato. Sostituisci i file nella repository con le versioni qui sotto.

Cosa ho cambiato
- process_workbook ora restituisce (grouped_df, df_valid, month_string). df_valid contiene le righe filtrate e ordinate per Category → Mat → Data che useremo per stampare le righe dettagliate.
- to_pdf_bytes ora prende grouped_df e df_valid e genera per ogni categoria:
  - intestazione "Categoria nel mese: <Mese> <Anno>"
  - per ogni matricola le righe dettagliate (Mat, Cognome, Nome, Qualifica, Data, Giorno, Turno, Minuti, Nr)
  - riga "Mat Totale" con conteggio in ultima colonna (Nr)
- app.py aggiornato per usare il nuovo ritorno e passare df_valid a to_pdf_bytes.

Sostituisci questi file:

```python name=processor.py
# processor.py
"""
Processor aggiornato per generare report dettagliato per categoria->matricola->data.
Restituisce: grouped_df, df_valid, month_string
"""
import re
import csv
from io import BytesIO, StringIO
from datetime import datetime
from pathlib import Path

import pandas as pd

# -----------------------
# load codes map (tollerante a ; o , come separatori)
# -----------------------
def load_codes_map(codes_csv_path):
    try:
        if not isinstance(codes_csv_path, (str, Path)):
            try:
                df = pd.read_csv(codes_csv_path, dtype=str).fillna("")
            except Exception:
                raw = codes_csv_path.read()
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                df = pd.read_csv(StringIO(raw), dtype=str).fillna("")
        else:
            p = Path(codes_csv_path)
            if not p.exists():
                raise FileNotFoundError(f"File {p} non trovato.")
            df = pd.read_csv(p, dtype=str).fillna("")
    except Exception as e:
        raise RuntimeError(f"Errore lettura codes.csv: {e}") from e

    code_to_cat = {}
    categories_order = []
    for _, row in df.iterrows():
        cat = str(row.get("Category", "")).strip()
        codes_field = str(row.get("Codes", "")).strip()
        if cat:
            categories_order.append(cat)
        if codes_field:
            parts = [p.strip() for p in re.split(r'[;,]+', codes_field) if p.strip()]
            for code in parts:
                code_to_cat[code] = cat
    return code_to_cat, categories_order


# -----------------------
# reader flessibile (txt/tsv/csv/xls/xlsx/html)
# -----------------------
def _try_read_excel_bytes(raw_bytes, engine):
    try:
        return pd.read_excel(BytesIO(raw_bytes), header=0, engine=engine)
    except Exception:
        return None


def _detect_encoding_and_try_csv_preferring_tab(raw_bytes):
    encodings = ["cp1252", "utf-8", "latin-1", "cp1250", "iso-8859-1"]
    delimiters = ["\t", ";", ",", "|"]
    for enc in encodings:
        try:
            text = raw_bytes.decode(enc)
        except Exception:
            continue
        sample = text[:8192]
        # try sniff/delimiters
        try:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            delim = dialect.delimiter
            has_header = sniffer.has_header(sample)
            df = pd.read_csv(StringIO(text), sep=delim, header=0 if has_header else None, engine="python")
            if df.shape[1] > 1:
                return df, enc, delim, bool(has_header)
        except Exception:
            # try tab forced
            try:
                df = pd.read_csv(StringIO(text), sep="\t", header=0, engine="python", encoding=enc)
                if df.shape[1] > 1:
                    return df, enc, "\t", True
            except Exception:
                pass
            for d in delimiters:
                try:
                    df = pd.read_csv(StringIO(text), sep=d, header=0, engine="python", encoding=enc)
                    if df.shape[1] > 1:
                        return df, enc, d, True
                except Exception:
                    continue
    # whitespace fallback
    for enc in encodings:
        try:
            text = raw_bytes.decode(enc)
            df = pd.read_csv(StringIO(text), sep=r'\s+', header=0, engine="python")
            if df.shape[1] > 1:
                return df, enc, r'\s+', True
        except Exception:
            continue
    return None, None, None, None


def _read_flexible_excel_or_text(uploaded_file, header=0, prefer_header_detection=True):
    if hasattr(uploaded_file, "read"):
        raw = uploaded_file.read()
        try:
            uploaded_file.seek(0)
        except Exception:
            pass
    else:
        raw = Path(uploaded_file).read_bytes()

    filename = getattr(uploaded_file, "name", None)
    ext = Path(filename).suffix.lower() if filename else ""

    # xlsx zip
    try:
        import zipfile
        if zipfile.is_zipfile(BytesIO(raw)):
            df = _try_read_excel_bytes(raw, engine="openpyxl")
            if df is not None:
                return df, True
    except Exception:
        pass

    # if txt/csv prefer text parsing (tab first)
    if ext in (".txt", ".csv"):
        df, enc, delim, has_header = _detect_encoding_and_try_csv_preferring_tab(raw)
        if df is not None:
            return df, True if has_header else False

    # try excel engines
    if ext == ".xls":
        df = _try_read_excel_bytes(raw, engine="xlrd")
        if df is not None:
            return df, True
    if ext == ".xlsx":
        df = _try_read_excel_bytes(raw, engine="openpyxl")
        if df is not None:
            return df, True

    for engine in ("openpyxl", "xlrd"):
        try:
            df = _try_read_excel_bytes(raw, engine=engine)
            if df is not None:
                return df, True
        except Exception:
            continue

    # general text attempt
    df, enc, delim, has_header = _detect_encoding_and_try_csv_preferring_tab(raw)
    if df is not None:
        return df, True if has_header else False

    # html fall back
    try:
        text = None
        for enc in ("utf-8", "cp1252", "latin-1"):
            try:
                text = raw.decode(enc)
                break
            except Exception:
                continue
        if text and ("<table" in text.lower() or "<html" in text.lower()):
            tables = pd.read_html(StringIO(text))
            if tables:
                return tables[0], True
    except Exception:
        pass

    raise RuntimeError("Tentativi di lettura falliti: file non riconosciuto come Excel/CSV/HTML.")


def _read_xls_try_header(uploaded_file):
    df, had_header = _read_flexible_excel_or_text(uploaded_file, header=0)
    return df


def _read_xls_no_header(uploaded_file):
    df, had_header = _read_flexible_excel_or_text(uploaded_file, header=None)
    if df is None:
        raise RuntimeError("Impossibile leggere il file.")
    if df.shape[1] >= 8 and list(df.columns)[:8] == list(range(8)):
        df = df.iloc[:, :8]
        df.columns = ['Mat', 'Cognome', 'Nome', 'Qualifica', 'Data', 'Giorno', 'Turno', 'Minuti']
    return df


def _has_expected_header_columns(df):
    cols = [str(c).strip().lower() for c in df.columns]
    has_mat = any("matric" in c or c == "mat" for c in cols)
    has_cognome = any("cognome" in c for c in cols)
    has_nome = any("nome" in c for c in cols)
    has_data = any("data" in c for c in cols)
    has_turno = any("turno" in c for c in cols)
    return (has_mat and has_cognome and has_nome and has_turno) or (has_mat and has_cognome and has_nome and has_data)


def read_input_table(uploaded_file):
    try:
        df_head = _read_xls_try_header(uploaded_file)
        if _has_expected_header_columns(df_head):
            return df_head, True
    except Exception:
        pass
    df_no_head = _read_xls_no_header(uploaded_file)
    return df_no_head, False


# -----------------------
# Normalization / tokenization / mapping / date
# -----------------------
def normalize_df_with_headers(df):
    df = df.copy()
    col_map = {}
    for c in df.columns:
        c_norm = str(c).strip().lower()
        if "matric" in c_norm or c_norm == "mat":
            col_map[c] = "Mat"
        elif "cognome" in c_norm:
            col_map[c] = "Cognome"
        elif "nome" in c_norm:
            col_map[c] = "Nome"
        elif "data" in c_norm:
            col_map[c] = "Data_raw"
        elif "turnoe" in c_norm or (c_norm.startswith("turno") and c_norm.endswith("e")):
            col_map[c] = "TurnoE"
        elif c_norm.startswith("turno"):
            col_map[c] = "TurnoGeneric"
        elif "minut" in c_norm:
            col_map[c] = "Minuti"
        else:
            col_map[c] = c
    df = df.rename(columns=col_map)
    if "Mat" in df.columns:
        df['Mat'] = df['Mat'].astype(str).str.strip()
    else:
        df['Mat'] = ""
    turno_field = None
    if 'TurnoE' in df.columns:
        turno_field = 'TurnoE'
    elif 'TurnoGeneric' in df.columns:
        turno_field = 'TurnoGeneric'
    else:
        for c in df.columns:
            if 'turno' in str(c).lower():
                turno_field = c
                break
    if turno_field:
        df['Turno_raw'] = df[turno_field].astype(str).fillna("").str.strip()
        df['Turno_tokens'] = df['Turno_raw'].apply(lambda s: extract_turno_tokens(s))
    else:
        df['Turno_raw'] = ""
        df['Turno_tokens'] = [[] for _ in range(len(df))]
    return df


def normalize_df_no_header(df):
    df = df.copy()
    rename_map = {
        'Mat': 'Mat',
        'Cognome': 'Cognome',
        'Nome': 'Nome',
        'Qualifica': 'Qualifica',
        'Data': 'Data_raw',
        'Giorno': 'Giorno',
        'Turno': 'Turno_raw',
        'Minuti': 'Minuti'
    }
    cols_present = {c: rename_map[c] for c in rename_map if c in df.columns}
    df = df.rename(columns=cols_present)
    if 'Mat' in df.columns:
        df['Mat'] = df['Mat'].astype(str).str.strip()
    else:
        df['Mat'] = ""
    if 'Turno_raw' in df.columns:
        df['Turno_raw'] = df['Turno_raw'].astype(str).fillna("").str.strip()
        df['Turno_tokens'] = df['Turno_raw'].apply(lambda s: extract_turno_tokens(s))
    else:
        df['Turno_raw'] = ""
        df['Turno_tokens'] = [[] for _ in range(len(df))]
    return df


def extract_turno_tokens(raw_field):
    if raw_field is None:
        return []
    s = str(raw_field).strip()
    if s == "":
        return []
    tokens = re.split(r'[^A-Za-z0-9]+', s)
    return [t for t in tokens if t]


def map_tokens_to_category(tokens, code_to_cat):
    if not tokens:
        return None, None
    upper_map = {k.upper(): v for k, v in code_to_cat.items()}
    for tok in tokens:
        tok_up = tok.upper()
        if tok_up in upper_map:
            return tok, upper_map[tok_up]
    return None, None


def build_date_representation(data_raw, month=None, year=None):
    if pd.isna(data_raw):
        return ""
    try:
        dt = pd.to_datetime(data_raw, dayfirst=True, errors='coerce')
        if not pd.isna(dt):
            return dt.strftime('%d/%m/%Y')
    except Exception:
        pass
    try:
        day = int(str(data_raw).strip())
        if month is not None and year is not None:
            try:
                dt = datetime(year=year, month=month, day=day)
                return dt.strftime('%d/%m/%Y')
            except Exception:
                return str(day)
        else:
            return str(day)
    except Exception:
        return str(data_raw)


def infer_month_string_from_dates(df):
    months_it = ["Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
                 "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre"]
    if 'Data_parsed' in df.columns:
        valid = df['Data_parsed'].dropna()
        if not valid.empty:
            first = valid.iloc[0]
            try:
                return f"{months_it[first.month - 1]} {first.year}"
            except Exception:
                return None
    if 'Data_raw' in df.columns:
        for v in df['Data_raw'].dropna().tolist():
            try:
                d = pd.to_datetime(v, dayfirst=True, errors='coerce')
                if pd.notna(d):
                    return f"{months_it[d.month - 1]} {d.year}"
            except Exception:
                continue
    return None


# -----------------------
# process_workbook (ritorna grouped_df, df_valid, month_string)
# -----------------------
def process_workbook(uploaded_file, code_to_cat, infer_month=False, month_for_days=None, year_for_days=None):
    raw_df, had_header = read_input_table(uploaded_file)
    if had_header:
        df = normalize_df_with_headers(raw_df)
    else:
        df = normalize_df_no_header(raw_df)

    # Data_repr e Data_parsed
    if 'Data_raw' in df.columns:
        df['Data_repr'] = df['Data_raw'].apply(lambda v: build_date_representation(v, month_for_days, year_for_days))
        df['Data_parsed'] = pd.to_datetime(df['Data_raw'], dayfirst=True, errors='coerce')
        if month_for_days is not None and year_for_days is not None:
            def maybe_build_parsed(v):
                try:
                    parsed = pd.to_datetime(v, dayfirst=True, errors='coerce')
                    if pd.isna(parsed):
                        day = int(str(v).strip())
                        return pd.Timestamp(datetime(year=year_for_days, month=month_for_days, day=day))
                    return parsed
                except Exception:
                    return pd.NaT
            df['Data_parsed'] = df['Data_raw'].apply(maybe_build_parsed)
    else:
        df['Data_repr'] = ""
        df['Data_parsed'] = pd.NaT

    # mappatura tokens -> Category
    def map_row_tokens(tokens):
        code, cat = map_tokens_to_category(tokens, code_to_cat)
        return pd.Series({"MatchedCode": code, "Category": cat})

    if 'Turno_tokens' in df.columns:
        mapped = df['Turno_tokens'].apply(lambda toks: map_row_tokens(toks))
        df = pd.concat([df, mapped], axis=1)
    else:
        df['MatchedCode'] = None
        df['Category'] = None

    # righe valide (Category non null)
    df_valid = df[df['Category'].notnull()].copy()

    # aggiungo Nr = 1 per ogni riga (utile per contare)
    df_valid['Nr'] = 1

    # Ordinamento: Category -> Mat (int se possibile) -> Data_parsed -> Data_repr
    def try_int(x):
        try:
            return int(x)
        except Exception:
            return x

    if 'Mat' not in df_valid.columns:
        df_valid['Mat'] = ""

    df_valid['Mat_sort_key'] = df_valid['Mat'].apply(try_int)
    df_valid['Data_sort_key'] = df_valid['Data_parsed'].fillna(pd.NaT)
    df_valid = df_valid.sort_values(by=['Category', 'Mat_sort_key', 'Data_sort_key', 'Data_repr'])

    # aggregazione per Category/Mat per la tabella riepilogativa (opzionale)
    def dates_agg(x):
        vals = [v for v in x.dropna().tolist() if str(v).strip() != ""]
        seen = []
        for v in vals:
            s = str(v)
            if s not in seen:
                seen.append(s)
        return ", ".join(seen)

    grouped = df_valid.groupby(['Category', 'Mat', 'Cognome', 'Nome', 'Qualifica'], sort=False).agg(
        Dates=('Data_repr', dates_agg),
        DaysCount=('Data_repr', lambda x: len([d for d in x.dropna().unique() if str(d).strip() != ""])),
        RawTurns=('Turno_raw', lambda s: ", ".join(pd.Series(s.dropna().unique()).astype(str).tolist()))
    ).reset_index()

    grouped['Mat_sort_key'] = grouped['Mat'].apply(try_int)
    grouped = grouped.sort_values(by=['Category', 'Mat_sort_key'])

    month_string = None
    if infer_month:
        month_string = infer_month_string_from_dates(df_valid)

    # pulizia colonne ausiliarie
    if 'Mat_sort_key' in grouped.columns:
        grouped = grouped.drop(columns=['Mat_sort_key'])
    if 'Mat_sort_key' in df_valid.columns:
        df_valid = df_valid.drop(columns=['Mat_sort_key', 'Data_sort_key'])

    return grouped, df_valid, month_string


# -----------------------
# PDF generator: usa grouped_df e df_valid per produrre il layout richiesto
# -----------------------
def to_pdf_bytes(grouped_df, df_valid, month_string=""):
    try:
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, PageBreak
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            "reportlab non è installato. Installa la dipendenza (es. pip install reportlab) "
            "o aggiungi 'reportlab' a requirements.txt e riavvia l'app."
        ) from e

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=12 * mm, leftMargin=12 * mm, topMargin=12 * mm, bottomMargin=12 * mm)
    styles = getSampleStyleSheet()
    style_title = ParagraphStyle('title', parent=styles['Heading1'], alignment=0, spaceAfter=6)
    style_cat = ParagraphStyle('cat', parent=styles['Heading2'], alignment=0, spaceAfter=6)
    style_normal = styles['Normal']
    style_small = ParagraphStyle('small', parent=styles['Normal'], fontSize=8)

    story = []

    # Se è presente month_string, usalo nell'intestazione
    # Per ogni categoria, generiamo il blocco
    if grouped_df.empty:
        story.append(Paragraph("Nessuna dicitura di assenza trovata.", style_normal))
    else:
        # determiniamo categoria in ordine apparizione grouped_df
        categories = grouped_df['Category'].unique().tolist()
        for idx_cat, cat in enumerate(categories):
            # intestazione categoria con mese se disponibile
            header = f"{cat} nel mese:"
            if month_string:
                header = f"{cat} nel mese:\t{month_string}"
            story.append(Paragraph(header, style_cat))
            story.append(Spacer(1, 4))

            # seleziona df_valid di questa categoria
            df_cat = df_valid[df_valid['Category'] == cat].copy()
            if df_cat.empty:
                story.append(Paragraph("Nessun elemento per questa categoria.", style_normal))
                if idx_cat != len(categories) - 1:
                    story.append(PageBreak())
                continue

            # elenchiamo per matricola (ordinata)
            mats = df_cat['Mat'].apply(lambda x: int(x) if str(x).isdigit() else x).unique().tolist()
            # mantieni ordine presente in df_cat (già ordinato)
            # raggruppa per Mat
            for i, mat in enumerate(mats):
                rows_mat = df_cat[df_cat['Mat'].astype(str) == str(mat)].copy()
                if rows_mat.empty:
                    continue

                # Tabella: header colonne
                data = [["Mat", "Cognome", "Nome", "Qualifica", "Data", "Giorno", "Turno", "Minuti", "Nr"]]
                for _, r in rows_mat.iterrows():
                    turno_display = r.get('MatchedCode') if pd.notna(r.get('MatchedCode')) and r.get('MatchedCode') is not None else r.get('Turno_raw', "")
                    minuti = r.get('Minuti', "")
                    # assicurati che Minuti sia stringa
                    minuti = str(int(minuti)) if (pd.notna(minuti) and str(minuti).strip() != "") else ""
                    nr = 1
                    data.append([r.get('Mat', ""), r.get('Cognome', ""), r.get('Nome', ""), r.get('Qualifica', ""), r.get('Data_repr', ""), r.get('Giorno', ""), turno_display, minuti, str(nr)])

                # Totale matricola: conteggio giorni unici (basato su Data_repr)
                unique_days = rows_mat['Data_repr'].dropna().astype(str).unique()
                tot_count = len([d for d in unique_days if d.strip() != ""])

                # aggiungi riga di totale con prima cella "Mat Totale" e conteggio nell'ultima colonna
                totale_row = [f"{mat} Totale"] + [""] * (len(data[0]) - 2) + [str(tot_count)]
                data.append(totale_row)

                # costruzione e stile tabella
                colWidths = [20 * mm, 40 * mm, 40 * mm, 25 * mm, 22 * mm, 25 * mm, 20 * mm, 15 * mm, 12 * mm]
                table = Table(data, colWidths=colWidths)
                tbl_style = TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#d3d3d3")),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 8),
                    ('ALIGN', (0, 0), (0, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
                    ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                    ('FONTSIZE', (0, 1), (-1, -1), 8),
                    ('SPAN', (0, len(data) - 1), (len(data[0]) - 2, len(data) - 1)),  # span tot row except last cell
                    ('ALIGN', (len(data[0]) - 1, len(data) - 1), (len(data[0]) - 1, len(data) - 1), 'CENTER'),
                ])
                table.setStyle(tbl_style)
                story.append(table)
                story.append(Spacer(1, 6))

            if idx_cat != len(categories) - 1:
                story.append(PageBreak())

    doc.build(story)
    pdf = buffer.getvalue()
    buffer.close()
    return pdf
