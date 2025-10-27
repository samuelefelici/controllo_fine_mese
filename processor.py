import re
import csv
from io import BytesIO, StringIO
from datetime import datetime
from pathlib import Path

import pandas as pd


# -----------------------
# load codes map
# -----------------------
def load_codes_map(codes_csv_path):
    try:
        if not isinstance(codes_csv_path, (str, Path)):
            # file-like (Streamlit UploadedFile)
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
# Flexible reader (TXT/CSV/TSV/XLS/XLSX/HTML)
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
        # try csv sniffer
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
    # read raw bytes once
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

    # xlsx (zip) -> openpyxl
    try:
        import zipfile
        if zipfile.is_zipfile(BytesIO(raw)):
            df = _try_read_excel_bytes(raw, engine="openpyxl")
            if df is not None:
                return df, True
    except Exception:
        pass

    # prefer text for txt/csv (tab-first)
    if ext in (".txt", ".csv"):
        df, enc, delim, has_header = _detect_encoding_and_try_csv_preferring_tab(raw)
        if df is not None:
            return df, True if has_header else False

    # try excel engines if ext suggests
    if ext == ".xls":
        df = _try_read_excel_bytes(raw, engine="xlrd")
        if df is not None:
            return df, True
    if ext == ".xlsx":
        df = _try_read_excel_bytes(raw, engine="openpyxl")
        if df is not None:
            return df, True

    # generic attempts (engines)
    for engine in ("openpyxl", "xlrd"):
        try:
            df = _try_read_excel_bytes(raw, engine=engine)
            if df is not None:
                return df, True
        except Exception:
            continue

    # general text attempt (useful if .xls is actually TSV)
    df, enc, delim, has_header = _detect_encoding_and_try_csv_preferring_tab(raw)
    if df is not None:
        return df, True if has_header else False

    # html fallback
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
    # map numeric 0..n columns to expected no-header layout if present
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
    # try parse as full date
    try:
        dt = pd.to_datetime(data_raw, dayfirst=True, errors='coerce')
        if not pd.isna(dt):
            return dt.strftime('%d/%m/%Y')
    except Exception:
        pass
    # try numeric day + provided month/year
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
