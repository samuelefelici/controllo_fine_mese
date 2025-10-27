# processor.py
"""
Processor resilient to files that are .xls but actually text (CSV/TSV/HTML) or true Excel.
- read_input_table tries flexible parsers and falls back to CSV/HTML parsing.
- rest of logic: normalization, token extraction, mapping, aggregation, PDF generation.
Note: no data validation is performed — the module extracts diciture.
"""

import re
import csv
from io import BytesIO, StringIO
from datetime import datetime
from pathlib import Path

import pandas as pd


# -----------------------
# codes.csv loader (supports path or file-like)
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
        cat = row.get("Category", "").strip()
        codes = row.get("Codes", "")
        if cat:
            categories_order.append(cat)
        for code in [c.strip() for c in str(codes).split(",") if c.strip()]:
            code_to_cat[code] = cat
    return code_to_cat, categories_order


# -----------------------
# Flexible Excel / CSV / HTML reader
# -----------------------
def _read_flexible_excel_or_text(uploaded_file, header=0, prefer_header_detection=True):
    """
    Prova diversi metodi per leggere input che potrebbe essere:
    - un .xls/.xlsx Excel vero (pandas.read_excel)
    - un file di testo (CSV/TSV) salvato con estensione .xls
    - un HTML contenente una tabella Excel-like

    Ritorna: (df, had_header_flag)
    """
    # Leggi bytes tutti in memoria così riusiamo più volte il contenuto
    if hasattr(uploaded_file, "read"):
        raw = uploaded_file.read()
        # riportiamo il puntatore all'inizio per sicurezza (strumenti che poi lo leggono)
        try:
            uploaded_file.seek(0)
        except Exception:
            pass
    else:
        # uploaded_file è un str/Path
        raw = Path(uploaded_file).read_bytes()

    # Primo tentativo: se abbiamo un nome con estensione, proviamo read_excel con engine corretto
    filename = getattr(uploaded_file, "name", None)
    ext = Path(filename).suffix.lower() if filename else ""

    # Utility per provare read_excel con una combinazione di engine
    def try_read_excel(bytes_buf, engine=None, header=header):
        try:
            return pd.read_excel(BytesIO(bytes_buf), header=header, engine=engine), True
        except Exception:
            return None, False

    # Se estensione è nota, prova engine specifico
    tried = []
    if ext == ".xlsx" or ext in (".xlsm", ".xltx", ".xltm"):
        df, ok = try_read_excel(raw, engine="openpyxl", header=header)
        tried.append(("openpyxl", ok))
        if ok:
            return df, True
    elif ext == ".xls":
        # xlrd>=2.0.1 legge .xls
        df, ok = try_read_excel(raw, engine="xlrd", header=header)
        tried.append(("xlrd", ok))
        if ok:
            return df, True

    # Generic attempts: try openpyxl, xlrd (order doesn't matter much)
    for engine in ("openpyxl", "xlrd"):
        df, ok = try_read_excel(raw, engine=engine, header=header)
        tried.append((engine, ok))
        if ok:
            return df, True

    # If all read_excel attempts failed, try to interpret the bytes as text
    # decode trying utf-8 then latin-1
    text = None
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            text = raw.decode(enc)
            break
        except Exception:
            continue
    if text is None:
        # fallback using replace
        text = raw.decode("utf-8", errors="replace")

    # Trim leading whitespace
    text_lstripped = text.lstrip()

    # If it looks like HTML, try pd.read_html
    if text_lstripped.startswith("<") or "<table" in text_lstripped.lower():
        try:
            tables = pd.read_html(StringIO(text))
            if tables:
                # choose first table
                return tables[0], True
        except Exception:
            pass

    # Try to detect delimiter using csv.Sniffer
    sample = text[:4096]
    sniffer = csv.Sniffer()
    detected_delim = None
    has_header = False
    try:
        dialect = sniffer.sniff(sample)
        detected_delim = dialect.delimiter
        try:
            has_header = sniffer.has_header(sample)
        except Exception:
            has_header = prefer_header_detection
    except Exception:
        # Fallback delimiters order to try
        for d in ["\t", ";", ",", "|"]:
            if d in sample:
                detected_delim = d
                break

    if detected_delim is None:
        # fallback: try tab, semicolon, comma
        for d in ["\t", ";", ",", "|"]:
            try:
                df = pd.read_csv(StringIO(text), sep=d, header=0 if prefer_header_detection else None, engine="python")
                # if dataframe has multiple columns it's likely correct
                if df.shape[1] > 1:
                    return df, True if prefer_header_detection else False
            except Exception:
                continue
        # last resort: try read_csv with whitespace delim
        try:
            df = pd.read_csv(StringIO(text), sep=r'\s+', header=0 if prefer_header_detection else None, engine="python")
            return df, True if prefer_header_detection else False
        except Exception:
            raise RuntimeError("Impossibile interpretare il file come Excel/CSV/HTML. Il file potrebbe essere corrotto o in formato non supportato.")

    # If we have a detected delimiter:
    try:
        header_row = 0 if has_header else None
        df = pd.read_csv(StringIO(text), sep=detected_delim, header=header_row, engine="python")
        # If header detection thought no header but first row looks like column names (non-numeric), keep header=None
        return df, True if header_row == 0 else False
    except Exception:
        # try a few common encodings/params
        for encoding in ("utf-8", "latin-1", "cp1252"):
            try:
                df = pd.read_csv(StringIO(text), sep=detected_delim, header=(0 if prefer_header_detection else None), encoding=encoding, engine="python")
                return df, True if prefer_header_detection else False
            except Exception:
                continue

    raise RuntimeError("Tentativi di lettura falliti: file non riconosciuto come Excel/CSV/HTML.")


# -----------------------
# Wrappers used by rest of processor
# -----------------------
def _read_xls_try_header(uploaded_file):
    # use flexible reader but ask for header=0
    df, had_header = _read_flexible_excel_or_text(uploaded_file, header=0)
    return df


def _read_xls_no_header(uploaded_file):
    df, had_header = _read_flexible_excel_or_text(uploaded_file, header=None)
    # If no header returned but df has >=8 columns, take first 8 as earlier expectation
    if df is None:
        raise RuntimeError("Impossibile leggere il file.")
    # If header=None produced column names like 0,1,2... we map to expected names
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
    # First attempt header read; if not recognized fallback to no-header version
    try:
        df_head = _read_xls_try_header(uploaded_file)
        if _has_expected_header_columns(df_head):
            return df_head, True
    except Exception:
        pass
    df_no_head = _read_xls_no_header(uploaded_file)
    return df_no_head, False


# -----------------------
# Normalizzazione, tokenization, mapping, date handling, aggregation, PDF
# (keep same logic as previous processor; see earlier messages for full implementation)
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


def process_workbook(uploaded_file, code_to_cat, infer_month=False, month_for_days=None, year_for_days=None):
    raw_df, had_header = read_input_table(uploaded_file)
    if had_header:
        df = normalize_df_with_headers(raw_df)
    else:
        df = normalize_df_no_header(raw_df)
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
    def map_row_tokens(tokens):
        code, cat = map_tokens_to_category(tokens, code_to_cat)
        return pd.Series({"MatchedCode": code, "Category": cat})
    if 'Turno_tokens' in df.columns:
        mapped = df['Turno_tokens'].apply(lambda toks: map_row_tokens(toks))
        df = pd.concat([df, mapped], axis=1)
    else:
        df['MatchedCode'] = None
        df['Category'] = None
    df_valid = df[df['Category'].notnull()].copy()
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
    def dates_agg(x):
        vals = [v for v in x.dropna().tolist() if str(v).strip() != ""]
        seen = []
        for v in vals:
            s = str(v)
            if s not in seen:
                seen.append(s)
        return ", ".join(seen)
    grouped = df_valid.groupby(['Category', 'Mat', 'Cognome', 'Nome'], sort=False).agg(
        Dates=('Data_repr', dates_agg),
        DaysCount=('Data_repr', lambda x: len([d for d in x.dropna().unique() if str(d).strip() != ""])),
        RawTurns=('Turno_raw', lambda s: ", ".join(pd.Series(s.dropna().unique()).astype(str).tolist()))
    ).reset_index()
    grouped['Mat_sort_key'] = grouped['Mat'].apply(try_int)
    grouped = grouped.sort_values(by=['Category', 'Mat_sort_key'])
    month_string = None
    if infer_month:
        month_string = infer_month_string_from_dates(df_valid)
    if 'Mat_sort_key' in grouped.columns:
        grouped = grouped.drop(columns=['Mat_sort_key'])
    if 'Mat_sort_key' in df_valid.columns:
        df_valid = df_valid.drop(columns=['Mat_sort_key'])
    return grouped, df, month_string


def to_pdf_bytes(grouped_df, month_string=""):
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
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=15 * mm, leftMargin=15 * mm, topMargin=15 * mm, bottomMargin=15 * mm)
    styles = getSampleStyleSheet()
    style_h = styles['Heading1']
    style_h.alignment = 0
    style_cat = ParagraphStyle('catstyle', parent=styles['Heading2'], spaceAfter=6)
    style_normal = styles['Normal']
    style_small = ParagraphStyle('small', parent=styles['Normal'], fontSize=8)

    story = []
    title = "Resoconto assenze"
    if month_string:
        title = f"{title} - {month_string}"
    story.append(Paragraph(title, style_h))
    story.append(Spacer(1, 6))

    if grouped_df.empty:
        story.append(Paragraph("Nessuna dicitura di assenza trovata.", style_normal))
    else:
        categories = grouped_df['Category'].unique().tolist()
        for idx, cat in enumerate(categories):
            story.append(Paragraph(f"{cat}", style_cat))
            df_cat = grouped_df[grouped_df['Category'] == cat].copy()
            if df_cat.empty:
                story.append(Paragraph("Nessun elemento per questa categoria.", style_normal))
            else:
                data = [["Mat", "Cognome Nome", "Giorni", "Date", "Turni (raw)"]]
                for _, row in df_cat.iterrows():
                    cognome_nome = f"{row.get('Cognome','')} {row.get('Nome','')}".strip()
                    giorni = str(row.get('DaysCount', ""))
                    dates = row.get('Dates', "")
                    rawturns = row.get('RawTurns', "")
                    data.append([row.get('Mat', ""), cognome_nome, giorni, dates, rawturns])

                table = Table(data, colWidths=[30 * mm, 60 * mm, 18 * mm, 55 * mm, 40 * mm])
                tbl_style = TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#d3d3d3")),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 9),
                    ('ALIGN', (0, 0), (0, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
                    ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                    ('FONTSIZE', (0, 1), (-1, -1), 8),
                ])
                table.setStyle(tbl_style)
                story.append(table)
            if idx != len(categories) - 1:
                story.append(PageBreak())

    doc.build(story)
    pdf = buffer.getvalue()
    buffer.close()
    return pdf
