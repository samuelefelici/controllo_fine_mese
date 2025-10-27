"""
processor.py - versione completa e robusta

Correzioni importanti:
- gestione robusta dei casi in cui df[col] ritorni un DataFrame (colonne duplicate).
- helper _ensure_series per garantire sempre una pandas.Series prima di usare .str o .apply.
- reader, normalizzazione, mappatura e generazione PDF inclusi.
"""
import re
import csv
from io import BytesIO, StringIO
from datetime import datetime
from pathlib import Path

import pandas as pd


# -----------------------
# Helper robusti
# -----------------------
def _ensure_series(df, col):
    """
    Garantisce che df[col] ritorni una pandas.Series.
    - Se la colonna non esiste -> ritorna Series vuota con indice del df.
    - Se df[col] è DataFrame (colonne duplicate), prende la prima colonna che contiene dati,
      altrimenti la prima colonna.
    """
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    s = df[col]
    # Se una selezione ritorna DataFrame (es. colonne duplicate), scegli la prima colonna sensata
    if isinstance(s, pd.DataFrame):
        # preferisci la prima colonna che non è tutta NaN/empty
        for c in s.columns:
            ser = s[c]
            try:
                if ser.notna().any():
                    return ser
            except Exception:
                pass
        # fallback: ritorna la prima colonna
        return s.iloc[:, 0]
    # normal case: è già Series
    return s


# -----------------------
# Load codes map
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
            parts = [p.strip() for p in re.split(r"[;,]+", codes_field) if p.strip()]
            for code in parts:
                code_to_cat[code.upper()] = cat
    return code_to_cat, categories_order


# -----------------------
# Reader flessibile
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
            df = pd.read_csv(StringIO(text), sep=r"\s+", header=0, engine="python")
            if df.shape[1] > 1:
                return df, enc, r"\s+", True
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

    # prefer text parsing for txt/csv
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

    # general text attempt (in case .xls is actually a TSV)
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
    if df.shape[1] >= 8 and list(df.columns)[:8] == list(range(8)):
        df = df.iloc[:, :8]
        df.columns = ["Mat", "Cognome", "Nome", "Qualifica", "Data_raw", "Giorno", "Turno_raw", "Minuti"]
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
# Normalization
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
        elif "qualif" in c_norm:
            col_map[c] = "Qualifica"
        elif "data" in c_norm:
            col_map[c] = "Data_raw"
        elif "gior" in c_norm:
            col_map[c] = "Giorno"
        elif "turnoe" in c_norm or (c_norm.startswith("turno") and c_norm.endswith("e")):
            col_map[c] = "Turno_raw"
        elif c_norm.startswith("turno"):
            col_map[c] = "Turno_raw"
        elif "minut" in c_norm:
            col_map[c] = "Minuti"
        else:
            col_map[c] = c
    df = df.rename(columns=col_map)

    df["Mat"] = _ensure_series(df, "Mat").astype(str).str.strip()
    df["Turno_raw"] = _ensure_series(df, "Turno_raw").astype(str).fillna("").str.strip()
    df["Turno_tokens"] = df["Turno_raw"].apply(extract_turno_tokens)

    for col in ("Cognome", "Nome", "Qualifica", "Data_raw", "Giorno", "Minuti"):
        df[col] = _ensure_series(df, col).astype(str).fillna("")

    return df


def normalize_df_no_header(df):
    df = df.copy()
    if df.shape[1] >= 8 and list(df.columns)[:8] == list(range(8)):
        df = df.iloc[:, :8]
        df.columns = ["Mat", "Cognome", "Nome", "Qualifica", "Data_raw", "Giorno", "Turno_raw", "Minuti"]
    else:
        # prova a rinominare colonne numeriche presenti
        rename_map = {0: "Mat", 1: "Cognome", 2: "Nome", 3: "Qualifica", 4: "Data_raw", 5: "Giorno", 6: "Turno_raw", 7: "Minuti"}
        cols_present = {c: rename_map[c] for c in rename_map if c in df.columns}
        df = df.rename(columns=cols_present)

    df["Mat"] = _ensure_series(df, "Mat").astype(str).str.strip()
    df["Turno_raw"] = _ensure_series(df, "Turno_raw").astype(str).fillna("").str.strip()
    df["Turno_tokens"] = df["Turno_raw"].apply(extract_turno_tokens)

    for col in ("Cognome", "Nome", "Qualifica", "Data_raw", "Giorno", "Minuti"):
        df[col] = _ensure_series(df, col).astype(str).fillna("")

    return df


# -----------------------
# Tokenization / mapping
# -----------------------
def extract_turno_tokens(raw_field):
    if raw_field is None:
        return []
    s = str(raw_field).strip()
    if s == "":
        return []
    tokens = re.split(r"[^A-Za-z0-9]+", s)
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


# -----------------------
# Date handling
# -----------------------
def build_date_representation(data_raw, month=None, year=None):
    if pd.isna(data_raw):
        return ""
    try:
        dt = pd.to_datetime(data_raw, dayfirst=True, errors="coerce")
        if not pd.isna(dt):
            return dt.strftime("%d/%m/%Y")
    except Exception:
        pass
    try:
        day = int(str(data_raw).strip())
        if month is not None and year is not None:
            try:
                dt = datetime(year=year, month=month, day=day)
                return dt.strftime("%d/%m/%Y")
            except Exception:
                return str(day)
        else:
            return str(day)
    except Exception:
        return str(data_raw)


def infer_month_string_from_dates(df):
    months_it = ["Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
                 "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre"]
    if "Data_parsed" in df.columns:
        valid = df["Data_parsed"].dropna()
        if not valid.empty:
            first = valid.iloc[0]
            try:
                return f"{months_it[first.month - 1]} {first.year}"
            except Exception:
                return None
    if "Data_raw" in df.columns:
        for v in df["Data_raw"].dropna().tolist():
            try:
                d = pd.to_datetime(v, dayfirst=True, errors="coerce")
                if pd.notna(d):
                    return f"{months_it[d.month - 1]} {d.year}"
            except Exception:
                continue
    return None


# -----------------------
# Main processing
# -----------------------
def process_workbook(uploaded_file, code_to_cat, infer_month=False, month_for_days=None, year_for_days=None):
    raw_df, had_header = read_input_table(uploaded_file)
    if had_header:
        df = normalize_df_with_headers(raw_df)
    else:
        df = normalize_df_no_header(raw_df)

    # Data_repr / Data_parsed
    if "Data_raw" in df.columns:
        df["Data_repr"] = df["Data_raw"].apply(lambda v: build_date_representation(v, month_for_days, year_for_days))
        df["Data_parsed"] = pd.to_datetime(df["Data_raw"], dayfirst=True, errors="coerce")
        if month_for_days is not None and year_for_days is not None:
            def maybe_build_parsed(v):
                try:
                    parsed = pd.to_datetime(v, dayfirst=True, errors="coerce")
                    if pd.isna(parsed):
                        day = int(str(v).strip())
                        return pd.Timestamp(datetime(year=year_for_days, month=month_for_days, day=day))
                    return parsed
                except Exception:
                    return pd.NaT
            df["Data_parsed"] = df["Data_raw"].apply(maybe_build_parsed)
    else:
        df["Data_repr"] = ""
        df["Data_parsed"] = pd.NaT

    # mapping tokens -> Category
    def map_row_tokens(tokens):
        code, cat = map_tokens_to_category(tokens, code_to_cat)
        return pd.Series({"MatchedCode": code, "Category": cat})

    if "Turno_tokens" in df.columns:
        mapped = df["Turno_tokens"].apply(lambda toks: map_row_tokens(toks))
        df = pd.concat([df, mapped], axis=1)
    else:
        df["MatchedCode"] = None
        df["Category"] = None

    df_valid = df[df["Category"].notnull()].copy()
    if df_valid.empty:
        grouped = pd.DataFrame(columns=["Category", "Mat", "Cognome", "Nome", "Qualifica", "Dates", "DaysCount", "RawTurns"])
        return grouped, df_valid, None

    df_valid["Nr"] = 1
    def try_int(x):
        try:
            return int(x)
        except Exception:
            return x
    df_valid["Mat_sort_key"] = _ensure_series(df_valid, "Mat").apply(try_int)
    df_valid["Data_sort_key"] = df_valid["Data_parsed"].fillna(pd.NaT)
    df_valid = df_valid.sort_values(by=["Category", "Mat_sort_key", "Data_sort_key", "Data_repr"])

    # grouped
    def dates_agg(x):
        vals = [v for v in x.dropna().tolist() if str(v).strip() != ""]
        seen = []
        for v in vals:
            s = str(v)
            if s not in seen:
                seen.append(s)
        return ", ".join(seen)

    grouped = df_valid.groupby(["Category", "Mat", "Cognome", "Nome", "Qualifica"], sort=False).agg(
        Dates=("Data_repr", dates_agg),
        DaysCount=("Data_repr", lambda x: len([d for d in x.dropna().unique() if str(d).strip() != ""])),
        RawTurns=("Turno_raw", lambda s: ", ".join(pd.Series(s.dropna().unique()).astype(str).tolist()))
    ).reset_index()

    grouped["Mat_sort_key"] = grouped["Mat"].apply(try_int)
    grouped = grouped.sort_values(by=["Category", "Mat_sort_key"])

    month_string = None
    if infer_month:
        month_string = infer_month_string_from_dates(df_valid)

    if "Mat_sort_key" in grouped.columns:
        grouped = grouped.drop(columns=["Mat_sort_key"])
    if "Mat_sort_key" in df_valid.columns:
        df_valid = df_valid.drop(columns=["Mat_sort_key", "Data_sort_key"])

    return grouped, df_valid, month_string


# -----------------------
# Generazione PDF
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
    style_cat = ParagraphStyle("cat", parent=styles["Heading2"], spaceAfter=6)
    style_normal = styles["Normal"]

    story = []

    if grouped_df.empty:
        story.append(Paragraph("Nessuna dicitura di assenza trovata.", style_normal))
    else:
        categories = grouped_df["Category"].unique().tolist()
        for idx_cat, cat in enumerate(categories):
            header = f"{cat} nel mese:"
            if month_string:
                header = f"{cat} nel mese:   {month_string}"
            story.append(Paragraph(header, style_cat))
            story.append(Spacer(1, 4))

            df_cat = df_valid[df_valid["Category"] == cat].copy()
            if df_cat.empty:
                story.append(Paragraph("Nessun elemento per questa categoria.", style_normal))
                if idx_cat != len(categories) - 1:
                    story.append(PageBreak())
                continue

            mats = df_cat["Mat"].astype(str).unique().tolist()
            for mat in mats:
                rows_mat = df_cat[df_cat["Mat"].astype(str) == str(mat)].copy()
                if rows_mat.empty:
                    continue

                data = [["Mat", "Cognome", "Nome", "Qualifica", "Data", "Giorno", "Turno", "Minuti", "Nr"]]
                for _, r in rows_mat.iterrows():
                    turno_display = r.get("MatchedCode") if (pd.notna(r.get("MatchedCode")) and r.get("MatchedCode") is not None) else r.get("Turno_raw", "")
                    minuti = r.get("Minuti", "")
                    try:
                        minuti = str(int(float(minuti))) if (pd.notna(minuti) and str(minuti).strip() != "") else ""
                    except Exception:
                        minuti = str(minuti) if pd.notna(minuti) else ""
                    nr = 1
                    data.append([
                        r.get("Mat", ""), r.get("Cognome", ""), r.get("Nome", ""), r.get("Qualifica", ""),
                        r.get("Data_repr", ""), r.get("Giorno", ""), turno_display, minuti, str(nr)
                    ])

                unique_days = rows_mat["Data_repr"].dropna().astype(str).unique()
                tot_count = len([d for d in unique_days if d.strip() != ""])
                totale_row = [f"{mat} Totale"] + [""] * (len(data[0]) - 2) + [str(tot_count)]
                data.append(totale_row)

                colWidths = [20 * mm, 40 * mm, 40 * mm, 25 * mm, 22 * mm, 25 * mm, 20 * mm, 15 * mm, 12 * mm]
                table = Table(data, colWidths=colWidths)
                tbl_style = TableStyle([
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#d3d3d3")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 8),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 1), (-1, -1), 8),
                    ("SPAN", (0, len(data) - 1), (len(data[0]) - 2, len(data) - 1)),
                    ("ALIGN", (len(data[0]) - 1, len(data) - 1), (len(data[0]) - 1, len(data) - 1), "CENTER"),
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
