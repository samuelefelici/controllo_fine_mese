"""
processor.py - versione completa e autonoma, pronta da incollare.

Scopo: leggere il file di input (.xls/.xlsx/.csv/.txt), estrarre le
diciture di assenza usando codes.csv, aggregare e generare PDF.

Nota:
- Mantengo il codice semplice e leggibile (commenti in italiano).
- Assicurati che requirements.txt contenga almeno: pandas, xlrd>=2.0.1, openpyxl, reportlab.
"""
import re
from io import BytesIO, StringIO
from datetime import datetime
from pathlib import Path

import pandas as pd


# -----------------------
# Load codes map
# -----------------------
def load_codes_map(codes_csv_path):
    """
    Legge codes.csv (path o file-like) e restituisce (code_to_cat, categories_order).
    Accetta codici separati da ',' o ';' nella colonna Codes.
    """
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
                code_to_cat[code.upper()] = cat  # salvo in uppercase per confronto case-insensitive
    return code_to_cat, categories_order


# -----------------------
# Reader semplice e robusto per file comuni
# -----------------------
def read_input_table(uploaded_file):
    """
    Legge il file caricato (Streamlit UploadedFile o path) e prova:
      - Excel (.xls -> xlrd, .xlsx -> openpyxl)
      - testo (.txt/.csv) preferendo tab ('\t') con encoding cp1252, poi altri delimitatori
    Ritorna: (df, had_header: bool)
    """
    name = getattr(uploaded_file, "name", None)
    ext = Path(name).suffix.lower() if name else ""

    # leggi tutti i bytes (se file-like)
    if hasattr(uploaded_file, "read"):
        raw = uploaded_file.read()
        try:
            uploaded_file.seek(0)
        except Exception:
            pass
    else:
        raw = Path(uploaded_file).read_bytes()

    # Helper: try excel engines
    def try_excel(engine):
        try:
            return pd.read_excel(BytesIO(raw), header=0, engine=engine)
        except Exception:
            return None

    # se estensione suggerisce excel
    if ext == ".xlsx":
        df = try_excel("openpyxl")
        if df is not None:
            return df, True
    if ext == ".xls":
        df = try_excel("xlrd")
        if df is not None:
            return df, True

    # se estensione è testo (.txt/.csv) o fallback: proviamo parsing testo
    def try_text_parsing():
        encodings = ["cp1252", "utf-8", "latin-1"]
        delimiters = ["\t", ";", ",", "|"]
        for enc in encodings:
            try:
                text = raw.decode(enc)
            except Exception:
                continue
            # prima proviamo tab (molto comune per i file esportati)
            try:
                df = pd.read_csv(StringIO(text), sep="\t", header=0, engine="python")
                if df.shape[1] > 1:
                    return df, True
            except Exception:
                pass
            # poi altri delimitatori
            for d in delimiters:
                try:
                    df = pd.read_csv(StringIO(text), sep=d, header=0, engine="python")
                    if df.shape[1] > 1:
                        return df, True
                except Exception:
                    continue
        # ultima prova: lascia che pandas cerchi di interpretare (sembra testo senza header)
        for enc in encodings:
            try:
                text = raw.decode(enc)
            except Exception:
                continue
            try:
                df = pd.read_csv(StringIO(text), sep=None, engine="python")
                if df.shape[1] > 1:
                    return df, True
            except Exception:
                continue
        return None, None

    if ext in (".txt", ".csv"):
        parsed, had_header = try_text_parsing()
        if parsed is not None:
            return parsed, had_header

    # fallback generico: proviamo excel engines (in caso l'estensione fosse errata)
    for engine in ("openpyxl", "xlrd"):
        df = try_excel(engine)
        if df is not None:
            return df, True

    # finalmente, tentativo testo generico
    parsed, had_header = try_text_parsing()
    if parsed is not None:
        return parsed, had_header

    raise RuntimeError("Impossibile leggere il file: formato non riconosciuto.")


# -----------------------
# Normalizzazione colonne
# -----------------------
def normalize_df_with_headers(df):
    """
    Rinomina colonne comuni in colonne standard:
      Mat, Cognome, Nome, Qualifica, Data_raw, Giorno, Turno_raw, Minuti
    Crea Turno_tokens (lista di token estratti da Turno_raw)
    """
    df = df.copy()
    col_map = {}
    for c in df.columns:
        cn = str(c).strip().lower()
        if "matric" in cn or cn == "mat":
            col_map[c] = "Mat"
        elif "cognome" in cn:
            col_map[c] = "Cognome"
        elif "nome" in cn:
            col_map[c] = "Nome"
        elif "qualif" in cn:
            col_map[c] = "Qualifica"
        elif "data" in cn:
            col_map[c] = "Data_raw"
        elif "gior" in cn:
            col_map[c] = "Giorno"
        elif "turnoe" in cn or (cn.startswith("turno") and cn.endswith("e")):
            col_map[c] = "Turno_raw"
        elif cn.startswith("turno"):
            col_map[c] = "Turno_raw"
        elif "minut" in cn:
            col_map[c] = "Minuti"
        else:
            col_map[c] = c
    df = df.rename(columns=col_map)

    if "Mat" in df.columns:
        df["Mat"] = df["Mat"].astype(str).str.strip()
    else:
        df["Mat"] = ""

    if "Turno_raw" in df.columns:
        df["Turno_raw"] = df["Turno_raw"].astype(str).fillna("").str.strip()
        df["Turno_tokens"] = df["Turno_raw"].apply(extract_turno_tokens)
    else:
        df["Turno_raw"] = ""
        df["Turno_tokens"] = [[] for _ in range(len(df))]

    # assicurati esistano colonne opzionali
    for col in ("Cognome", "Nome", "Qualifica", "Data_raw", "Giorno", "Minuti"):
        if col not in df.columns:
            df[col] = ""

    return df


def normalize_df_no_header(df):
    """
    Quando il file non ha header e ha layout semplice (Mat,Cognome,Nome,...)
    proviamo a rinominare le prime colonne se sono numeriche 0..n.
    Se non corrispondono, manteniamo i nomi attuali.
    """
    df = df.copy()
    # se le colonne sono numeriche 0.. e abbiamo almeno 8 colonne, mappiamo come atteso
    if df.shape[1] >= 8 and list(df.columns)[:8] == list(range(8)):
        df = df.iloc[:, :8]
        df.columns = ["Mat", "Cognome", "Nome", "Qualifica", "Data_raw", "Giorno", "Turno_raw", "Minuti"]
    else:
        # fallback: rinomina colonne conosciute se presenti
        df = df.rename(columns={0: "Mat", 1: "Cognome", 2: "Nome", 3: "Qualifica", 4: "Data_raw", 5: "Giorno", 6: "Turno_raw", 7: "Minuti"})
    # assicurati tipi
    if "Mat" in df.columns:
        df["Mat"] = df["Mat"].astype(str).str.strip()
    else:
        df["Mat"] = ""
    if "Turno_raw" in df.columns:
        df["Turno_raw"] = df["Turno_raw"].astype(str).fillna("").str.strip()
        df["Turno_tokens"] = df["Turno_raw"].apply(extract_turno_tokens)
    else:
        df["Turno_raw"] = ""
        df["Turno_tokens"] = [[] for _ in range(len(df))]
    for col in ("Cognome", "Nome", "Qualifica", "Data_raw", "Giorno", "Minuti"):
        if col not in df.columns:
            df[col] = ""
    return df


# -----------------------
# Tokenization / mapping
# -----------------------
def extract_turno_tokens(raw_field):
    """Estrae token alfanumerici dall campo turno, preservando ordine."""
    if raw_field is None:
        return []
    s = str(raw_field).strip()
    if s == "":
        return []
    tokens = re.split(r"[^A-Za-z0-9]+", s)
    return [t for t in tokens if t]


def map_tokens_to_category(tokens, code_to_cat):
    """
    Cerca il primo token che matcha una chiave in code_to_cat (case-insensitive).
    Restituisce (matched_token, category) oppure (None, None).
    """
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
    """
    Restituisce stringa dd/mm/YYYY se possibile.
    - se data_raw è già una data parseable -> formato dd/mm/YYYY
    - se è un giorno numerico e month/year forniti -> costruisce la data
    - altrimenti ritorna la rappresentazione stringa originale
    """
    if pd.isna(data_raw):
        return ""
    # prova parse
    try:
        dt = pd.to_datetime(data_raw, dayfirst=True, errors="coerce")
        if not pd.isna(dt):
            return dt.strftime("%d/%m/%Y")
    except Exception:
        pass
    # prova day + month/year
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
    """Se possibile, inferisce 'Mese YYYY' dalla prima Data_parsed presente."""
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
    # fallback: prova a parsare Data_raw
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
    """
    Esegue tutto il flusso:
    - legge il file (read_input_table)
    - normalizza colonne
    - costruisce Data_repr/Data_parsed
    - estrae token e mappa categorie tramite code_to_cat
    - filtra righe riconosciute e ordina
    Ritorna: (grouped_df, df_valid, month_string)
    """
    raw_df, had_header = read_input_table(uploaded_file)

    # normalizzazione
    if had_header:
        df = normalize_df_with_headers(raw_df)
    else:
        df = normalize_df_no_header(raw_df)

    # Data_repr e Data_parsed
    if "Data_raw" in df.columns:
        df["Data_repr"] = df["Data_raw"].apply(lambda v: build_date_representation(v, month_for_days, year_for_days))
        df["Data_parsed"] = pd.to_datetime(df["Data_raw"], dayfirst=True, errors="coerce")
        # se month/year forniti proviamo a costruire Data_parsed quando non parseable
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

    # mapping token -> category
    def map_row_tokens(tokens):
        code, cat = map_tokens_to_category(tokens, code_to_cat)
        return pd.Series({"MatchedCode": code, "Category": cat})

    if "Turno_tokens" in df.columns:
        mapped = df["Turno_tokens"].apply(lambda toks: map_row_tokens(toks))
        df = pd.concat([df, mapped], axis=1)
    else:
        df["MatchedCode"] = None
        df["Category"] = None

    # filtra righe con categoria nota
    df_valid = df[df["Category"].notnull()].copy()
    if df_valid.empty:
        # ritorna strutture vuote coerenti
        grouped = pd.DataFrame(columns=["Category", "Mat", "Cognome", "Nome", "Qualifica", "Dates", "DaysCount", "RawTurns"])
        return grouped, df_valid, None

    # Nr per riga
    df_valid["Nr"] = 1

    # ordinamento: Category -> Mat (numeric se possibile) -> Data_parsed -> Data_repr
    def try_int(x):
        try:
            return int(x)
        except Exception:
            return x

    df_valid["Mat_sort_key"] = df_valid["Mat"].apply(try_int)
    df_valid["Data_sort_key"] = df_valid["Data_parsed"].fillna(pd.NaT)
    df_valid = df_valid.sort_values(by=["Category", "Mat_sort_key", "Data_sort_key", "Data_repr"])

    # aggregazione di supporto
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

    # pulizia chiavi temporanee
    if "Mat_sort_key" in grouped.columns:
        grouped = grouped.drop(columns=["Mat_sort_key"])
    if "Mat_sort_key" in df_valid.columns:
        df_valid = df_valid.drop(columns=["Mat_sort_key", "Data_sort_key"])

    return grouped, df_valid, month_string


# -----------------------
# Generazione PDF
# -----------------------
def to_pdf_bytes(grouped_df, df_valid, month_string=""):
    """
    Genera PDF con il layout per categoria -> matricola -> righe dettagliate + Totale matricola.
    Restituisce bytes del PDF.
    (reportlab importato qui per evitare import-time error se non installato)
    """
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

                # crea tabella
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
