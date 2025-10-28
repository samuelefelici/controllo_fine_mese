"""
processor.py - versione Conerobus (ottobre 2025)

Specifico per i file mensili Conerobus:
- Colonna B = Matricola
- Colonna C = Cognome
- Colonna D = Nome
- Colonna E = Giorno numerico (1–31)
- Colonna F = Tipo giorno (Lun, Mar, Mer…)
- Colonna M = TurnoE (codice turno)

Gestione semplificata e robusta:
- Lettura automatica CSV/XLS
- Normalizzazione basata su struttura fissa
- Mapping codici → categorie tramite codes.csv
- Aggregazione e generazione PDF
"""

import re
import csv
from io import BytesIO, StringIO
from datetime import datetime
from pathlib import Path

import pandas as pd


# ============================================================
# LETTURA FILE (Excel o CSV)
# ============================================================

def _detect_encoding_and_try_csv(raw_bytes):
    encodings = ["utf-8", "cp1252", "latin-1"]
    for enc in encodings:
        try:
            text = raw_bytes.decode(enc)
            df = pd.read_csv(StringIO(text), sep="\t", header=0)
            if df.shape[1] > 1:
                return df, enc, "\t"
        except Exception:
            continue
    return None, None, None


def _read_xls_try_header(uploaded_file):
    """Legge un file Excel o CSV/TXT restituendo un DataFrame pandas."""
    if hasattr(uploaded_file, "read"):
        raw = uploaded_file.read()
        uploaded_file.seek(0)
    else:
        raw = Path(uploaded_file).read_bytes()

    filename = getattr(uploaded_file, "name", "")
    ext = Path(filename).suffix.lower()

    if ext in (".xls", ".xlsx"):
        try:
            df = pd.read_excel(BytesIO(raw), header=0)
            return df, True
        except Exception:
            pass

    df_detect, enc, delim = _detect_encoding_and_try_csv(raw)
    if df_detect is not None:
        return df_detect, True

    raise RuntimeError("Formato file non riconosciuto. Usa .xls, .xlsx o .txt tabulato.")


# ============================================================
# NORMALIZZAZIONE (struttura fissa Conerobus)
# ============================================================

def normalize_conerobus_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizza il file mensile Conerobus con struttura fissa:
    B=Matricola, C=Cognome, D=Nome, E=Data (giorno numerico),
    F=Tipo giorno (Lun, Mar...), M=TurnoE (codice turno).
    """
    df = df.copy()
    df = df.dropna(how="all")

    col_map = {
        df.columns[1]: "Mat",
        df.columns[2]: "Cognome",
        df.columns[3]: "Nome",
        df.columns[4]: "Data_raw",
        df.columns[5]: "Giorno",
        df.columns[12]: "TurnoE",  # colonna M (13°)
    }
    df = df.rename(columns=col_map)

    # pulizia
    df["Mat"] = df["Mat"].astype(str).str.strip()
    df["Cognome"] = df["Cognome"].astype(str).str.strip().str.upper()
    df["Nome"] = df["Nome"].astype(str).str.strip().str.upper()
    df["Giorno"] = df["Giorno"].astype(str).str.strip().str.capitalize()

    # Data (giorno numerico)
    df["Data_raw"] = (
        df["Data_raw"].astype(str).str.extract(r"(\d+)", expand=False).fillna("")
    )

    # Turno
    df["Turno_raw"] = (
        df["TurnoE"].astype(str).fillna("").str.strip().replace("nan", "")
    )

    # tokenizzazione turno
    df["Turno_tokens"] = df["Turno_raw"].apply(
        lambda x: [t.strip() for t in str(x).split() if t.strip()]
    )

    return df


# ============================================================
# MAPPATURA CODICI → CATEGORIE
# ============================================================

def load_codes_map(codes_csv_path):
    """Carica il file codes.csv con colonne: Category, Codes"""
    if not isinstance(codes_csv_path, (str, Path)):
        df = pd.read_csv(codes_csv_path, dtype=str).fillna("")
    else:
        df = pd.read_csv(codes_csv_path, dtype=str).fillna("")

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


def map_turni_to_category(df: pd.DataFrame, code_to_cat: dict) -> pd.DataFrame:
    """Aggiunge colonne MatchedCode e Category al DataFrame."""
    df = df.copy()
    codes_upper = {k.upper(): v for k, v in code_to_cat.items()}

    def find_mapping(tokens):
        for t in tokens:
            t_up = t.upper()
            if t_up in codes_upper:
                return (t, codes_upper[t_up])
        return (None, None)

    results = df["Turno_tokens"].apply(find_mapping)
    df["MatchedCode"] = [mc for mc, _ in results]
    df["Category"] = [cat for _, cat in results]
    return df


# ============================================================
# COSTRUZIONE DATA E AGGREGAZIONE
# ============================================================

def build_date_representation(data_raw, month=None, year=None):
    """Costruisce una data leggibile (es. 05/11/2025) dal numero giorno."""
    try:
        day = int(str(data_raw).strip())
        if month and year:
            dt = datetime(year=year, month=month, day=day)
            return dt.strftime("%d/%m/%Y")
        return str(day)
    except Exception:
        return str(data_raw)


def process_workbook(uploaded_file, code_to_cat, month_for_days=None, year_for_days=None):
    """Flusso principale di elaborazione Conerobus."""
    raw_df, _ = _read_xls_try_header(uploaded_file)
    df = normalize_conerobus_df(raw_df)
    df = map_turni_to_category(df, code_to_cat)

    df["Data_repr"] = df["Data_raw"].apply(lambda v: build_date_representation(v, month_for_days, year_for_days))

    df_valid = df[df["Category"].notnull()].copy()
    if df_valid.empty:
        return pd.DataFrame(), pd.DataFrame(), None

    df_valid["Nr"] = 1

    def dates_agg(x):
        vals = [v for v in x.dropna().unique() if str(v).strip()]
        return ", ".join(map(str, vals))

    grouped = df_valid.groupby(["Category", "Mat", "Cognome", "Nome"], sort=False).agg(
        Dates=("Data_repr", dates_agg),
        DaysCount=("Data_repr", "nunique"),
        RawTurns=("Turno_raw", lambda s: ", ".join(pd.Series(s.dropna().unique()).astype(str).tolist()))
    ).reset_index()

    return grouped, df_valid, None


# ============================================================
# GENERAZIONE PDF
# ============================================================

def to_pdf_bytes(grouped_df, df_valid, month_string=""):
    """Crea un PDF riepilogativo per ogni categoria."""
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=A4,
        rightMargin=12 * mm, leftMargin=12 * mm,
        topMargin=12 * mm, bottomMargin=12 * mm
    )

    styles = getSampleStyleSheet()
    style_cat = ParagraphStyle("cat", parent=styles["Heading2"], spaceAfter=6)
    style_normal = styles["Normal"]

    story = []

    if grouped_df.empty:
        story.append(Paragraph("Nessuna dicitura trovata.", style_normal))
    else:
        for idx_cat, cat in enumerate(grouped_df["Category"].unique()):
            story.append(Paragraph(f"{cat}", style_cat))
            story.append(Spacer(1, 4))

            df_cat = df_valid[df_valid["Category"] == cat]
            for mat in df_cat["Mat"].unique():
                rows = df_cat[df_cat["Mat"] == mat]
                data = [["Mat", "Cognome", "Nome", "Data", "Giorno", "Turno"]]
                for _, r in rows.iterrows():
                    data.append([
                        r["Mat"], r["Cognome"], r["Nome"],
                        r["Data_repr"], r["Giorno"], r["MatchedCode"] or r["Turno_raw"]
                    ])
                tot = len(rows["Data_repr"].dropna().unique())
                data.append([f"Totale: {tot} giorni"] + [""] * 5)
                table = Table(data, colWidths=[25*mm, 35*mm, 35*mm, 25*mm, 20*mm, 30*mm])
                table.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#cccccc")),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                ]))
                story.append(table)
                story.append(Spacer(1, 6))

            if idx_cat != len(grouped_df["Category"].unique()) - 1:
                story.append(PageBreak())

    doc.build(story)
    pdf = buffer.getvalue()
    buffer.close()
    return pdf
