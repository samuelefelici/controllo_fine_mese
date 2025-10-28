"""
processor.py - versione Conerobus (ottobre 2025) - FIX ALLINEAMENTO

PROBLEMA: Le prime 5 colonne NON hanno intestazione nel file originale.
Le intestazioni partono dalla colonna 6 (shift di 5 posizioni).

Soluzione: Ricostruiamo le intestazioni corrette ignorando quelle presenti.
"""

import re
import csv
import warnings
from io import BytesIO, StringIO
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, List, Union

import pandas as pd


# ============================================================
# LETTURA FILE (Excel o CSV)
# ============================================================

def _detect_encoding_and_try_csv(raw_bytes: bytes) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
    """Prova alcune codifiche e separatori comuni e ritorna il DataFrame se valido."""
    encodings = ["utf-8", "cp1252", "latin-1"]
    delims = ["\t", ";", ","]

    for enc in encodings:
        try:
            text = raw_bytes.decode(enc)
        except Exception:
            continue

        for d in delims:
            try:
                df = pd.read_csv(StringIO(text), sep=d, header=0, dtype=str)
                if df.shape[1] > 1:
                    return df, enc, d
            except Exception:
                continue

        try:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(text[:4096])
            sep = dialect.delimiter
            df = pd.read_csv(StringIO(text), sep=sep, header=0, dtype=str)
            if df.shape[1] > 1:
                return df, enc, sep
        except Exception:
            pass

    return None, None, None


def _read_xls_try_header(uploaded_file, max_header_row_search=10):
    if hasattr(uploaded_file, "seek"):
        try:
            uploaded_file.seek(0)
        except Exception:
            pass

    if hasattr(uploaded_file, "read"):
        raw = uploaded_file.read()
        try:
            uploaded_file.seek(0)
        except Exception:
            pass
    else:
        raw = Path(uploaded_file).read_bytes()

    filename = getattr(uploaded_file, "name", "") or str(uploaded_file)
    ext = Path(filename).suffix.lower()

    if ext in (".xls", ".xlsx"):
        engines_to_try = []
        if ext == ".xlsx":
            engines_to_try = ["openpyxl"]
        else:
            engines_to_try = ["xlrd", None]

        for engine in engines_to_try:
            for header_row in range(0, max_header_row_search):
                try:
                    if engine is None:
                        df_try = pd.read_excel(BytesIO(raw), header=header_row, dtype=str)
                    else:
                        df_try = pd.read_excel(BytesIO(raw), header=header_row, engine=engine, dtype=str)
                    cols_text = " ".join([str(c).lower() for c in df_try.columns])
                    keywords = {"mat", "matricola", "cognome", "nome", "turno", "data", "residenza"}
                    if any(k in cols_text for k in keywords):
                        return df_try, True
                except Exception:
                    continue
        try:
            df = pd.read_excel(BytesIO(raw), header=0, dtype=str)
            return df, True
        except Exception:
            pass

    df_detect, enc, delim = _detect_encoding_and_try_csv(raw)
    if df_detect is not None:
        return df_detect, True

    raise RuntimeError("Formato file non riconosciuto. Usa .xls, .xlsx o .txt tabulato.")


# ============================================================
# FIX INTESTAZIONI SPOSTATE
# ============================================================

def fix_misaligned_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    FIX per il problema Conerobus: le prime 5 colonne hanno dati ma NON hanno intestazione.
    Le intestazioni reali partono dalla colonna 6.
    
    Strategia: ricostruiamo le intestazioni corrette basandoci sulla posizione dei dati.
    """
    df = df.copy()
    
    # Conta quante colonne hanno intestazione vuota o generica
    empty_header_cols = []
    for i, col in enumerate(df.columns):
        col_str = str(col).strip()
        # Se la colonna è vuota, ha nome "Unnamed", o è solo spazi
        if not col_str or col_str.startswith("Unnamed") or col_str == "":
            empty_header_cols.append(i)
        else:
            break  # Ci fermiamo alla prima colonna con intestazione valida
    
    num_empty = len(empty_header_cols)
    
    if num_empty >= 5:
        # Ricostruiamo le intestazioni corrette per le prime 5 colonne
        new_columns = list(df.columns)
        new_columns[0] = "Residenza"
        new_columns[1] = "Matricola"
        new_columns[2] = "Cognome"
        new_columns[3] = "Nome"
        new_columns[4] = "Gruppo"
        
        # Le colonne successive mantengono le intestazioni originali (già presenti)
        # MA sono spostate: dobbiamo rinominare anche quelle
        if len(df.columns) > 5:
            new_columns[5] = "Data"
        if len(df.columns) > 6:
            new_columns[6] = "GiornoSettimana"
        if len(df.columns) > 7:
            new_columns[7] = "TurnoC"
        if len(df.columns) > 8:
            new_columns[8] = "InizioC"
        if len(df.columns) > 9:
            new_columns[9] = "FineC"
        if len(df.columns) > 10:
            new_columns[10] = "ValoreC"
        if len(df.columns) > 11:
            new_columns[11] = "DistaccatoE"
        if len(df.columns) > 12:
            new_columns[12] = "TurnoE"
        if len(df.columns) > 13:
            new_columns[13] = "InizioE"
        if len(df.columns) > 14:
            new_columns[14] = "FineE"
        if len(df.columns) > 15:
            new_columns[15] = "ValoreE"
        if len(df.columns) > 16:
            new_columns[16] = "Indennita"
        if len(df.columns) > 17:
            new_columns[17] = "Aggiuntive"
        
        df.columns = new_columns
    
    return df


# ============================================================
# NORMALIZZAZIONE
# ============================================================

def normalize_conerobus_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizza il file mensile Conerobus DOPO aver corretto le intestazioni.
    """
    df = df.copy()
    df = df.dropna(how="all")
    if df.empty:
        return df
    
    # PRIMA: correggi le intestazioni spostate
    df = fix_misaligned_headers(df)
    
    # ORA le colonne dovrebbero essere corrette
    # Verifichiamo che esistano le colonne necessarie
    required_cols = ["Residenza", "Matricola", "Cognome", "Nome", "Data", "TurnoE"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""
    
    # Rinominiamo per compatibilità con il resto del codice
    df = df.rename(columns={
        "Matricola": "Mat",
        "Data": "Data_raw",
    })
    
    # Aggiungiamo colonne mancanti
    if "Qualifica" not in df.columns:
        df["Qualifica"] = ""
    if "Giorno" not in df.columns:
        df["Giorno"] = df.get("GiornoSettimana", "")
    
    # Pulizia dati
    df["Residenza"] = df["Residenza"].astype(str).str.strip().str.upper().replace("NAN", "")
    df["Mat"] = df["Mat"].astype(str).str.strip().replace("nan", "")
    df["Cognome"] = df["Cognome"].astype(str).str.strip().str.upper().replace("NAN", "")
    df["Nome"] = df["Nome"].astype(str).str.strip().str.upper().replace("NAN", "")
    df["Gruppo"] = df.get("Gruppo", pd.Series([""] * len(df))).astype(str).str.strip().replace("nan", "")
    df["Giorno"] = df["Giorno"].astype(str).str.strip().str.capitalize().replace("Nan", "")
    
    # Data (estrazione numero giorno)
    def _extract_first_number(v):
        s = str(v)
        m = re.search(r"(\d+)", s)
        return m.group(1) if m else ""
    
    df["Data_raw"] = df["Data_raw"].apply(_extract_first_number).fillna("").astype(str)
    
    # Turno
    df["Turno_raw"] = df["TurnoE"].astype(str).fillna("").str.strip().replace("nan", "")
    
    # Tokenizzazione turno
    def _tokenize_turno(x):
        if x is None:
            return []
        s = str(x)
        parts = [t.strip() for t in s.split() if t.strip()]
        return parts
    
    df["Turno_tokens"] = df["Turno_raw"].apply(_tokenize_turno)
    
    return df


# ============================================================
# MAPPATURA CODICI → CATEGORIE
# ============================================================

def load_codes_map(codes_csv_path: Union[str, Path, object]) -> Tuple[Dict[str, str], List[str]]:
    """Carica il file codes.csv con colonne: Category, Codes."""
    if not isinstance(codes_csv_path, (str, Path)):
        df = pd.read_csv(codes_csv_path, dtype=str).fillna("")
    else:
        df = pd.read_csv(codes_csv_path, dtype=str).fillna("")

    code_to_cat: Dict[str, str] = {}
    categories_order: List[str] = []
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


def map_turni_to_category(df: pd.DataFrame, code_to_cat: Dict[str, str]) -> pd.DataFrame:
    """Aggiunge colonne MatchedCode e Category al DataFrame."""
    df = df.copy()
    codes_upper = {k.upper(): v for k, v in code_to_cat.items()}

    def find_mapping(tokens: List[str]) -> Tuple[Optional[str], Optional[str]]:
        for t in tokens:
            t_up = t.upper()
            if t_up in codes_upper:
                return (t, codes_upper[t_up])
        return (None, None)

    if "Turno_tokens" not in df.columns:
        df["Turno_tokens"] = [[] for _ in range(len(df))]

    results = df["Turno_tokens"].apply(find_mapping)
    df["MatchedCode"] = [mc for mc, _ in results]
    df["Category"] = [cat for _, cat in results]
    return df


# ============================================================
# COSTRUZIONE DATA E AGGREGAZIONE
# ============================================================

def build_date_representation(data_raw: Union[str, int], month: Optional[int] = None, year: Optional[int] = None) -> str:
    """Costruisce una data leggibile dal numero giorno."""
    try:
        day = int(str(data_raw).strip())
        if month and year:
            dt = datetime(year=year, month=month, day=day)
            return dt.strftime("%d/%m/%Y")
        return str(day)
    except Exception:
        return str(data_raw)


def process_workbook(
    uploaded_file: Union[bytes, Path, object],
    code_to_cat: Dict[str, str],
    infer_month: bool = False,
    month_for_days: int | None = None,
    year_for_days: int | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[str]]:
    """Flusso principale di elaborazione Conerobus."""
    raw_df, _ = _read_xls_try_header(uploaded_file)
    df = normalize_conerobus_df(raw_df)
    df = map_turni_to_category(df, code_to_cat)

    df["Data_repr"] = df["Data_raw"].apply(
        lambda v: build_date_representation(v, month_for_days, year_for_days)
    )

    def _to_ts(v):
        try:
            d = int(str(v).strip())
            if month_for_days and year_for_days:
                return pd.Timestamp(year_for_days, month_for_days, d)
        except Exception:
            pass
        return pd.NaT

    df["Data_parsed"] = df["Data_raw"].apply(_to_ts)

    df_valid = df[df["Category"].notnull() & df["Category"].astype(bool)].copy()
    if df_valid.empty:
        grouped = pd.DataFrame(
            columns=["Category", "Mat", "Cognome", "Nome", "Qualifica", "Dates", "DaysCount", "RawTurns"]
        )
        return grouped, df_valid, None

    df_valid["Nr"] = 1

    if "Qualifica" not in df_valid.columns:
        df_valid["Qualifica"] = ""

    def dates_agg(x):
        vals = [v for v in x.dropna().unique() if str(v).strip()]
        return ", ".join(map(str, vals))

    grouped = (
        df_valid
        .groupby(["Category", "Mat", "Cognome", "Nome", "Qualifica"], sort=False)
        .agg(
            Dates=("Data_repr", dates_agg),
            DaysCount=("Data_repr", "nunique"),
            RawTurns=("Turno_raw", lambda s: ", ".join(pd.Series(s.dropna().unique()).astype(str).tolist())),
        )
        .reset_index()
    )

    month_string = None
    return grouped, df_valid, month_string


# ============================================================
# GENERAZIONE PDF
# ============================================================

def to_pdf_bytes(grouped_df: pd.DataFrame, df_valid: pd.DataFrame, month_string: str = "") -> bytes:
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
                        r.get("Mat", ""), r.get("Cognome", ""), r.get("Nome", ""),
                        r.get("Data_repr", ""), r.get("Giorno", ""), r.get("MatchedCode", "") or r.get("Turno_raw", "")
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
