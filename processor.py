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
- Lettura automatica CSV/XLS con ricerca riga header
- Normalizzazione tollerante basata su riconoscimento colonne
- Mapping codici → categorie tramite codes.csv
- Aggregazione e generazione PDF
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

        # proviamo diversi separatori
        for d in delims:
            try:
                df = pd.read_csv(StringIO(text), sep=d, header=0, dtype=str)
                if df.shape[1] > 1:
                    return df, enc, d
            except Exception:
                continue

        # fallback: usare csv.Sniffer per provare a indovinare il separatore
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
    # rewind se possibile
    if hasattr(uploaded_file, "seek"):
        try:
            uploaded_file.seek(0)
        except Exception:
            pass

    if hasattr(uploaded_file, "read"):
        raw = uploaded_file.read()
        # dopo la lettura, riportiamo il pointer all'inizio per sicurezza
        try:
            uploaded_file.seek(0)
        except Exception:
            pass
    else:
        raw = Path(uploaded_file).read_bytes()

    filename = getattr(uploaded_file, "name", "") or str(uploaded_file)
    ext = Path(filename).suffix.lower()

    # Prima proviamo a usare pd.read_excel con engine appropriato
    if ext in (".xls", ".xlsx"):
        # proviamo a lasciare che pandas scelga o forzire engine se presente
        engines_to_try = []
        if ext == ".xlsx":
            engines_to_try = ["openpyxl"]
        else:
            # .xls: proviamo xlrd (richiede xlrd installato, o fallback a engine auto)
            engines_to_try = ["xlrd", None]

        for engine in engines_to_try:
            for header_row in range(0, max_header_row_search):
                try:
                    if engine is None:
                        df_try = pd.read_excel(BytesIO(raw), header=header_row, dtype=str)
                    else:
                        df_try = pd.read_excel(BytesIO(raw), header=header_row, engine=engine, dtype=str)
                    cols_text = " ".join([str(c).lower() for c in df_try.columns])
                    keywords = {"mat", "matricola", "cognome", "nome", "turno", "giorno", "residenza"}
                    if any(k in cols_text for k in keywords):
                        return df_try, True
                except Exception:
                    # ignora e proviamo con la prossima riga/engine
                    continue
        # fallback: proviamo almeno header=0 senza engine specifico
        try:
            df = pd.read_excel(BytesIO(raw), header=0, dtype=str)
            return df, True
        except Exception:
            pass

    # Altrimenti proviamo il rilevamento CSV/TXT (come prima)
    df_detect, enc, delim = _detect_encoding_and_try_csv(raw)
    if df_detect is not None:
        return df_detect, True

    raise RuntimeError("Formato file non riconosciuto. Usa .xls, .xlsx o .txt tabulato.")


# ============================================================
# UTILI PER MAPPATURA COLONNE
# ============================================================

def _find_col_by_keywords(df: pd.DataFrame, keywords: List[str], fallback_index: Optional[int] = None) -> Optional[str]:
    """Cerca una colonna il cui nome contiene una delle keywords (case-insensitive).
    Se non trova nulla, ritorna la colonna alla posizione fallback_index se valida, altrimenti None.
    """
    lowered = [str(c).lower() for c in df.columns]
    for i, name in enumerate(lowered):
        for kw in keywords:
            if kw in name:
                return df.columns[i]
    if fallback_index is not None and 0 <= fallback_index < len(df.columns):
        return df.columns[fallback_index]
    return None


# ============================================================
# NORMALIZZAZIONE (struttura fissa Conerobus) - più robusta
# ============================================================

def normalize_conerobus_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizza il file mensile Conerobus con struttura fissa:
    B=Matricola, C=Cognome, D=Nome, E=Data (giorno numerico),
    F=Tipo giorno (Lun, Mar...), M=TurnoE (codice turno).

    Questa versione non si basa esclusivamente sugli indici di colonna: cerca
    le colonne per parole chiave e usa indici di fallback quando necessario.
    Gestisce anche colonne vuote iniziali (es. 5 colonne vuote).
    """
    df = df.copy()
    df = df.dropna(how="all")
    if df.empty:
        return df

    # Indici attesi originali (0-based) dalla versione Conerobus
    expected_indices = {
        "Mat": 1,
        "Cognome": 2,
        "Nome": 3,
        "Data_raw": 5,
        "Giorno": 6,
        "TurnoE": 12,
    }

    # Se esiste una colonna con nome 'mat' o 'matricola', usiamola per rilevare eventuale offset
    lowered = [str(c).strip().lower() for c in df.columns]
    found_mat_idx = None
    for i, name in enumerate(lowered):
        if "mat" in name or "matricola" in name:
            found_mat_idx = i
            break

    offset = 0
    if found_mat_idx is not None:
        offset = found_mat_idx - expected_indices["Mat"]
        # non permettiamo offset negativo automatico
        if offset < 0:
            offset = 0

    # Proviamo a trovare colonne per keyword, passando fallback indicizzato corretto (applicando offset)
    mat_col = _find_col_by_keywords(df, ["mat", "matricola"], fallback_index=expected_indices["Mat"] + offset)
    cognome_col = _find_col_by_keywords(df, ["cognome", "surname", "cognome"], fallback_index=expected_indices["Cognome"] + offset)
    nome_col = _find_col_by_keywords(df, ["nome", "name"], fallback_index=expected_indices["Nome"] + offset)
    data_col = _find_col_by_keywords(df, ["data", "giorno", "day"], fallback_index=expected_indices["Data_raw"] + offset)
    giorno_col = _find_col_by_keywords(df, ["tipo", "giorno", "gg", "giorn"], fallback_index=expected_indices["Giorno"] + offset)
    turno_col = _find_col_by_keywords(df, ["turno", "turnoe", "turnoE", "codice"], fallback_index=expected_indices["TurnoE"] + offset)

    col_map = {}
    if mat_col is not None:
        col_map[mat_col] = "Mat"
    if cognome_col is not None:
        col_map[cognome_col] = "Cognome"
    if nome_col is not None:
        col_map[nome_col] = "Nome"
    if data_col is not None:
        col_map[data_col] = "Data_raw"
    if giorno_col is not None:
        col_map[giorno_col] = "Giorno"
    if turno_col is not None:
        col_map[turno_col] = "TurnoE"

    # Rinominazione (se col_map è vuoto non farà nulla)
    if col_map:
        df = df.rename(columns=col_map)

    # Garantiamo l'esistenza delle colonne usate dal flusso
    for required in ["Mat", "Cognome", "Nome", "Data_raw", "Giorno", "TurnoE"]:
        if required not in df.columns:
            df[required] = ""

    # Se manca la colonna "Qualifica", aggiungila vuota per compatibilità app.py
    if "Qualifica" not in df.columns:
        df["Qualifica"] = ""

    # Pulizia base, con protezioni
    df["Mat"] = df["Mat"].astype(str).str.strip().replace("nan", "")
    df["Cognome"] = df["Cognome"].astype(str).str.strip().str.upper().replace("NAN", "")
    df["Nome"] = df["Nome"].astype(str).str.strip().str.upper().replace("NAN", "")
    df["Giorno"] = df["Giorno"].astype(str).str.strip().str.capitalize().replace("Nan", "")

    # Data (giorno numerico) - estraiamo il primo numero nella cella
    def _extract_first_number(v):
        s = str(v)
        m = re.search(r"(\d+)", s)
        return m.group(1) if m else ""

    df["Data_raw"] = df["Data_raw"].apply(_extract_first_number).fillna("").astype(str)

    # Turno
    df["Turno_raw"] = df["TurnoE"].astype(str).fillna("").str.strip().replace("nan", "")

    # Tokenizzazione turno (lista di token)
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
    """Carica il file codes.csv con colonne: Category, Codes.

    Accetta sia path che file-like (es. uploaded file in Streamlit).
    """
    if not isinstance(codes_csv_path, (str, Path)):
        # file-like object (es. streamlit upload)
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

    # Assicuriamoci che Turno_tokens esista
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
    """Costruisce una data leggibile (es. 05/11/2025) dal numero giorno."""
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
    infer_month: bool = False,     # parametro mantenuto per compatibilità ma non usato internamente
    month_for_days: int | None = None,
    year_for_days: int | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[str]]:
    """Flusso principale di elaborazione Conerobus.
    Restituisce (grouped_df, df_valid, month_string)
    """
    raw_df, _ = _read_xls_try_header(uploaded_file)
    df = normalize_conerobus_df(raw_df)
    df = map_turni_to_category(df, code_to_cat)

    # Data leggibile dal numero giorno + mese/anno forniti
    df["Data_repr"] = df["Data_raw"].apply(
        lambda v: build_date_representation(v, month_for_days, year_for_days)
    )

    # (opzionale) una Data_parsed, utile per ordinamenti cronologici in app.py
    def _to_ts(v):
        try:
            d = int(str(v).strip())
            if month_for_days and year_for_days:
                return pd.Timestamp(year_for_days, month_for_days, d)
        except Exception:
            pass
        return pd.NaT

    df["Data_parsed"] = df["Data_raw"].apply(_to_ts)

    # Filtra solo righe con categoria mappata
    df_valid = df[df["Category"].notnull() & df["Category"].astype(bool)].copy()
    if df_valid.empty:
        grouped = pd.DataFrame(
            columns=["Category", "Mat", "Cognome", "Nome", "Qualifica", "Dates", "DaysCount", "RawTurns"]
        )
        return grouped, df_valid, None

    df_valid["Nr"] = 1

    # se manca la colonna Qualifica, la aggiungiamo vuota per compatibilità con app.py
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
