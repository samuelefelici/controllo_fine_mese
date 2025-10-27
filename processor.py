# processor.py
"""
Modulo di processing per l'app "controlli_fine_mese".

Funzionalità:
- load_codes_map: legge codes.csv (Category,Codes) e costruisce mapping codice->categoria.
- read_input_table: legge file .xls provando prima con header e in fallback come file senza header.
- normalize: normalizza dataframe sia per file con header sia senza header.
- estrazione token da campo turno (TurnoE / Turno).
- mappatura token -> categoria (first-match, case-insensitive).
- costruzione rappresentazione data (senza verifiche, usa month+year forniti se necessario).
- aggregazione per Category -> Mat -> Cognome/ Nome con lista date, conteggio giorni e raw turns.
- to_pdf_bytes: genera PDF (import di reportlab fatto qui per evitare errori all'import del modulo).

Nota: il codice non esegue verifiche o correzioni sui dati — si limita ad estrarre e aggregare le diciture.
"""

import re
from io import BytesIO
from datetime import datetime
from pathlib import Path

import pandas as pd


def load_codes_map(codes_csv_path):
    """
    Legge codes.csv e ritorna (code_to_cat, categories_order).
    codes_csv_path: percorso del file CSV con colonne Category,Codes (codici separati da virgola).
    """
    df = pd.read_csv(Path(codes_csv_path), dtype=str).fillna("")
    code_to_cat = {}
    categories_order = []
    for _, row in df.iterrows():
        cat = row.get("Category", "").strip()
        codes = row.get("Codes", "")
        if cat:
            categories_order.append(cat)
        for code in [c.strip() for c in str(codes).split(",") if c.strip()]:
            # manteniamo la chiave originale (case sensitive map) ma confronti verranno fatti in uppercase
            code_to_cat[code] = cat
    return code_to_cat, categories_order


def _read_xls_try_header(uploaded_file):
    """
    Prova a leggere il file .xls assumendo header in prima riga.
    """
    return pd.read_excel(uploaded_file, header=0, engine="xlrd")


def _read_xls_no_header(uploaded_file):
    """
    Legge il file .xls senza header (header=None).
    Ritorna DataFrame con colonne standardizzate: Mat, Cognome, Nome, Qualifica,
    Data, Giorno, Turno, Minuti (se presenti).
    """
    df = pd.read_excel(uploaded_file, header=None, engine="xlrd")
    # prendi fino a 8 colonne (se presenti)
    df = df.iloc[:, :8]
    df.columns = ['Mat', 'Cognome', 'Nome', 'Qualifica', 'Data', 'Giorno', 'Turno', 'Minuti']
    return df


def _has_expected_header_columns(df):
    """
    Verifica se il dataframe letto con header ha colonne riconoscibili
    (es. Matricola/Mat, Cognome, Nome, Data, TurnoE o Turno).
    """
    cols = [str(c).strip().lower() for c in df.columns]
    has_mat = any("matric" in c or c == "mat" for c in cols)
    has_cognome = any("cognome" in c for c in cols)
    has_nome = any("nome" in c for c in cols)
    has_data = any("data" in c for c in cols)
    has_turno = any("turno" in c for c in cols)
    return (has_mat and has_cognome and has_nome and has_turno) or (has_mat and has_cognome and has_nome and has_data)


def read_input_table(uploaded_file):
    """
    Legge il file .xls cercando di gestire sia file con header che senza header.
    Ritorna (df, had_header_flag)
    """
    # prima proviamo con header
    try:
        df_head = _read_xls_try_header(uploaded_file)
        if _has_expected_header_columns(df_head):
            return df_head, True
        # se header non sembra valido, fallback a no-header
    except Exception:
        # ignora e prova fallback
        pass

    # fallback: read without header
    df_no_head = _read_xls_no_header(uploaded_file)
    return df_no_head, False


# -----------------------
# Normalizzazione
# -----------------------
def normalize_df_with_headers(df):
    """
    Normalizza i nomi delle colonne quando il file ha intestazione.
    Standardizza colonne principali in: Mat, Cognome, Nome, Data_raw, TurnoE, Turno (se esistente), Minuti, ...
    Aggiunge Turno_tokens: lista dei token estratti dal campo turno da usare per la mappatura.
    """
    df = df.copy()
    col_map = {}
    for c in df.columns:
        c_norm = str(c).strip().lower()
        # Matricola
        if "matric" in c_norm or c_norm == "mat":
            col_map[c] = "Mat"
        elif "cognome" in c_norm:
            col_map[c] = "Cognome"
        elif "nome" in c_norm:
            col_map[c] = "Nome"
        elif "data" in c_norm:
            col_map[c] = "Data_raw"
        # cerchiamo "TurnoE" o colonne tipo TurnoE, Turno C/E etc.
        elif "turnoe" in c_norm or (c_norm.startswith("turno") and c_norm.endswith("e")):
            col_map[c] = "TurnoE"
        elif c_norm.startswith("turno"):
            # generic TurnoC o TurnoE o Turno
            # manteniamo come TurnoGeneric
            col_map[c] = "TurnoGeneric"
        elif "minut" in c_norm:
            col_map[c] = "Minuti"
        else:
            # lascio il nome originale per eventuali colonne aggiuntive (Residenza, Gruppo, Indennità, ...)
            col_map[c] = c
    df = df.rename(columns=col_map)

    # Tipi base
    if "Mat" in df.columns:
        df['Mat'] = df['Mat'].astype(str).str.strip()
    else:
        # se Mat non c'è creiamo colonna vuota per uniformità
        df['Mat'] = ""

    # scegliamo quale campo turno usare (preferenza TurnoE, altrimenti TurnoGeneric, altrimenti Turno)
    turno_field = None
    if 'TurnoE' in df.columns:
        turno_field = 'TurnoE'
    elif 'TurnoGeneric' in df.columns:
        turno_field = 'TurnoGeneric'
    else:
        # cerca colonne che contengono "turno" nel nome originale
        for c in df.columns:
            if 'turno' in str(c).lower():
                turno_field = c
                break

    # Normalizziamo campo turno e creiamo tokens
    if turno_field:
        df['Turno_raw'] = df[turno_field].astype(str).fillna("").str.strip()
        df['Turno_tokens'] = df['Turno_raw'].apply(lambda s: extract_turno_tokens(s))
    else:
        df['Turno_raw'] = ""
        df['Turno_tokens'] = [[] for _ in range(len(df))]

    return df


def normalize_df_no_header(df):
    """
    Normalizzazione per dataframe già convertito dal no-header reader.
    Si assume colonne: Mat, Cognome, Nome, Qualifica, Data, Giorno, Turno, Minuti
    """
    df = df.copy()
    # rinominiamo coerentemente
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
    # applica solo se colonna esiste
    cols_present = {c: rename_map[c] for c in rename_map if c in df.columns}
    df = df.rename(columns=cols_present)

    # tipi base
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


# -----------------------
# Tokenization e mappatura
# -----------------------
def extract_turno_tokens(raw_field):
    """
    Estrae i token alfanumerici dal campo raw_field, mantenendo ordine di apparizione.
    Separatore: qualsiasi carattere non alfanumerico.
    Esempio:
      "M158 13:05 20:20" -> ["M158", "13", "05", "20", "20"]
    Nota: gli orari vengono scomposti — la mappatura cercherà token alpha/numeric che corrispondono ai codici.
    """
    if raw_field is None:
        return []
    s = str(raw_field).strip()
    if s == "":
        return []
    # split su sequenze di caratteri non alfanumerici
    tokens = re.split(r'[^A-Za-z0-9]+', s)
    # filtra token vuoti
    return [t for t in tokens if t]


def map_tokens_to_category(tokens, code_to_cat):
    """
    Cerca il primo token che corrisponde a una chiave di code_to_cat (case-insensitive).
    Restituisce (matched_token, category) oppure (None, None).
    Prima corrispondenza (first-match). Nessuna verifica/trasformazione aggiuntiva.
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
# Data handling (no validation)
# -----------------------
def build_date_representation(data_raw, month=None, year=None):
    """
    Costruisce una rappresentazione stringa della data senza effettuare verifiche complesse.
    - Se data_raw è una data parseable -> dd/mm/YYYY
    - Se data_raw è giorno numerico e month/year forniti -> dd/mm/YYYY
    - Altrimenti -> restituisce la stringa originale (strip)
    """
    if pd.isna(data_raw):
        return ""
    # prova parse diretto
    try:
        dt = pd.to_datetime(data_raw, dayfirst=True, errors='coerce')
        if not pd.isna(dt):
            return dt.strftime('%d/%m/%Y')
    except Exception:
        pass
    # prova a usare giorno numerico con month/year forniti
    try:
        day = int(str(data_raw).strip())
        if month is not None and year is not None:
            try:
                dt = datetime(year=year, month=month, day=day)
                return dt.strftime('%d/%m/%Y')
            except Exception:
                # non validiamo, ritorniamo solo giorno come stringa
                return str(day)
        else:
            return str(day)
    except Exception:
        return str(data_raw)


def infer_month_string_from_dates(df):
    """
    Cerca la prima data parseable nella colonna Data_parsed e restituisce "NomeMese YYYY".
    """
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
    # fallback: cerca valori in Data_raw parseable
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
# Main processing
# -----------------------
def process_workbook(uploaded_file, code_to_cat, infer_month=False, month_for_days=None, year_for_days=None):
    """
    Funzione principale.
    - uploaded_file: file-like (Streamlit upload) o percorso cui pandas può leggere.
    - code_to_cat: dict codice->categoria (caricato da load_codes_map).
    - infer_month: se True prova a inferire il mese dalle date parseable.
    - month_for_days/year_for_days: se forniti e la colonna Data è solo giorno numerico, vengono usati per costruire la data.
    Ritorna: (grouped_df, normalized_raw_df, month_string)
    grouped_df colonne: Category, Mat, Cognome, Nome, Dates, DaysCount, RawTurns
    """
    raw_df, had_header = read_input_table(uploaded_file)

    # Normalizzazione
    if had_header:
        df = normalize_df_with_headers(raw_df)
    else:
        df = normalize_df_no_header(raw_df)

    # Costruzione Data_repr e Data_parsed (non validating)
    if 'Data_raw' in df.columns:
        df['Data_repr'] = df['Data_raw'].apply(lambda v: build_date_representation(v, month_for_days, year_for_days))
        # Data_parsed prova parse diretto (potrebbe essere NaT)
        df['Data_parsed'] = pd.to_datetime(df['Data_raw'], dayfirst=True, errors='coerce')
        # se Data_parsed è NaT e abbiamo month/year e Data_raw è giorno -> proviamo costruzione (senza validazione)
        def maybe_build_parsed(v):
            try:
                if pd.isna(pd.to_datetime(v, dayfirst=True, errors='coerce')):
                    # v potrebbe essere giorno numerico
                    day = int(str(v).strip())
                    if month_for_days is not None and year_for_days is not None:
                        try:
                            return pd.Timestamp(datetime(year=year_for_days, month=month_for_days, day=day))
                        except Exception:
                            return pd.NaT
                else:
                    return pd.to_datetime(v, dayfirst=True, errors='coerce')
            except Exception:
                return pd.NaT
        # Applichiamo la costruzione solo se month/year forniti
        if month_for_days is not None and year_for_days is not None:
            df['Data_parsed'] = df['Data_raw'].apply(lambda v: maybe_build_parsed(v))
    else:
        df['Data_repr'] = ""
        df['Data_parsed'] = pd.NaT

    # Mappatura tokens -> categoria
    def map_row_tokens(tokens):
        code, cat = map_tokens_to_category(tokens, code_to_cat)
        return pd.Series({"MatchedCode": code, "Category": cat})

    if 'Turno_tokens' in df.columns:
        mapped = df['Turno_tokens'].apply(lambda toks: map_row_tokens(toks))
        df = pd.concat([df, mapped], axis=1)
    else:
        df['MatchedCode'] = None
        df['Category'] = None

    # Manteniamo solo righe con Category non nullo (estrazione delle diciture di assenza)
    df_valid = df[df['Category'].notnull()].copy()

    # Ordinamento: Category -> Mat (numeric se possibile) -> Data_parsed (se presente) -> Data_repr
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

    # Aggregazione: Dates (Data_repr unici in ordine), DaysCount, RawTurns (unici)
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

    # infer month string se richiesto
    month_string = None
    if infer_month:
        month_string = infer_month_string_from_dates(df_valid)

    # pulizia colonne ausiliarie
    if 'Mat_sort_key' in grouped.columns:
        grouped = grouped.drop(columns=['Mat_sort_key'])
    if 'Mat_sort_key' in df_valid.columns:
        df_valid = df_valid.drop(columns=['Mat_sort_key'])

    # ritorniamo grouped, df normalizzato completo (non filtrato) e month_string (potrebbe essere None)
    return grouped, df, month_string


# -----------------------
# Generazione PDF (import reportlab localmente)
# -----------------------
def to_pdf_bytes(grouped_df, month_string=""):
    """
    Genera PDF e ritorna bytes.
    L'import di reportlab è fatto dentro la funzione per evitare ModuleNotFoundError
    al momento dell'import del modulo processor se reportlab non è installato.
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
