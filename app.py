# app.py
import streamlit as st
import pandas as pd
from io import BytesIO, StringIO
import csv
import chardet
import re
from pathlib import Path

# reportlab import guard (mostro snippet come richiesto)
try:
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, KeepTogether
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import mm
    HAS_REPORTLAB = True
except Exception:
    HAS_REPORTLAB = False

st.set_page_config(page_title="Controllo Paghe", layout="wide")
st.title("Controllo Paghe")

st.markdown(
    "Carica il file. Verranno mostrate in anteprima SOLO le colonne: Matricola, Cognome, Nome, Data, "
    "giorno (colonna subito dopo Data) e TurnoE. Poi scegli Mese/Anno e le Categorie da elaborare e clicca 'Elabora' "
    "per ottenere un PDF con, per ciascuna categoria selezionata, l'elenco per conducente dei giorni trovati."
)

# -------------------- utilities e parsing robusto (come prima) --------------------
KEYWORDS = ["Residenza", "Matricola", "Cognome", "Nome", "Gruppo", "Data", "Turno", "Inizio", "Fine"]
ENC_CANDIDATES = [
    "utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be",
    "cp1252", "iso-8859-1", "latin1", "cp1250",
]
TOKEN_RE = re.compile(r"[A-Za-z0-9/\.]+")  # token extractor per TurnoE


def try_read_excel(raw_bytes):
    try:
        sheets = pd.read_excel(BytesIO(raw_bytes), sheet_name=None, engine=None)
        if isinstance(sheets, dict):
            return list(sheets.values())[0]
        return sheets
    except Exception:
        return None


def detect_with_chardet(raw_bytes: bytes):
    try:
        res = chardet.detect(raw_bytes)
        return res.get("encoding"), res.get("confidence", 0.0)
    except Exception:
        return None, 0.0


def score_decoded_text(text: str) -> int:
    t = text.lower()
    score = 0
    for kw in KEYWORDS:
        score += t.count(kw.lower())
    score -= text.count("�") * 5
    return score


def generate_encoding_candidates(raw_bytes: bytes, n_top=4):
    detected_enc, _ = detect_with_chardet(raw_bytes)
    candidates = list(ENC_CANDIDATES)
    if detected_enc:
        de = detected_enc.lower()
        if de not in candidates:
            candidates.insert(0, de)
        else:
            candidates.remove(de)
            candidates.insert(0, de)

    sample_bytes = raw_bytes[:8000]
    scored = []
    for enc in candidates:
        try:
            decoded = sample_bytes.decode(enc)
        except Exception:
            try:
                decoded = sample_bytes.decode(enc, errors="replace")
            except Exception:
                continue
        sc = score_decoded_text(decoded)
        non_printable = sum(1 for ch in decoded if ord(ch) < 9 or (11 <= ord(ch) <= 31))
        scored.append({"encoding": enc, "score": sc, "snippet": decoded[:1000], "non_print": non_printable})
    scored_sorted = sorted(scored, key=lambda x: (-x["score"], x["non_print"]))
    return scored_sorted[:n_top]


def guess_separator_from_text(text: str):
    lines = "\n".join(text.splitlines()[:20])
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(lines)
        sep = dialect.delimiter
        if sep.isspace():
            sep = "\t"
    except Exception:
        if "\t" in lines:
            sep = "\t"
        elif ";" in lines:
            sep = ";"
        elif "," in lines:
            sep = ","
        else:
            sep = r"\s+"
    return sep


def parse_rows_with_sep(decoded_text: str, sep_choice: str):
    lines = [ln for ln in decoded_text.splitlines() if ln.strip() != ""]
    if not lines:
        return []
    if sep_choice in (r"\\t", r"\t", "\\t", "\t"):
        delimiter = "\t"
        reader = csv.reader(lines, delimiter=delimiter)
        rows = [row for row in reader]
    elif sep_choice == r"\s+":
        rows = [re.split(r'\s+', line.strip()) for line in lines]
    else:
        delimiter = sep_choice
        reader = csv.reader(lines, delimiter=delimiter)
        rows = [row for row in reader]
    return rows


def robust_rows_to_df(rows):
    if not rows:
        return pd.DataFrame()
    header = [h.strip() for h in rows[0]]
    hdr_len = len(header)
    processed = []
    for row in rows[1:]:
        if all((cell is None or str(cell).strip() == "") for cell in row):
            continue
        if len(row) < hdr_len:
            row = row + [""] * (hdr_len - len(row))
        elif len(row) > hdr_len:
            row = row[:hdr_len-1] + [" ".join([str(x).strip() for x in row[hdr_len-1:]])]
        if len(row) != hdr_len:
            row = (row + [""] * hdr_len)[:hdr_len]
        processed.append([str(x).strip() for x in row])
    df = pd.DataFrame(processed, columns=header)
    return df


def robust_read_text_to_df(decoded_text: str, sep_choice: str):
    rows = parse_rows_with_sep(decoded_text, sep_choice)
    if not rows:
        raise ValueError("Nessuna riga trovata nel testo.")
    df = robust_rows_to_df(rows)
    return df


def clean_dataframe_strings(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()
    df_clean.columns = [c.strip() if isinstance(c, str) else c for c in df_clean.columns]
    def _strip_val(x):
        return x.strip() if isinstance(x, str) else x
    df_clean = df_clean.applymap(_strip_val)
    return df_clean


# -------------------- gestione colonne richieste --------------------
def select_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    cols = [str(c).strip() if c is not None else "" for c in df2.columns]
    df2.columns = cols

    idx_data = next((i for i, c in enumerate(cols) if c.lower() == "data"), None)

    giorno_source = None
    if idx_data is not None and (idx_data + 1) < len(cols):
        giorno_source = cols[idx_data + 1]

    if giorno_source is None:
        empty_col = next((c for c in cols if c == ""), None)
        if empty_col is not None:
            giorno_source = empty_col
        else:
            daycol = next((c for c in cols if c.lower() == "giorno"), None)
            if daycol is not None:
                giorno_source = daycol

    def find_col(ci_name):
        return next((c for c in cols if c.lower() == ci_name.lower()), None)

    matricola_col = find_col("Matricola")
    cognome_col = find_col("Cognome")
    nome_col = find_col("Nome")
    data_col = find_col("Data")
    turnoe_col = next((c for c in cols if c.lower().replace(" ", "") in ("turnoe", "turnoe", "turnoe")), None)

    result = pd.DataFrame()
    result["Matricola"] = df2[matricola_col] if matricola_col in df2.columns and matricola_col else ""
    result["Cognome"] = df2[cognome_col] if cognome_col in df2.columns and cognome_col else ""
    result["Nome"] = df2[nome_col] if nome_col in df2.columns and nome_col else ""
    result["Data"] = df2[data_col] if data_col in df2.columns and data_col else ""
    if giorno_source is not None and giorno_source in df2.columns:
        result["giorno"] = df2[giorno_source]
    else:
        result["giorno"] = ""
    result["TurnoE"] = df2[turnoe_col] if turnoe_col in df2.columns and turnoe_col else ""
    return result[["Matricola", "Cognome", "Nome", "Data", "giorno", "TurnoE"]]


# -------------------- codes.csv loader --------------------
def load_codes_file() -> dict:
    base = Path(__file__).parent if "__file__" in globals() else Path.cwd()
    candidates = [base / "codes.csv", Path.cwd() / "codes.csv"]
    for p in candidates:
        if p.exists():
            try:
                codes_df = pd.read_csv(p)
                mapping = {}
                for _, r in codes_df.iterrows():
                    cat = str(r["Category"]).strip()
                    codes_raw = str(r["Codes"]).strip() if not pd.isna(r["Codes"]) else ""
                    codes = [c.strip() for c in codes_raw.split(";") if c.strip() != ""]
                    mapping[cat] = codes
                return mapping
            except Exception:
                return {}
    return {}


def build_normalized_code_map(codes_map: dict) -> dict:
    norm_map = {}
    for cat, codes in codes_map.items():
        for code in codes:
            norm = re.sub(r'[^A-Za-z0-9]', '', code).upper()
            if not norm:
                continue
            norm_map.setdefault(norm, []).append(cat)
    return norm_map


def extract_matched_codes(cell_value: str, normalized_code_map: dict):
    if not cell_value or str(cell_value).strip() == "":
        return []
    text = str(cell_value)
    tokens = TOKEN_RE.findall(text)
    matched = []
    for tok in tokens:
        norm = re.sub(r'[^A-Za-z0-9]', '', tok).upper()
        if not norm:
            continue
        cats = normalized_code_map.get(norm)
        if cats:
            matched.append((tok.strip(), norm, cats))
    return matched


# -------------------- PDF generation (tabellare per conducente) --------------------
def generate_pdf_for_categories_table(category_results: dict, month_name: str, year: str) -> bytes:
    """
    category_results: dict category -> DataFrame (with columns Matricola,Cognome,Nome,Data,giorno,TurnoE_matched,_sort_data)
    Produce un PDF dove per ogni categoria c'è un blocco: titolo e poi per ogni conducente una tabella
    | Matricola Nome |
    | Data | Giorno | Assenza |
    | ... rows ... |
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                            leftMargin=15*mm, rightMargin=15*mm, topMargin=15*mm, bottomMargin=15*mm)
    styles = getSampleStyleSheet()
    style_title = styles["Heading2"]
    style_cat = styles["Heading3"]
    style_normal = styles["Normal"]

    elements = []

    for cat, df_cat in category_results.items():
        # category header
        elements.append(Paragraph(f"{cat} — {month_name} {year}", style_cat))
        elements.append(Spacer(1, 4))

        if df_cat.empty:
            elements.append(Paragraph("Nessuna occorrenza trovata per questa categoria.", style_normal))
            elements.append(PageBreak())
            continue

        # group by matricola
        for mat, group in df_cat.groupby("Matricola", sort=False):
            first = group.iloc[0]
            cogn = first.get("Cognome", "")
            nom = first.get("Nome", "")
            # table data: first a single-cell row with matricola + name (we'll style it)
            header_cell = f"{mat}   {cogn} {nom}"
            # table body: header row then data rows
            table_data = []
            table_data.append([header_cell])  # single cell header (span later)
            table_data.append(["Data", "Giorno", "Assenza"])
            grp_sorted = group.sort_values("_sort_data")
            for _, row in grp_sorted.iterrows():
                data_val = str(row.get("Data", "")).strip()
                giorno = str(row.get("giorno", "")).strip()
                turno_code = str(row.get("TurnoE_matched", "")).strip()
                table_data.append([data_val, giorno, turno_code])

            # create table: first row 1 column, subsequent rows 3 columns -> normalize by expanding first row with colspan
            # We'll build a table with maximum 3 columns; for the first row, set colspan to 3 by duplicating the value
            tbl_rows = []
            for i, r in enumerate(table_data):
                if i == 0:
                    tbl_rows.append([r[0], "", ""])
                else:
                    # ensure row length is 3
                    if len(r) == 1:
                        tbl_rows.append([r[0], "", ""])
                    elif len(r) == 3:
                        tbl_rows.append(r)
                    else:
                        # pad/truncate
                        row3 = (r + ["", "", ""])[:3]
                        tbl_rows.append(row3)

            tbl = Table(tbl_rows, colWidths=[45*mm, 35*mm, 90*mm])
            # style
            tbl_style = TableStyle([
                ('GRID', (0,0), (-1,-1), 0.5, colors.black),
                ('SPAN', (0,0), (-1,0)),  # first row spans all columns
                ('BACKGROUND', (0,1), (-1,1), colors.lightgrey),  # header row background
                ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                ('ALIGN', (0,1), (-1,1), 'CENTER'),
                ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
                ('LEFTPADDING', (0,0), (-1,-1), 4),
                ('RIGHTPADDING', (0,0), (-1,-1), 4),
                ('FONTSIZE', (0,0), (-1,-1), 9),
            ])
            tbl.setStyle(tbl_style)

            # keep together the table for readability
            elements.append(KeepTogether(tbl))
            elements.append(Spacer(1, 4))

        # page break after category
        elements.append(PageBreak())

    doc.build(elements)
    buffer.seek(0)
    return buffer.read()


# -------------------- UI principale --------------------
uploaded_file = st.file_uploader(
    "Carica file (xls, xlsx, csv, txt - anche se ha estensione .xls)",
    type=["xls", "xlsx", "csv", "txt"],
)

codes_map = load_codes_file() if 'load_codes_file' in globals() else {}
# if previous helper functions defined elsewhere, ensure they exist; otherwise re-define minimal loaders:
if not codes_map:
    # try loading using local function defined earlier in conversation scope
    from pathlib import Path
    def load_codes_file_local():
        base = Path(__file__).parent if "__file__" in globals() else Path.cwd()
        p = base / "codes.csv"
        if p.exists():
            try:
                df = pd.read_csv(p)
                mapping = {}
                for _, r in df.iterrows():
                    cat = str(r["Category"]).strip()
                    codes_raw = str(r["Codes"]).strip() if not pd.isna(r["Codes"]) else ""
                    codes = [c.strip() for c in codes_raw.split(";") if c.strip() != ""]
                    mapping[cat] = codes
                return mapping
            except Exception:
                return {}
        return {}
    codes_map = load_codes_file_local()

normalized_code_map = build_normalized_code_map(codes_map) if 'build_normalized_code_map' in globals() else {}
categories_available = sorted(list(codes_map.keys())) if codes_map else []

if not categories_available:
    st.warning("Attenzione: non è stato trovato 'codes.csv' nella repo. Le categorie non saranno disponibili.")
else:
    st.markdown("Seleziona le categorie di assenza da visualizzare (verranno elaborate SEPARATAMENTE, una categoria dopo l'altra):")
    chosen_categories = st.multiselect("Categorie", options=categories_available, default=[])

# Mese e Anno input
mesi = [
    "Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
    "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre"
]
mese_scelto = st.selectbox("Mese", options=mesi, index=0)
anno_input = st.text_input("Anno", value=str(pd.Timestamp.now().year))

if uploaded_file is None:
    st.info("Carica un file per iniziare.")
    st.stop()

# lettura file (excel o testo) - detection automatica
raw = uploaded_file.read()
df_excel = try_read_excel(raw)
if df_excel is not None:
    df = df_excel
else:
    candidates = generate_encoding_candidates(raw, n_top=4)
    top_enc = candidates[0]["encoding"] if candidates else "utf-8"
    snippet = candidates[0]["snippet"] if candidates else raw.decode("latin1", errors="replace")[:1000]
    guessed_sep = guess_separator_from_text(snippet)
    enc_to_use = top_enc
    sep_to_use = guessed_sep
    try:
        decoded_full = raw.decode(enc_to_use)
    except Exception:
        decoded_full = raw.decode(enc_to_use, errors="replace")
    df = robust_read_text_to_df(decoded_full, sep_to_use)

df = clean_dataframe_strings(df)
df_sel = select_required_columns(df)

# preparazione valore ordinabile per Data
def sortable_date_value(v):
    try:
        return int(str(v).strip())
    except Exception:
        pass
    try:
        dt = pd.to_datetime(v, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return str(v)
        return dt
    except Exception:
        return str(v)

df_sel["_sort_data"] = df_sel["Data"].apply(sortable_date_value)

st.subheader("Anteprima (prime 19 righe) — colonne richieste")
st.dataframe(df_sel.head(19))

if not categories_available:
    st.stop()

if not chosen_categories:
    st.info("Seleziona una o più categorie per poter generare il PDF.")
    st.stop()

# se manca reportlab, segnalo e interrompo la generazione PDF ma lascio il resto funzionare
if not HAS_REPORTLAB:
    st.error(
        "La libreria 'reportlab' non è assente nell'ambiente. Per generare il PDF aggiungi 'reportlab>=3.6.12' a requirements.txt e riavvia l'app (o esegui 'pip install reportlab')."
    )
    st.stop()

# bottone Elabora
if st.button("Elabora"):
    category_results = {}
    for cat in chosen_categories:
        selected_norm_codes = set()
        for code in codes_map.get(cat, []):
            norm = re.sub(r'[^A-Za-z0-9]', '', code).upper()
            if norm:
                selected_norm_codes.add(norm)

        rows = []
        for _, r in df_sel.iterrows():
            turno_cell = r.get("TurnoE", "")
            matched = extract_matched_codes(turno_cell, normalized_code_map)
            matched_selected = [m for m in matched if m[1] in selected_norm_codes]
            if matched_selected:
                matched_tokens = [m[0] for m in matched_selected]
                rows.append({
                    "Matricola": r.get("Matricola", ""),
                    "Cognome": r.get("Cognome", ""),
                    "Nome": r.get("Nome", ""),
                    "Data": r.get("Data", ""),
                    "giorno": r.get("giorno", ""),
                    "TurnoE_matched": " ".join(matched_tokens),
                    "_sort_data": r.get("_sort_data")
                })

        if not rows:
            category_results[cat] = pd.DataFrame(columns=["Matricola", "Cognome", "Nome", "Data", "giorno", "TurnoE_matched", "_sort_data"])
            continue

        result_df = pd.DataFrame(rows)

        def matricola_sort_key(v):
            try:
                return int(str(v).strip())
            except Exception:
                return str(v).zfill(10)

        result_df["_mat_sort"] = result_df["Matricola"].apply(matricola_sort_key)
        result_df = result_df.sort_values(by=["_mat_sort", "_sort_data"])
        category_results[cat] = result_df

    # Genera PDF tabellare per categoria
    try:
        pdf_bytes = generate_pdf_for_categories_table(category_results, mese_scelto, anno_input)
        fname = f"{uploaded_file.name.rsplit('.',1)[0]}_{mese_scelto}_{anno_input}_assenze.pdf"
        st.success("PDF generato correttamente.")
        st.download_button("Scarica PDF", data=pdf_bytes, file_name=fname, mime="application/pdf")
    except Exception as e:
        st.error(f"Errore nella generazione del PDF: {e}")
