import streamlit as st
import pandas as pd
from io import BytesIO, StringIO
import csv
import chardet
import re
from pathlib import Path

st.set_page_config(page_title="Controllo Paghe", layout="wide")
st.title("Controllo Paghe")

st.markdown(
    "Carica il file. Verranno mostrate in anteprima SOLO le colonne: Matricola, Cognome, Nome, Data, "
    "giorno (colonna subito dopo Data) e TurnoE. Dopo aver selezionato le categorie di assenza, "
    "l'app mostrerà per ciascuna categoria (selezionata) l'elenco dei conducenti e i giorni in cui hanno avuto quella tipologia."
)

# ---------- configurazioni / utility ----------
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


# ---------- codes.csv loader ----------
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


# ---------- selezione colonne richieste ----------
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


# ---------- UI e flusso ----------
uploaded_file = st.file_uploader(
    "Carica file (xls, xlsx, csv, txt - anche se ha estensione .xls)",
    type=["xls", "xlsx", "csv", "txt"],
)

codes_map = load_codes_file()
normalized_code_map = build_normalized_code_map(codes_map)
categories_available = sorted(list(codes_map.keys())) if codes_map else []

if not categories_available:
    st.warning("Attenzione: non è stato trovato 'codes.csv' nella repo. Le categorie non saranno disponibili.")
else:
    st.markdown("Seleziona le categorie di assenza da visualizzare (verranno mostrate SEPARATAMENTE, una categoria dopo l'altra):")
    chosen_categories = st.multiselect("Categorie", options=categories_available, default=[])

if uploaded_file is None:
    st.info("Carica un file per iniziare.")
    st.stop()

# lettura file (excel o testo) - detection automatica, senza mostrare candidate all'utente
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
    st.info("Seleziona una o più categorie per visualizzare gli elenchi per categoria.")
    st.stop()

# --- per ogni categoria selezionata: costruisco e mostro il risultato separatamente ---
for cat in chosen_categories:
    st.markdown("---")
    st.header(f"Categoria: {cat}")
    # costruisco set dei codici normalizzati per questa singola categoria
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
        st.write(f"Nessuna occorrenza trovata per la categoria '{cat}'.")
        continue

    result_df = pd.DataFrame(rows)

    # ordina per matricola (numerica se possibile) e data crescente
    def matricola_sort_key(v):
        try:
            return int(str(v).strip())
        except Exception:
            return str(v).zfill(10)

    result_df["_mat_sort"] = result_df["Matricola"].apply(matricola_sort_key)
    result_df = result_df.sort_values(by=["_mat_sort", "_sort_data"])

    # raggruppa e mostra per dipendente
    for mat, group in result_df.groupby("Matricola", sort=False):
        first = group.iloc[0]
        cogn = first.get("Cognome", "")
        nom = first.get("Nome", "")
        st.subheader(f"{mat}  {cogn} {nom}")
        grp_sorted = group.sort_values("_sort_data")
        for _, row in grp_sorted.iterrows():
            giorno = str(row.get("giorno", "")).strip()
            data_val = row.get("Data", "")
            turno_code = row.get("TurnoE_matched", "")
            st.text(f"{giorno} {data_val} {turno_code}")

    # download CSV per categoria
    csv_bytes = result_df.drop(columns=["_mat_sort", "_sort_data"], errors="ignore").to_csv(index=False).encode("utf-8-sig")
    st.download_button(f"Scarica CSV per categoria '{cat}'", csv_bytes, file_name=f"{uploaded_file.name.rsplit('.',1)[0]}_{cat}_assenze.csv", mime="text/csv")
