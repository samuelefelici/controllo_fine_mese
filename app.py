import streamlit as st
import pandas as pd
from io import BytesIO, StringIO
import csv
import chardet
import re

st.set_page_config(page_title="Controllo Paghe", layout="wide")
st.title("Controllo Paghe")

st.markdown(
    "Carica il file. Verranno mostrate in anteprima SOLO le colonne: Matricola, Cognome, Nome, Data, "
    "giorno (colonna subito dopo Data) e TurnoE. Encoding e separatore sono rilevati automaticamente."
)

uploaded_file = st.file_uploader(
    "Carica file (xls, xlsx, csv, txt - anche se ha estensione .xls)",
    type=["xls", "xlsx", "csv", "txt"],
)

KEYWORDS = ["Residenza", "Matricola", "Cognome", "Nome", "Gruppo", "Data", "Turno", "Inizio", "Fine"]
ENC_CANDIDATES = [
    "utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be",
    "cp1252", "iso-8859-1", "latin1", "cp1250",
]

def try_read_excel(raw_bytes):
    try:
        sheets = pd.read_excel(BytesIO(raw_bytes), sheet_name=None, engine=None)
        if isinstance(sheets, dict):
            first = list(sheets.keys())[0]
            return sheets[first]
        return sheets
    except Exception:
        return None

def score_decoded_text(text: str) -> int:
    t = text.lower()
    score = 0
    for kw in KEYWORDS:
        score += t.count(kw.lower())
    score -= text.count("�") * 5
    return score

def detect_with_chardet(raw_bytes: bytes):
    try:
        res = chardet.detect(raw_bytes)
        return res.get("encoding"), res.get("confidence", 0.0)
    except Exception:
        return None, 0.0

def generate_encoding_candidates(raw_bytes: bytes, n_top=4):
    detected_enc, _ = detect_with_chardet(raw_bytes)
    candidates = list(ENC_CANDIDATES)
    if detected_enc:
        detected_enc = detected_enc.lower()
        if detected_enc not in candidates:
            candidates.insert(0, detected_enc)
        else:
            candidates.remove(detected_enc)
            candidates.insert(0, detected_enc)

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

def select_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    cols = [str(c).strip() if c is not None else "" for c in df2.columns]
    df2.columns = cols

    # trova indice della colonna 'Data' (case-insensitive)
    idx_data = next((i for i, c in enumerate(cols) if c.lower() == "data"), None)

    # se 'Data' trovata, prendiamo la colonna immediatamente dopo (anche se ha intestazione vuota)
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

def clean_dataframe_strings(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()
    df_clean.columns = [c.strip() if isinstance(c, str) else c for c in df_clean.columns]
    def _strip_val(x):
        return x.strip() if isinstance(x, str) else x
    df_clean = df_clean.applymap(_strip_val)
    return df_clean

if uploaded_file is not None:
    raw = uploaded_file.read()
    # 1) prova come vero excel
    df_excel = try_read_excel(raw)
    if df_excel is not None:
        # excel vero: pulisco e mostro solo colonne richieste
        df_excel = clean_dataframe_strings(df_excel)
        df_sel = select_required_columns(df_excel)
        st.subheader("Anteprima (prime 19 righe) — colonne richieste")
        st.dataframe(df_sel.head(19))
        st.write(f"Dimensione dati (colonne richieste): {df_sel.shape[0]} righe × {df_sel.shape[1]} colonne")
        st.download_button(
            label="Scarica CSV (solo colonne richieste)",
            data=df_sel.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"{uploaded_file.name.rsplit('.',1)[0]}_selezione_colonne.csv",
            mime="text/csv",
        )
    else:
        # file testuale: rileviamo automaticamente encoding/separatore (ma NON mostriamo campi per forzarli)
        candidates = generate_encoding_candidates(raw, n_top=4)
        detected_enc, _ = detect_with_chardet(raw)
        top_enc = candidates[0]["encoding"] if candidates else (detected_enc or "utf-8")
        snippet = candidates[0]["snippet"] if candidates else (raw.decode("latin1", errors="replace")[:1000])
        guessed_sep = guess_separator_from_text(snippet)

        # usiamo automaticamente rilevamento (nessun campo visibile per forzare)
        enc_to_use = top_enc
        sep_to_use = guessed_sep

        try:
            decoded_full = raw.decode(enc_to_use)
        except Exception:
            decoded_full = raw.decode(enc_to_use, errors="replace")

        try:
            df = robust_read_text_to_df(decoded_full, sep_to_use)
            df = clean_dataframe_strings(df)
            df_sel = select_required_columns(df)
            st.subheader("Anteprima")
            st.dataframe(df_sel.head(19))
            st.write(f"Dimensione dati (colonne richieste): {df_sel.shape[0]} righe × {df_sel.shape[1]} colonne")
            st.download_button(
                label="Scarica CSV (solo colonne richieste)",
                data=df_sel.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"{uploaded_file.name.rsplit('.',1)[0]}_selezione_colonne.csv",
                mime="text/csv",
            )
        except Exception as e:
            st.error("Impossibile convertire il file in tabella.")
            st.write("Errore tecnico:", str(e))
else:
    st.info("Carica un file per iniziare.")
