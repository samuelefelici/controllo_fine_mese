# app.py
import streamlit as st
import pandas as pd
from io import BytesIO, StringIO
import csv

st.set_page_config(page_title="Controllo Paghe", layout="wide")
st.title("Controllo Paghe")

st.markdown("Carica un file (anche se ha estensione .xls/.xlsx ma è in realtà un file di testo). L'app proverà a rilevarne il formato e mostrerà le prime 19 righe.")

uploaded_file = st.file_uploader("Carica file (xls, xlsx o file testo salvato come .xls)", type=["xls", "xlsx", "csv", "txt"])

def try_read_excel(raw_bytes):
    try:
        sheets = pd.read_excel(BytesIO(raw_bytes), sheet_name=None, engine=None)
        # Se è un dict (più fogli), prendi il primo foglio per la preview
        if isinstance(sheets, dict):
            first_sheet = list(sheets.keys())[0]
            return sheets[first_sheet], f"excel (foglio '{first_sheet}')"
        return sheets, "excel"
    except Exception:
        return None, None

def detect_text_format_and_read(raw_bytes):
    # prova vari encoding comuni
    encodings = ["utf-8", "utf-16", "cp1252", "latin1"]
    text = None
    used_enc = None
    for enc in encodings:
        try:
            text = raw_bytes.decode(enc)
            used_enc = enc
            break
        except Exception:
            continue
    if text is None:
        # fallback permissivo
        text = raw_bytes.decode("latin1", errors="replace")
        used_enc = "latin1 (fallback, with replace)"

    sample = text.lstrip()[:1000].lower()

    # HTML?
    if sample.startswith("<html") or "<table" in sample:
        try:
            dfs = pd.read_html(StringIO(text))
            if dfs:
                return dfs[0], f"html (parsed via read_html), encoding={used_enc}"
        except Exception:
            pass

    # XML (Spreadsheet)?
    if sample.startswith("<?xml"):
        try:
            # pandas.read_xml può funzionare per alcuni XML tabulari
            df = pd.read_xml(StringIO(text))
            return df, f"xml, encoding={used_enc}"
        except Exception:
            pass

    # SYLK (starts with ID;)
    if sample.startswith("id;"):
        try:
            df = pd.read_csv(StringIO(text), sep=";", engine="python", encoding=used_enc)
            return df, f"sylk-like (read as ;-sep), encoding={used_enc}"
        except Exception:
            pass

    # Prova a sniffare il separatore con csv.Sniffer
    first_lines = "\n".join(text.splitlines()[:20])
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(first_lines)
        sep = dialect.delimiter
        # se separatore è whitespace, preferiamo tab o comma
        if sep.isspace():
            sep = "\t"
    except Exception:
        # heuristics: prefer tab se contiene \t, altrimenti ; o ,
        if "\t" in first_lines:
            sep = "\t"
        elif ";" in first_lines:
            sep = ";"
        elif "," in first_lines:
            sep = ","
        else:
            # fallback: split by whitespace
            sep = r"\s+"

    # prova lettura con il separatore trovato, poi con altri comuni
    tried = []
    for sep_try in [sep, "\t", ";", ",", r"\s+"]:
        if sep_try in tried:
            continue
        tried.append(sep_try)
        try:
            if sep_try == r"\s+":
                df = pd.read_csv(StringIO(text), sep=r"\s+", engine="python", encoding=used_enc)
            else:
                df = pd.read_csv(StringIO(text), sep=sep_try, engine="python", encoding=used_enc)
            return df, f"text, sep='{sep_try}', encoding={used_enc}"
        except Exception:
            continue

    # Se tutto fallisce, ritorna None
    return None, f"unknown text format, tried encodings={encodings}"

def detect_and_read(uploaded):
    raw = uploaded.read()
    # Reset file pointer not necessary perché abbiamo letto tutto in raw
    # 1) prova come vero excel
    df, info = try_read_excel(raw)
    if df is not None:
        return df, info
    # 2) prova come testo/CSV/TSV/HTML/XML/SYLK
    df, info = detect_text_format_and_read(raw)
    return df, info

if uploaded_file is not None:
    df, info = detect_and_read(uploaded_file)
    if df is None:
        st.error(f"Impossibile determinare/leggere il formato del file. Info rilevate: {info}")
    else:
        st.success(f"File caricato. Formato rilevato: {info}")
        st.subheader("Anteprima (prime 19 righe)")
        try:
            st.dataframe(df.head(19))
        except Exception:
            st.write(df.head(19))

        # pulizia minima
        def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
            df_clean = df.copy()
            df_clean.columns = [c.strip() if isinstance(c, str) else c for c in df_clean.columns]
            def _strip_val(x):
                return x.strip() if isinstance(x, str) else x
            df_clean = df_clean.applymap(_strip_val)
            return df_clean

        df_clean = clean_dataframe(df)
        st.markdown("---")
        st.write(f"Dimensione dati: {df_clean.shape[0]} righe × {df_clean.shape[1]} colonne")
        st.download_button(
            label="Scarica CSV pulito",
            data=df_clean.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"{uploaded_file.name.rsplit('.',1)[0]}_pulito.csv",
            mime="text/csv",
        )
else:
    st.info("Carica un file per iniziare.")
