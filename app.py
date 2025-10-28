import streamlit as st
import pandas as pd
from io import StringIO, BytesIO
from pathlib import Path
from processor import process_workbook, to_pdf_bytes, load_codes_map
import csv

# ================================================================
# CONFIGURAZIONE BASE
# ================================================================
st.set_page_config(page_title="Controlli Fine Mese", layout="wide")
st.title("Controlli Fine Mese - Anteprima grezza del file (senza interpretare header)")

repo_root = Path(__file__).parent
codes_file = repo_root / "codes.csv"

st.markdown(
    "Carica il file (.xls/.xlsx/.csv/.txt). "
    "Questa pagina mostra il contenuto così com'è (grezzo): non interpretiamo la riga di intestazione, "
    "non applichiamo mappature, non scaliamo gli indici. Serve per verificare esattamente come pandas vede il foglio."
)

# ================================================================
# SIDEBAR (opzionale)
# ================================================================
st.sidebar.header("Opzioni")
st.sidebar.markdown(f"File di mappatura codici: `{codes_file.name}` (non usato qui)")

# ================================================================
# FILE UPLOADER
# ================================================================
uploaded_file = st.file_uploader(
    "Carica il file (.xls/.xlsx/.csv/.txt)",
    type=["xls", "xlsx", "csv", "txt"],
    accept_multiple_files=False
)

if uploaded_file is None:
    st.info("Carica un file per iniziare.")
    st.stop()

# ================================================================
# LETTURA GREZZA E ANTEPRIMA (NESSUNA INTERPRETAZIONE DELL'HEADER)
# ================================================================
with st.spinner("Caricamento grezzo in corso..."):
    # Leggiamo i bytes una volta sola
    try:
        raw_bytes = uploaded_file.read()
    except Exception:
        try:
            raw_bytes = uploaded_file.getvalue()
        except Exception as e:
            st.error(f"Impossibile leggere il file caricato: {e}")
            st.stop()

    df = None
    detected_format = None
    # 1) Proviamo a leggere come Excel ma SENZA interpretare la prima riga come header:
    try:
        # tentativo con openpyxl (xlsx)
        df = pd.read_excel(BytesIO(raw_bytes), header=None, engine="openpyxl", dtype=str)
        detected_format = "Excel (letto con header=None - openpyxl)"
    except Exception:
        try:
            # tentativo auto engine (pandas sceglie)
            df = pd.read_excel(BytesIO(raw_bytes), header=None, dtype=str)
            detected_format = "Excel (letto con header=None - engine auto)"
        except Exception:
            try:
                # tentativo con xlrd per .xls (se presente nell'ambiente)
                df = pd.read_excel(BytesIO(raw_bytes), header=None, engine="xlrd", dtype=str)
                detected_format = "Excel .xls (letto con header=None - xlrd)"
            except Exception:
                df = None

    # 2) Se non è Excel, proviamo come testo/CSV e NON interpretiamo header (header=None)
    if df is None:
        try:
            # decodifica testo per provare sniffing
            try:
                text = raw_bytes.decode("utf-8")
            except UnicodeDecodeError:
                text = raw_bytes.decode("latin-1", errors="replace")

            # proviamo a usare csv.Sniffer per determinare il separatore più probabile
            try:
                sniffer = csv.Sniffer()
                dialect = sniffer.sniff(text[:4096])
                sep = dialect.delimiter
            except Exception:
                # fallback a tab, ;, ,
                for candidate in ["\t", ";", ","]:
                    try:
                        tmp = pd.read_csv(StringIO(text), sep=candidate, header=None, nrows=2, dtype=str)
                        sep = candidate
                        break
                    except Exception:
                        sep = None
                if sep is None:
                    sep = ","  # ultima risorsa

            df = pd.read_csv(StringIO(text), sep=sep, header=None, dtype=str)
            detected_format = f"Testo/CSV (sep='{sep}') - letto con header=None"
        except Exception as e:
            st.error(f"Impossibile interpretare il file come Excel o CSV/TXT: {e}")
            st.stop()

    # Normalizziamo i NaN a stringa vuota per una visualizzazione pulita
    df = df.fillna("")

st.success(f"Caricato: {detected_format}")
st.write("✅ Anteprima grezza (prime 20 righe, header NON interpretato):")
st.dataframe(df.head(20), use_container_width=True)

st.write("Informazioni grezze:")
st.write(f"Dimensione DataFrame: {df.shape[0]} righe × {df.shape[1]} colonne")
st.write("Colonne (index così come pandas le riporta):")
for i, c in enumerate(df.columns):
    st.write(f"{i}: {c!s}")

st.write("Prime 20 righe (visualizzazione testuale):")
rows_preview = df.head(20).astype(str).values.tolist()
for r in rows_preview:
    st.text(" | ".join(r))

st.markdown("---")
st.info("Questa vista mostra il file così com'è: la prima riga del foglio viene mostrata come riga 0 (non come intestazione). "
        "Se vedi che le intestazioni sono già in una riga interna, ora puoi indicarmi qual è la riga giusta o chiedermi di rilevare automaticamente la riga di header e rieffettuare l'anteprima interpretata.")

# Opzione: procedere con l'elaborazione normale usando il BytesIO originale
if st.button("Procedi con l'elaborazione (usando il file caricato)"):
    with st.spinner("Elaborazione in corso..."):
        try:
            file_for_processing = BytesIO(raw_bytes)
            file_for_processing.seek(0)
            grouped_df, df_valid, inferred_month_str = process_workbook(
                file_for_processing,
                load_codes_map(codes_file)[0] if codes_file.exists() else {},
                infer_month=True,
                month_for_days=None,
                year_for_days=None
            )
            st.success("Elaborazione completata.")
            st.subheader("Anteprima riepilogo (aggregato)")
            st.dataframe(grouped_df, use_container_width=True)
            st.subheader("Dati validi (anteprima)")
            st.dataframe(df_valid.head(200), use_container_width=True)
        except Exception as e:
            st.error(f"Errore durante l'elaborazione: {e}")
