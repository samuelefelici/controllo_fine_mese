import streamlit as st
import pandas as pd
from io import StringIO, BytesIO
from pathlib import Path
from processor import process_workbook, to_pdf_bytes, load_codes_map

# ================================================================
# CONFIGURAZIONE BASE
# ================================================================
st.set_page_config(page_title="Controlli Fine Mese", layout="wide")
st.title("Controlli Fine Mese - Estrazione diciture di assenza (PDF)")

repo_root = Path(__file__).parent
codes_file = repo_root / "codes.csv"

st.markdown(
    "Carica il file (.xls/.xlsx/.csv/.txt). "
    "L'app estrae le diciture di assenza e genera un PDF ordinato per categoria → matricola → data."
)

# ================================================================
# SIDEBAR
# ================================================================
st.sidebar.header("Opzioni")
st.sidebar.markdown(f"File di mappatura codici: `{codes_file.name}`")

show_raw = st.sidebar.checkbox("Mostra dati grezzi", value=False)
infer_month = st.sidebar.checkbox("Usa mese/anno forniti se 'Data' è solo giorno", value=True)

month_input = st.sidebar.selectbox(
    "Mese",
    [""] + [
        "Gennaio", "Febbraio", "Marzo", "Aprile", "Maggio", "Giugno",
        "Luglio", "Agosto", "Settembre", "Ottobre", "Novembre", "Dicembre"
    ]
)
year_input = st.sidebar.text_input("Anno (es. 2025)", value="")

# ================================================================
# CARICAMENTO CODES MAP
# ================================================================
if codes_file.exists():
    try:
        codes_map, categories_order = load_codes_map(codes_file)
    except Exception as e:
        st.error(f"Errore caricamento {codes_file.name}: {e}")
        codes_map, categories_order = {}, []
else:
    st.sidebar.warning(f"{codes_file.name} non trovato nella repo.")
    uploaded_codes = st.sidebar.file_uploader("Carica codes.csv (opzionale)", type=["csv"])
    if uploaded_codes:
        try:
            codes_map, categories_order = load_codes_map(uploaded_codes)
        except Exception as e:
            st.error(f"Errore caricamento file caricato: {e}")
            codes_map, categories_order = {}, []
    else:
        codes_map, categories_order = {}, []

st.sidebar.subheader("Categorie caricate")
st.sidebar.write(", ".join(categories_order) if categories_order else "Nessuna")

# ================================================================
# MESE NUMERICO
# ================================================================
months_it = {
    "Gennaio": 1, "Febbraio": 2, "Marzo": 3, "Aprile": 4, "Maggio": 5, "Giugno": 6,
    "Luglio": 7, "Agosto": 8, "Settembre": 9, "Ottobre": 10, "Novembre": 11, "Dicembre": 12
}
month_num = months_it.get(month_input)

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
# LETTURA FILE GREZZO (robusta, non consuma lo stream usato dopo)
# ================================================================
with st.spinner("Tentativo di lettura del file..."):
    # Leggiamo tutti i bytes una volta sola e poi creiamo BytesIO per anteprima + elaborazione.
    try:
        raw_bytes = uploaded_file.read()
    except Exception:
        # fallback: se non è possibile leggere direttamente, proviamo a usare uploaded_file.getvalue()
        try:
            raw_bytes = uploaded_file.getvalue()
        except Exception as e:
            st.error(f"Impossibile leggere il file caricato: {e}")
            st.stop()

    preview_df = None
    read_ok = False
    # Proviamo a leggere come Excel (.xlsx) con openpyxl, poi senza engine, poi con xlrd (.xls)
    try:
        preview_df = pd.read_excel(BytesIO(raw_bytes), header=0, engine="openpyxl", dtype=str)
        st.success("File letto come Excel (.xlsx) con openpyxl")
        read_ok = True
    except Exception:
        # proviamo senza specificare engine (pandas sceglie)
        try:
            preview_df = pd.read_excel(BytesIO(raw_bytes), header=0, dtype=str)
            st.success("File letto come Excel (engine auto)")
            read_ok = True
        except Exception:
            # proviamo engine xlrd (per .xls). Nota: xlrd recenti non supportano xlsx; potrebbe essere necessario xlrd==1.2.0
            try:
                preview_df = pd.read_excel(BytesIO(raw_bytes), header=0, engine="xlrd", dtype=str)
                st.success("File letto come Excel (.xls) con xlrd")
                read_ok = True
            except Exception:
                # Non è stato possibile leggere come Excel: proviamo come testo/csv
                pass

    if not read_ok:
        # Decodifica testo provando utf-8 prima, poi latin-1 (cp1252 già gestito come latin-1)
        try:
            text = raw_bytes.decode("utf-8")
        except UnicodeDecodeError:
            text = raw_bytes.decode("latin-1", errors="replace")

        # Proviamo separatori comuni
        tried = []
        for sep, label in [("\t", ".txt (tab)"), (";", "CSV ';'"), (",", "CSV ','")]:
            try:
                preview_df = pd.read_csv(StringIO(text), sep=sep, header=0, dtype=str)
                st.success(f"File letto come testo: {label}")
                read_ok = True
                break
            except Exception as e:
                tried.append((sep, str(e)))
                continue

        if not read_ok:
            st.error("Impossibile interpretare il file come Excel o testo tabulato/CSV.")
            st.stop()

    # Se siamo qui preview_df è valorizzato
    df = preview_df.fillna("")

st.write("✅ Anteprima file caricato:")
st.dataframe(df.head(20))

st.write("Colonne trovate:")
for i, c in enumerate(df.columns):
    st.write(f"{i}: {c}")

# ================================================================
# ELABORAZIONE NORMALE (usiamo BytesIO(raw_bytes) per non dipendere dallo stream consumato)
# ================================================================
with st.spinner("Elaborazione file..."):
    try:
        # Creiamo un nuovo BytesIO per la funzione di process_workbook (pointer a 0)
        file_for_processing = BytesIO(raw_bytes)
        file_for_processing.seek(0)
        grouped_df, df_valid, inferred_month_str = process_workbook(
            file_for_processing,
            codes_map,
            infer_month=infer_month,
            month_for_days=month_num,
            year_for_days=(int(year_input) if year_input.strip().isdigit() else None)
        )
    except Exception as e:
        st.error(f"Errore durante l'elaborazione: {e}")
        st.stop()

# ================================================================
# ANTEPRIMA RISULTATI
# ================================================================
st.subheader("Anteprima riepilogo (aggregato)")
st.dataframe(grouped_df, use_container_width=True)

if show_raw:
    st.subheader("Dati validi (righe filtrate, anteprima)")
    st.dataframe(df_valid.head(200), use_container_width=True)
