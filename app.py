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
# LETTURA FILE GREZZO (anche se è falso .xls)
# ================================================================
with st.spinner("Tentativo di lettura del file..."):
    try:
        df = pd.read_excel(uploaded_file, header=0, engine="openpyxl")
        st.success("File letto come Excel (.xlsx)")
    except Exception:
        uploaded_file.seek(0)
        raw = uploaded_file.read()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("latin-1", errors="replace")

        try:
            df = pd.read_csv(StringIO(text), sep="\t", header=0)
            st.success("File letto come testo tabulato (.txt travestito da .xls)")
        except Exception:
            df = pd.read_csv(StringIO(text), sep=";", header=0)
            st.success("File letto come CSV separato da ';'")

st.write("✅ Anteprima file caricato:")
st.dataframe(df.head(20))

st.write("Colonne trovate:")
for i, c in enumerate(df.columns):
    st.write(f"{i}: {c}")

# ================================================================
# ELABORAZIONE NORMALE
# ================================================================
with st.spinner("Elaborazione file..."):
    try:
        grouped_df, df_valid, inferred_month_str = process_workbook(
            uploaded_file,
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
