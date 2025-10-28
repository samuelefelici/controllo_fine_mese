import streamlit as st
import pandas as pd
from io import BytesIO
from pathlib import Path
from processor import process_workbook, to_pdf_bytes, load_codes_map

# ---------------------------
# CONFIGURAZIONE BASE
# ---------------------------
st.set_page_config(page_title="Controlli Fine Mese", layout="wide")
st.title("Controlli Fine Mese - Estrazione diciture di assenza (PDF)")
st.set_page_config(page_title="Test Lettura File Conerobus", layout="wide")
st.title("Test Lettura File Excel Conerobus")

repo_root = Path(__file__).parent
codes_file = repo_root / "codes.csv"
uploaded_file = st.file_uploader("Carica file (.xls o .xlsx)", type=["xls", "xlsx"])

st.markdown(
    "Carica il file (.xls/.xlsx/.csv/.txt). "
    "L'app estrae le diciture di assenza e genera un PDF ordinato per categoria → matricola → data."
)

# ---------------------------
# SIDEBAR
# ---------------------------
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

# ---------------------------
# CARICAMENTO CODES MAP
# ---------------------------
if codes_file.exists():
    try:
        codes_map, categories_order = load_codes_map(codes_file)
    except Exception as e:
        st.error(f"Errore caricamento {codes_file.name}: {e}")
        codes_map, categories_order = {}, []
else:
    st.sidebar.warning(f"{codes_file.name} non trovato nella repo. Caricalo qui o aggiungilo alla repo.")
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

# ---------------------------
# MESE NUMERICO
# ---------------------------
month_num = None
if month_input:
    months_it = {
        "Gennaio": 1, "Febbraio": 2, "Marzo": 3, "Aprile": 4, "Maggio": 5, "Giugno": 6,
        "Luglio": 7, "Agosto": 8, "Settembre": 9, "Ottobre": 10, "Novembre": 11, "Dicembre": 12
    }
    month_num = months_it.get(month_input)

# ---------------------------
# FILE INPUT
# ---------------------------
uploaded_file = st.file_uploader(
    "Carica il file (.xls/.xlsx/.csv/.txt)",
    type=["xls", "xlsx", "csv", "txt"],
    accept_multiple_files=False
)
if uploaded_file is None:
    st.info("Carica un file per iniziare.")
st.stop()

# ---------------------------
# ELABORAZIONE FILE
# ---------------------------
with st.spinner("Elaborazione file..."):
# --- PROVA 1: tenta lettura con openpyxl ---
try:
    df = pd.read_excel(uploaded_file, header=0, engine="openpyxl")
    st.success("File letto con 'openpyxl'")
except Exception as e1:
    st.warning(f"openpyxl fallito: {e1}")
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
        df = pd.read_excel(uploaded_file, header=0, engine="xlrd")
        st.success("File letto con 'xlrd'")
    except Exception as e2:
        st.error(f"xlrd fallito: {e2}")
st.stop()

# ---------------------------
# RIEPILOGO
# ---------------------------
st.subheader("Anteprima riepilogo (aggregato)")
st.dataframe(grouped_df, use_container_width=True)

if show_raw:
    st.subheader("Dati validi (righe filtrate, anteprima)")
    st.dataframe(df_valid.head(200), use_container_width=True)

month_string = inferred_month_str or (
    f"{month_input} {year_input}" if month_input and year_input else "Mese non specificato"
)
st.markdown(f"**Mese di riferimento**: {month_string}")
st.write("✅ File letto con successo! Ecco le prime righe:")
st.dataframe(df.head(30))

# ---------------------------
# FILTRO CATEGORIA
# ---------------------------
available_categories = categories_order if categories_order else sorted(
    [c for c in df_valid["Category"].dropna().unique().tolist()]
)
selected_categories = st.sidebar.multiselect(
    "Filtra categorie (es. spunta Malattia)",
    options=available_categories,
    default=[],
)

# ---------------------------
# DETTAGLIO FILTRATO
# ---------------------------
df_view = df_valid.copy()
if selected_categories:
    df_view = df_view[df_view["Category"].isin(selected_categories)]

def _try_int(x):
    try:
        return int(x)
    except Exception:
        return x

df_view["Mat_sort_key"] = df_view["Mat"].apply(_try_int)
df_view = df_view.sort_values(by=["Category", "Mat_sort_key", "Data_parsed", "Data_repr"])

# Tabella richiesta: Matricola, Cognome, Nome, Giorno, Codice assenza
dettaglio_df = df_view[["Mat", "Cognome", "Nome", "Data_repr", "MatchedCode"]].rename(
    columns={
        "Mat": "Matricola",
        "Cognome": "Cognome",
        "Nome": "Nome",
        "Data_repr": "Giorno",
        "MatchedCode": "Codice assenza",
    }
)

st.subheader("Dettaglio per categoria selezionata")
if selected_categories:
    st.caption("Ordinato per categoria → matricola → giorno. Poi passa al conducente successivo.")
else:
    st.caption("Nessun filtro selezionato: mostra tutte le categorie.")

st.dataframe(dettaglio_df, use_container_width=True)

# ---------------------------
# RIEPILOGO PDF FILTRATO
# ---------------------------
grouped_filtered = (
    df_view.groupby(["Category", "Mat", "Cognome", "Nome", "Qualifica"], sort=False)
    .agg(
        Dates=("Data_repr", lambda x: ", ".join(pd.Series([v for v in x if str(v).strip() != ""]).drop_duplicates().astype(str).tolist())),
        DaysCount=("Data_repr", lambda x: len([d for d in pd.Series(x).dropna().unique() if str(d).strip() != ""])),
        RawTurns=("Turno_raw", lambda s: ", ".join(pd.Series(s.dropna().unique()).astype(str).tolist())),
    )
    .reset_index()
)

# Ordina come prima
grouped_filtered["Mat_sort_key"] = grouped_filtered["Mat"].apply(_try_int)
grouped_filtered = grouped_filtered.sort_values(by=["Category", "Mat_sort_key"])
grouped_filtered = grouped_filtered.drop(columns=["Mat_sort_key"], errors="ignore")

# ---------------------------
# GENERAZIONE PDF
# ---------------------------
try:
    pdf_bytes = to_pdf_bytes(grouped_filtered, df_view, month_string, categories_order=categories_order)
    st.download_button(
        "📄 Scarica PDF resoconto (vista filtrata)",
        data=pdf_bytes,
        file_name=f"resoconto_assenze_{month_string.replace(' ','_')}.pdf",
        mime="application/pdf",
    )
except ModuleNotFoundError as e:
    st.error(str(e))
    st.info("Per abilitare la generazione PDF aggiungi 'reportlab' a requirements.txt e riavvia.")
except Exception as e:
    st.error(f"Errore durante la generazione del PDF: {e}")
st.write("Colonne trovate:")
for i, c in enumerate(df.columns):
    st.write(f"{i}: {c}")
