# app.py
import streamlit as st
from pathlib import Path
from processor import process_workbook, to_pdf_bytes, load_codes_map

st.set_page_config(page_title="Controlli Fine Mese", layout="wide")
st.title("Controlli Fine Mese - Estrazione diciture di assenza (PDF)")

repo_root = Path(__file__).parent
codes_file = repo_root / "codes.csv"

st.markdown(
    "Questa app estrae le diciture di assenza dai file Excel (.xls/.xlsx) o testo (.csv/.txt). "
    "Non vengono fatte verifiche sui dati: il programma riporta ciò che trova."
)

# Sidebar opzioni
st.sidebar.header("Opzioni")
st.sidebar.markdown(f"File di mappatura codici: `{codes_file.name}` (modificabile nella repo)")
show_raw = st.sidebar.checkbox("Mostra dati grezzi", value=False)
infer_month = st.sidebar.checkbox("Usa mese/anno forniti se 'Data' è solo giorno", value=True)

# Mese/Anno per comporre date quando Data è solo giorno (opzionale)
st.sidebar.subheader("Mese/Anno (usati se 'Data' è solo giorno)")
month_input = st.sidebar.selectbox("Mese", [""] + ["Gennaio","Febbraio","Marzo","Aprile","Maggio","Giugno","Luglio","Agosto","Settembre","Ottobre","Novembre","Dicembre"])
year_input = st.sidebar.text_input("Anno (es. 2025)", value="")

# codes.csv: se non presente permetti caricamento
uploaded_codes_file = None
if codes_file.exists():
    try:
        codes_map, categories_order = load_codes_map(codes_file)
    except Exception as e:
        st.error(f"Errore caricamento {codes_file.name}: {e}")
        codes_map, categories_order = {}, []
else:
    st.sidebar.warning(f"{codes_file.name} non trovato nella repository. Puoi caricarlo qui sotto oppure aggiungerlo alla repo.")
    uploaded_codes_file = st.sidebar.file_uploader("Carica codes.csv (opzionale)", type=["csv"])
    if uploaded_codes_file is not None:
        try:
            codes_map, categories_order = load_codes_map(uploaded_codes_file)
        except Exception as e:
            st.error(f"Errore caricamento file caricato: {e}")
            codes_map, categories_order = {}, []
    else:
        codes_map, categories_order = {}, []

st.sidebar.subheader("Categorie trovate")
st.sidebar.write(", ".join(categories_order) if categories_order else "Nessuna (mappa vuota)")

# convert month_input -> numeric month or None
month_num = None
if month_input:
    months_it = { "Gennaio":1,"Febbraio":2,"Marzo":3,"Aprile":4,"Maggio":5,"Giugno":6,"Luglio":7,"Agosto":8,"Settembre":9,"Ottobre":10,"Novembre":11,"Dicembre":12 }
    month_num = months_it.get(month_input)

uploaded_file = st.file_uploader("Carica il file (.xls, .xlsx, .csv, .txt)", type=["xls","xlsx","csv","txt"], accept_multiple_files=False)
if uploaded_file is None:
    st.stop()

with st.spinner("Elaborazione file..."):
    try:
        grouped_df, raw_df, inferred_month_str = process_workbook(
            uploaded_file,
            codes_map,
            infer_month=infer_month,
            month_for_days=month_num,
            year_for_days=(int(year_input) if year_input.strip().isdigit() else None)
        )
    except Exception as e:
        st.error(f"Errore durante l'elaborazione: {e}")
        st.stop()

st.subheader("Riepilogo estratto per categoria / matricola")
st.write("Colonne: Category, Mat, Cognome, Nome, Dates, DaysCount, RawTurns")
st.dataframe(grouped_df)

if show_raw:
    st.subheader("Dati grezzi normalizzati (anteprima)")
    st.dataframe(raw_df.head(200))

month_string = inferred_month_str or (f"{month_input} {year_input}" if month_input and year_input else "Mese non specificato")
st.markdown(f"**Mese di riferimento**: {month_string}")

# Genera PDF e download
try:
    pdf_bytes = to_pdf_bytes(grouped_df, month_string)
    st.download_button("Scarica PDF resoconto", data=pdf_bytes, file_name=f"resoconto_assenze_{month_string.replace(' ','_')}.pdf", mime="application/pdf")
except ModuleNotFoundError as e:
    st.error(str(e))
    st.info("Per abilitare la generazione PDF aggiungi 'reportlab' a requirements.txt e riavvia.")
