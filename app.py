import streamlit as st
from pathlib import Path
from processor import process_workbook, to_pdf_bytes, load_codes_map
import pandas as pd

st.set_page_config(page_title="Controlli Fine Mese", layout="wide")
st.title("Controlli Fine Mese - Resoconto assenze (PDF)")

repo_root = Path(__file__).parent
codes_file = repo_root / "codes.csv"

st.markdown("Carica il file .xls (con intestazione). L'app userà la colonna `TurnoE` per mappare i codici e richiederà mese/anno se la colonna `Data` contiene solo il giorno del mese.")

# Sidebar opzioni
st.sidebar.header("Opzioni")
st.sidebar.markdown(f"File di mappatura codici: `{codes_file.name}` (modificabile nella repo)")
show_raw = st.sidebar.checkbox("Mostra dati grezzi", value=False)
infer_month = st.sidebar.checkbox("Inferisci mese automaticamente (se possibile)", value=False)

# Quando la Data è solo giorno, l'utente può fornire mese/anno
st.sidebar.subheader("Mese/Anno (usati se 'Data' è solo giorno)")
month_input = st.sidebar.selectbox("Mese", [""] + ["Gennaio","Febbraio","Marzo","Aprile","Maggio","Giugno","Luglio","Agosto","Settembre","Ottobre","Novembre","Dicembre"])
year_input = st.sidebar.text_input("Anno (es. 2025)", value="")

uploaded_file = st.file_uploader("Carica il file .xls", type=["xls"], accept_multiple_files=False)
if uploaded_file is None:
    st.stop()

# load codes map
codes_map, categories_order = load_codes_map(codes_file)
st.sidebar.subheader("Categorie trovate")
st.sidebar.write(", ".join(categories_order))

# convert month_input -> numeric month or None
month_num = None
if month_input:
    months_it = { "Gennaio":1,"Febbraio":2,"Marzo":3,"Aprile":4,"Maggio":5,"Giugno":6,"Luglio":7,"Agosto":8,"Settembre":9,"Ottobre":10,"Novembre":11,"Dicembre":12 }
    month_num = months_it.get(month_input)

# processa
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
        raise

st.subheader("Riepilogo per categoria / matricola")
st.write("Colonne: Category, Mat, Cognome, Nome, Qualifica, Dates, DaysCount")
st.dataframe(grouped_df)

if show_raw:
    st.subheader("Dati grezzi normalizzati (anteprima)")
    st.dataframe(raw_df.head(200))

month_string = inferred_month_str or (f"{month_input} {year_input}" if month_input and year_input else "Mese non specificato")
st.markdown(f"**Mese di riferimento**: {month_string}")

# Genera PDF e download
pdf_bytes = to_pdf_bytes(grouped_df, month_string)

st.download_button("Scarica PDF resoconto", data=pdf_bytes, file_name=f"resoconto_assenze_{month_string.replace(' ','_')}.pdf", mime="application/pdf")

st.success("PDF pronto — clicca 'Scarica PDF resoconto' per ottenere il file.")
