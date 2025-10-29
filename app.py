# app.py
import streamlit as st
import pandas as pd
from io import BytesIO

st.set_page_config(page_title="Controllo Paghe", layout="wide")
st.title("Controllo Paghe")

st.markdown("Carica un file Excel (.xls o .xlsx). Verrà mostrata un'anteprima delle prime 19 righe e potrai scaricare una versione pulita in CSV.")

uploaded_file = st.file_uploader("Carica file Excel", type=["xls", "xlsx"])

def read_excel_file(uploaded):
    # Scegli engine in base all'estensione (xlrd per .xls, openpyxl per .xlsx)
    name = uploaded.name.lower()
    if name.endswith(".xls"):
        engine = "xlrd"
    else:
        engine = "openpyxl"
    try:
        # sheet_name=None -> ritorna un dict {sheet_name: DataFrame}
        with st.spinner("Leggo il file..."):
            sheets = pd.read_excel(uploaded, sheet_name=None, engine=engine)
        return sheets
    except Exception as e:
        st.error(f"Errore nella lettura del file: {e}")
        return None

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Rimuove spazi nelle intestazioni e nelle celle stringa
    df_clean = df.copy()
    # strip colonne
    df_clean.columns = [c.strip() if isinstance(c, str) else c for c in df_clean.columns]
    # strip valori stringa
    def _strip_val(x):
        return x.strip() if isinstance(x, str) else x
    df_clean = df_clean.applymap(_strip_val)
    return df_clean

if uploaded_file is not None:
    sheets = read_excel_file(uploaded_file)
    if sheets is None:
        st.stop()

    sheet_names = list(sheets.keys())
    if len(sheet_names) > 1:
        sheet_choice = st.selectbox("Seleziona foglio", sheet_names)
    else:
        sheet_choice = sheet_names[0]

    df = sheets[sheet_choice]

    st.subheader(f"Anteprima del foglio: {sheet_choice}")
    # Mostra prime 19 righe come richiesto
    try:
        st.dataframe(df.head(19))
    except Exception:
        # fallback semplice
        st.write(df.head(19))

    st.markdown("---")
    st.subheader("Opzioni di pulizia e download")

    do_clean = st.checkbox("Esegui pulizia base (rimuovi spazi nelle intestazioni e nelle celle stringa)", value=True)

    if do_clean:
        df_clean = clean_dataframe(df)
    else:
        df_clean = df.copy()

    st.write(f"Dimensione dati: {df_clean.shape[0]} righe × {df_clean.shape[1]} colonne")
    st.dataframe(df_clean.head(19))

    # Bottone di download CSV
    try:
        csv_bytes = df_clean.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="Scarica CSV pulito",
            data=csv_bytes,
            file_name=f"{uploaded_file.name.rsplit('.',1)[0]}_pulito.csv",
            mime="text/csv",
        )
    except Exception as e:
        st.error(f"Impossibile preparare il download: {e}")

    st.markdown("""
    NOTE:
    - Se il file ha formattazioni particolari (colonne unite, righe di intestazione multiple, celle con separatori particolari)
      potrebbe essere necessario un parsing più avanzato: posso aggiungere opzioni per specificare quante righe usare come intestazione, 
      unire colonne, o trasformare colonne orarie in durate.
    - Il codice cerca automaticamente l'engine corretto per .xls/.xlsx ma assicurati di avere le dipendenze installate.
    """)
else:
    st.info("Carica un file Excel per iniziare.")
