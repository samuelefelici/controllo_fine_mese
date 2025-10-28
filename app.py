import streamlit as st
import pandas as pd
from io import BytesIO
from pathlib import Path

st.set_page_config(page_title="Test Lettura File Conerobus", layout="wide")
st.title("Test Lettura File Excel Conerobus")

uploaded_file = st.file_uploader("Carica file (.xls o .xlsx)", type=["xls", "xlsx"])

if uploaded_file is None:
    st.info("Carica un file per iniziare.")
    st.stop()

# --- PROVA 1: tenta lettura con openpyxl ---
try:
    df = pd.read_excel(uploaded_file, header=0, engine="openpyxl")
    st.success("File letto con 'openpyxl'")
except Exception as e1:
    st.warning(f"openpyxl fallito: {e1}")
    try:
        df = pd.read_excel(uploaded_file, header=0, engine="xlrd")
        st.success("File letto con 'xlrd'")
    except Exception as e2:
        st.error(f"xlrd fallito: {e2}")
        st.stop()

st.write("✅ File letto con successo! Ecco le prime righe:")
st.dataframe(df.head(30))

st.write("Colonne trovate:")
for i, c in enumerate(df.columns):
    st.write(f"{i}: {c}")
