import streamlit as st
import pandas as pd
from io import StringIO, BytesIO
from pathlib import Path
import csv
import warnings
import tempfile
import os
from processor import process_workbook, load_codes_map

st.set_page_config(page_title="DEBUG - Controlli Fine Mese", layout="wide")
st.title("DEBUG: Controlli Fine Mese - diagnostica lettura file")

uploaded_file = st.file_uploader("Carica il file (.xls/.xlsx/.csv/.txt)", type=["xls", "xlsx", "csv", "txt"], accept_multiple_files=False)
if uploaded_file is None:
    st.info("Carica un file per iniziare.")
    st.stop()

try:
    raw_bytes = uploaded_file.read()
except Exception:
    try:
        raw_bytes = uploaded_file.getvalue()
    except Exception as e:
        st.error(f"Impossibile leggere il file caricato: {e}")
        st.stop()

st.write("Lunghezza bytes:", len(raw_bytes))

# mostra prime bytes (hex) per riconoscere formato
st.write("Prime 64 bytes (hex):", raw_bytes[:64].hex())

# semplice riconoscimento "magic"
magic = raw_bytes[:8]
if magic.startswith(b'PK'):
    st.write("Probabile ZIP container (xlsx o docx/pptx) -> formato XLSX probabile (PK..).")
elif magic.startswith(b'\xd0\xcf\x11\xe0'):
    st.write("Probabile file BIFF (vecchio .xls).")
else:
    # check se è testo
    try:
        raw_bytes.decode("utf-8")
        st.write("Sembra testo UTF-8 (potrebbe essere CSV/TXT).")
    except Exception:
        st.write("Non è chiaro il formato dai primi bytes.")

# salvo temporaneamente il file su disco per testare come file path
suffix = ".bin"
if raw_bytes[:2] == b'PK':
    suffix = ".xlsx"
elif raw_bytes[:4] == b'\xd0\xcf\x11\xe0':
    suffix = ".xls"
else:
    # proviamo csv
    suffix = ".csv"

tmp_dir = Path(tempfile.gettempdir())
tmp_path = tmp_dir / f"tmp_upload_debug{suffix}"
tmp_path.write_bytes(raw_bytes)
st.write("Saved tmp file to:", str(tmp_path))

# Provo pandas.read_excel / read_csv su tmp file
st.header("Tentativi di lettura con pandas/openpyxl/csv")
read_results = {}

# 1) proviamo pd.read_excel (openpyxl) se xlsx
if suffix == ".xlsx" or suffix == ".xls":
    try:
        df = pd.read_excel(tmp_path, header=None, engine="openpyxl", dtype=str)
        read_results["pandas_openpyxl_headerNone"] = {"shape": df.shape, "head": df.head(5).astype(str).values.tolist()}
    except Exception as e:
        read_results["pandas_openpyxl_headerNone_error"] = repr(e)

    # anche con engine automatico senza dtype forzato
    try:
        df2 = pd.read_excel(tmp_path, header=None)
        read_results["pandas_readexcel_headerNone_auto"] = {"shape": df2.shape, "head": df2.head(5).astype(str).values.tolist()}
    except Exception as e:
        read_results["pandas_readexcel_error"] = repr(e)

# 2) proviamo a leggere come CSV/TXT
try:
    text = None
    try:
        text = raw_bytes.decode("utf-8")
        enc = "utf-8"
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1", errors="replace")
        enc = "latin-1"
    st.write("Decodifica effettuata con:", enc)
    # sniff delimiter
    try:
        sample = "\n".join(text.splitlines()[:20])
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample)
        st.write("Sniffer CSV delimiter:", dialect.delimiter)
        df_csv = pd.read_csv(StringIO(text), sep=dialect.delimiter, header=None, dtype=str, engine="python", on_bad_lines="warn")
        read_results["pd_read_csv_sniff"] = {"shape": df_csv.shape, "head": df_csv.head(5).astype(str).values.tolist()}
    except Exception as e:
        read_results["pd_read_csv_sniff_error"] = repr(e)
        # fallback: try common sep
        for sep in ["\t", ";", ",", "|"]:
            try:
                df_try = pd.read_csv(StringIO(text), sep=sep, header=None, dtype=str, engine="python", on_bad_lines="warn")
                read_results[f"pd_read_csv_sep_{sep}"] = {"shape": df_try.shape, "head": df_try.head(5).astype(str).values.tolist()}
            except Exception as e2:
                read_results[f"pd_read_csv_sep_{sep}_error"] = repr(e2)
except Exception as e:
    read_results["csv_decode_error"] = repr(e)

# 3) openpyxl inspection (se xlsx)
if suffix == ".xlsx":
    try:
        from openpyxl import load_workbook
        wb = load_workbook(filename=str(tmp_path), read_only=True, data_only=True)
        sheetnames = wb.sheetnames
        st.write("openpyxl wb.sheetnames:", sheetnames)
        ws = wb.active
        st.write("active sheet:", ws.title, " max_row:", ws.max_row, " max_column:", ws.max_column)
        sample_rows = []
        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            sample_rows.append([("" if v is None else str(v)) for v in row])
            if i >= 6:
                break
        read_results["openpyxl_active_head"] = sample_rows
    except Exception as e:
        read_results["openpyxl_error"] = repr(e)

st.write(read_results)

# Mostriamo anche info sul DataFrame 'df' se esiste dal parsing principale
st.header("Controlli sul DataFrame creato dal parsing principale (se presente)")
try:
    # ricreiamo il parsing come fai nel tuo app per essere coincidenti
    df_main = None
    try:
        df_main = pd.read_excel(BytesIO(raw_bytes), header=None, engine="openpyxl", dtype=str)
    except Exception:
        df_main = None
    if df_main is None:
        try:
            # prova openpyxl manual
            from openpyxl import load_workbook
            wb2 = load_workbook(filename=BytesIO(raw_bytes), read_only=True, data_only=True)
            ws2 = wb2.active
            rows = []
            maxcols = 0
            for row in ws2.iter_rows(values_only=True):
                r = ["" if v is None else str(v) for v in row]
                rows.append(r)
                if len(r) > maxcols:
                    maxcols = len(r)
            norm = [r + [""] * (maxcols - len(r)) for r in rows]
            df_main = pd.DataFrame(norm).astype(str)
        except Exception:
            df_main = None
    if df_main is not None:
        st.write("df_main.shape:", df_main.shape)
        st.write("Prime 10 righe (testo):")
        for r in df_main.head(10).astype(str).values.tolist():
            st.text(" | ".join(r))
        # statistiche semplici
        st.write("Conteggio valori vuoti per colonna (stringhe vuote):")
        empties = (df_main == "").sum(axis=0).to_dict()
        st.write(empties)
    else:
        st.write("df_main è None dopo i tentativi di parsing come nel tuo app.")
except Exception as e:
    st.write("Errore in controllo df_main:", repr(e))

# Ora proviamo a chiamare process_workbook in due modi (file-like e path) e segnaliamo forma dei risultati
st.header("Chiamata a process_workbook (debug)")

codes_file = Path(__file__).parent / "codes.csv"
codes_map = load_codes_map(codes_file)[0] if codes_file.exists() else {}

# 1) file-like (BytesIO) - quello che usi ora
try:
    bio = BytesIO(raw_bytes)
    bio.seek(0)
    res = process_workbook(bio, codes_map, infer_month=True, month_for_days=None, year_for_days=None)
    st.write("process_workbook(file-like) risultato type/len:", type(res), None if res is None else [type(x) for x in res])
    st.write("process_workbook(file-like) raw repr (prima 2000 chars):", repr(res)[:2000])
except Exception as e:
    st.write("process_workbook(file-like) errore:", repr(e))

# 2) passiamo il tmp path (alcune funzioni richiedono path)
try:
    res2 = process_workbook(str(tmp_path), codes_map, infer_month=True, month_for_days=None, year_for_days=None)
    st.write("process_workbook(path) risultato type/len:", type(res2), None if res2 is None else [type(x) for x in res2])
    st.write("process_workbook(path) raw repr (prima 2000 chars):", repr(res2)[:2000])
except Exception as e:
    st.write("process_workbook(path) errore:", repr(e))

st.success("DEBUG completato. Incolla qui i risultati principali (detected_format / warnings / shapes / error logs) per proseguire.")
