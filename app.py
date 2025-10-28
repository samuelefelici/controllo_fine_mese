import streamlit as st
import pandas as pd
from io import StringIO, BytesIO
from pathlib import Path
from processor import process_workbook, to_pdf_bytes, load_codes_map
import csv
import warnings

# ================================================================
# CONFIGURAZIONE BASE
# ================================================================
st.set_page_config(page_title="Controlli Fine Mese", layout="wide")
st.title("Controlli Fine Mese - Anteprima grezza del file (robusta)")

repo_root = Path(__file__).parent
codes_file = repo_root / "codes.csv"

st.markdown(
    "Carica il file (.xls/.xlsx/.csv/.txt). "
    "Questa vista prova a leggere il foglio 'così com'è' con più strategie tolleranti "
    "e mostra diagnostica per capire righe malformate."
)

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
# LETTURA GREZZA + STRATEGIE TOLLERANTI
# ================================================================
with st.spinner("Lettura e analisi file in corso..."):
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
    warnings_list = []

    # 1) Proviamo pd.read_excel con openpyxl (xlsx) e header=None per visualizzare grezzo
    try:
        df = pd.read_excel(BytesIO(raw_bytes), header=None, engine="openpyxl", dtype=str)
        detected_format = "excel_openpyxl_headerNone"
    except Exception as e:
        warnings_list.append(f"openpyxl read failed: {e}")
        df = None

    # 2) Se non funziona, fallback: usare openpyxl.load_workbook per leggere cella per cella (solo xlsx)
    if df is None:
        try:
            from openpyxl import load_workbook
            wb = load_workbook(filename=BytesIO(raw_bytes), read_only=True, data_only=True)
            ws = wb.active
            rows = []
            maxcols = 0
            for row in ws.iter_rows(values_only=True):
                r = ["" if v is None else str(v) for v in row]
                rows.append(r)
                if len(r) > maxcols:
                    maxcols = len(r)
            # pad rows
            norm = [r + [""] * (maxcols - len(r)) for r in rows]
            df = pd.DataFrame(norm).astype(str)
            detected_format = "excel_openpyxl_manual_rows"
        except Exception as e:
            warnings_list.append(f"openpyxl load_workbook failed: {e}")
            df = None

    # 3) Se ancora nulla, proviamo pd.read_excel con xlrd (per .xls storici)
    if df is None:
        try:
            df = pd.read_excel(BytesIO(raw_bytes), header=None, engine="xlrd", dtype=str)
            detected_format = "excel_xlrd_headerNone"
        except Exception as e:
            warnings_list.append(f"xlrd read failed: {e}")
            df = None

    # 4) Se non è Excel o i precedenti falliscono, proviamo parsing testo/CSV con engine='python' (tollerante)
    if df is None:
        try:
            try:
                text = raw_bytes.decode("utf-8")
            except UnicodeDecodeError:
                text = raw_bytes.decode("latin-1", errors="replace")

            # Tentiamo prima pd.read_csv con engine='python' e sep=None (autodetect), on_bad_lines='warn'
            try:
                df = pd.read_csv(StringIO(text), sep=None, engine="python", header=None, dtype=str, on_bad_lines="warn")
                detected_format = "csv_python_sepNone_on_bad_lines_warn"
            except Exception as e:
                warnings_list.append(f"pd.read_csv python sep=None failed: {e}")
                df = None

            # 5) se ancora nulla, proviamo separatori espliciti con engine='python' e on_bad_lines='skip'
            if df is None:
                for sep in ["\t", ";", ",", "|"]:
                    try:
                        df = pd.read_csv(StringIO(text), sep=sep, engine="python", header=None, dtype=str, on_bad_lines="warn")
                        detected_format = f"csv_python_sep_{sep}_on_bad_lines_warn"
                        break
                    except Exception as e:
                        warnings_list.append(f"pd.read_csv python sep={sep} failed: {e}")
                        df = None

            # 6) fallback manuale riga-per-riga con csv.reader provando diversi separatori
            if df is None:
                lines = text.splitlines()
                rows = []
                problem_lines = []
                maxcols = 0
                for i, line in enumerate(lines, start=1):
                    parsed = None
                    for sep in [";", "\t", ",", "|"]:
                        try:
                            parsed = next(csv.reader([line], delimiter=sep))
                            # accept parsed if at least 2 columns
                            if len(parsed) >= 1:
                                break
                        except Exception:
                            parsed = None
                    if parsed is None:
                        problem_lines.append((i, line))
                        parsed = [line]
                    rows.append(parsed)
                    if len(parsed) > maxcols:
                        maxcols = len(parsed)
                # pad rows to maxcols
                norm_rows = [r + [""] * (maxcols - len(r)) for r in rows]
                df = pd.DataFrame(norm_rows).astype(str)
                detected_format = "manual_csv_guess"
                if problem_lines:
                    warnings_list.append(f"Manual parsing detected {len(problem_lines)} problematic lines (first 10 shown).")
        except Exception as e:
            st.error(f"Impossibile interpretare il file come Excel o CSV/TXT: {e}")
            st.stop()

    # Normalizziamo i NaN a stringa vuota per una visualizzazione pulita
    df = df.fillna("")

st.success(f"Caricato con: {detected_format}")
if warnings_list:
    st.warning("Avvisi lettura:")
    for w in warnings_list[:20]:
        st.text(w)

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
st.info("Se le intestazioni sono in una riga interna, indica qui quale riga usare come header (es. 0 per prima riga, 5 per sesta), o premi il pulsante per procedere con l'elaborazione grezza.")

# CONTROLLO: input riga header da usare
header_row_input = st.number_input("Riga da usare come header (opzionale, -1 = nessuna)", min_value=-1, max_value=max(0, df.shape[0]-1), value=-1, step=1)
use_header = None if header_row_input < 0 else int(header_row_input)

if st.button("Mostra anteprima interpretata con la riga di header scelta"):
    try:
        if use_header is None:
            st.error("Nessuna riga di header scelta.")
        else:
            # ricreiamo un DataFrame interpretando la riga scelta come header
            header_vals = list(df.iloc[use_header].astype(str).tolist())
            df_interp = df.copy()
            df_interp.columns = [f"col_{i}" for i in range(df.shape[1])]
            df_interp = df_interp.drop(index=use_header).reset_index(drop=True)
            df_interp.columns = header_vals
            st.write(f"Anteprima interpretata usando la riga {use_header} come header:")
            st.dataframe(df_interp.head(40), use_container_width=True)
    except Exception as e:
        st.error(f"Errore interpretazione header: {e}")

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
