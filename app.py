import streamlit as st
import pandas as pd
from io import BytesIO, StringIO
import csv
import chardet

st.set_page_config(page_title="Controllo Paghe", layout="wide")
st.title("Controllo Paghe")

st.markdown(
    "Carica il file. Se l'anteprima appare in cinese significa che l'encoding usato è sbagliato: "
    "qui sotto vengono provate più decodifiche e puoi scegliere quella corretta."
)

uploaded_file = st.file_uploader("Carica file (xls, xlsx, csv, txt - anche se ha estensione .xls)", type=["xls", "xlsx", "csv", "txt"])

# parole chiave italiane attese per aiutare il riconoscimento
KEYWORDS = ["Residenza", "Matricola", "Cognome", "Nome", "Gruppo", "Data", "Turno", "Inizio", "Fine"]

ENC_CANDIDATES = [
    "utf-8",
    "utf-8-sig",
    "utf-16",
    "utf-16-le",
    "utf-16-be",
    "cp1252",
    "iso-8859-1",
    "latin1",
    "cp1250",
]

def try_read_excel(raw_bytes):
    try:
        # prova con pandas.read_excel (per .xls/.xlsx veri)
        sheets = pd.read_excel(BytesIO(raw_bytes), sheet_name=None, engine=None)
        if isinstance(sheets, dict):
            first = list(sheets.keys())[0]
            return sheets[first], f"excel (foglio '{first}')"
        return sheets, "excel"
    except Exception:
        return None, None

def score_decoded_text(text: str) -> int:
    t = text.lower()
    score = 0
    # conta occorrenze delle parole chiave
    for kw in KEYWORDS:
        score += t.count(kw.lower())
    # penalizza caratteri di replacement o troppi caratteri non-ASCII di controllo
    score -= text.count("�") * 5
    return score

def detect_with_chardet(raw_bytes: bytes):
    try:
        res = chardet.detect(raw_bytes)
        return res.get("encoding"), res.get("confidence", 0.0)
    except Exception:
        return None, 0.0

def generate_encoding_candidates(raw_bytes: bytes, n_top=6):
    # prima: lascia che chardet suggerisca
    detected_enc, conf = detect_with_chardet(raw_bytes)
    candidates = list(ENC_CANDIDATES)
    if detected_enc:
        detected_enc = detected_enc.lower()
        if detected_enc not in candidates:
            candidates.insert(0, detected_enc)
        else:
            candidates.remove(detected_enc)
            candidates.insert(0, detected_enc)

    scored = []
    sample_bytes = raw_bytes[:8000]  # campione
    for enc in candidates:
        try:
            decoded = sample_bytes.decode(enc)
        except Exception:
            # prova decode permissivo (sostituisce gli errori)
            try:
                decoded = sample_bytes.decode(enc, errors="replace")
            except Exception:
                continue
        sc = score_decoded_text(decoded)
        # calcola anche percentuale di caratteri non-stampa visibili (approssimazione)
        non_printable = sum(1 for ch in decoded if ord(ch) < 9 or (11 <= ord(ch) <= 31))
        scored.append({"encoding": enc, "score": sc, "snippet": decoded[:1000], "non_print": non_printable})

    # ordina per score desc, non_print asc
    scored_sorted = sorted(scored, key=lambda x: (-x["score"], x["non_print"]))
    return scored_sorted[:n_top]

def guess_separator_from_text(text: str):
    # prova csv.Sniffer su prime righe
    lines = "\n".join(text.splitlines()[:20])
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(lines)
        sep = dialect.delimiter
        if sep.isspace():
            sep = "\t"
    except Exception:
        if "\t" in lines:
            sep = "\t"
        elif ";" in lines:
            sep = ";"
        elif "," in lines:
            sep = ","
        else:
            sep = r"\s+"
    return sep

def read_text_to_df(decoded_text: str, sep_choice: str):
    # prova a leggere con pandas.read_csv usando engine python per regex sep
    si = StringIO(decoded_text)
    try:
        if sep_choice == r"\s+":
            df = pd.read_csv(si, sep=r"\s+", engine="python")
        else:
            df = pd.read_csv(si, sep=sep_choice, engine="python")
        return df
    except Exception as e:
        # fallback: provare con separatori comuni
        for s in ["\t", ";", ",", r"\s+"]:
            si.seek(0)
            try:
                if s == r"\s+":
                    df = pd.read_csv(si, sep=r"\s+", engine="python")
                else:
                    df = pd.read_csv(si, sep=s, engine="python")
                return df
            except Exception:
                continue
        raise e

if uploaded_file is not None:
    raw = uploaded_file.read()
    # 1) prova come vero excel
    df_excel, info = try_read_excel(raw)
    if df_excel is not None:
        st.success(f"File letto come {info}")
        st.subheader("Anteprima (prime 19 righe)")
        st.dataframe(df_excel.head(19))
    else:
        # 2) file testuale: genera candidati encodings
        st.info("File riconosciuto come testo. Provo diverse decodifiche...")
        candidates = generate_encoding_candidates(raw, n_top=8)
        detected_enc, detected_conf = detect_with_chardet(raw)

        st.subheader("Decodifiche candidate (scegli quella che mostra intestazioni leggibili)")
        # mostra elenco con snippet e punteggio
        enc_options = []
        for i, c in enumerate(candidates):
            label = f"{i+1}) {c['encoding']} — score={c['score']} — non_print={c['non_print']}"
            enc_options.append((label, c))

        labels = [lab for lab, _ in enc_options]

        chosen = None
        guessed_sep = r"\s+"
        if labels:
            sel_idx = st.radio(
                "Decodifiche trovate (anteprima snippet):",
                options=list(range(len(labels))),
                format_func=lambda i: labels[i]
            )
            chosen = enc_options[sel_idx][1]
            st.markdown(f"**Encoding suggerito:** {chosen['encoding']}  — punteggio: {chosen['score']}")
            st.code(chosen['snippet'][:1000], language="text")
            guessed_sep = guess_separator_from_text(chosen["snippet"])
            st.write(f"Separatore suggerito: '{guessed_sep}'")
        else:
            st.write("Nessuna decodifica candidata trovata.")
            # default separator guess from raw sniff using latin1 fallback
            try:
                sample_text = raw.decode("latin1")
                guessed_sep = guess_separator_from_text(sample_text)
            except Exception:
                guessed_sep = r"\s+"

        # opzioni manuali
        st.markdown("---")
        st.subheader("Opzioni manuali / prova diretta")
        manual_enc = st.text_input("Forza encoding (lascia vuoto per usare la selezione sopra)", value="")
        # se l'utente non forza, usa chosen se presente, altrimenti usa chardet detected, altrimenti utf-8
        if manual_enc.strip():
            enc_to_use = manual_enc.strip()
        elif chosen:
            enc_to_use = chosen["encoding"]
        elif detected_enc:
            enc_to_use = detected_enc
        else:
            enc_to_use = "utf-8"
        st.write(f"Encoding selezionato per il parsing: {enc_to_use}")

        manual_sep = st.text_input("Forza separatore (es. '\\t' o ';' o ',' o '\\\\s+' per whitespace)", value=(guessed_sep if guessed_sep else "\\s+"))
        st.write(f"Separatore usato per il parsing: {manual_sep}")

        # decode full text con l'encoding scelto (con fallback permissivo)
        try:
            decoded_full = raw.decode(enc_to_use)
        except Exception:
            decoded_full = raw.decode(enc_to_use, errors="replace")

        # prova convertire in DataFrame
        try:
            df = read_text_to_df(decoded_full, manual_sep)
            st.success("Lettura testo -> DataFrame avvenuta con successo")
            st.subheader("Anteprima (prime 19 righe)")
            st.dataframe(df.head(19))
            # pulizia di base
            def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
                df_clean = df.copy()
                df_clean.columns = [c.strip() if isinstance(c, str) else c for c in df_clean.columns]
                def _strip_val(x):
                    return x.strip() if isinstance(x, str) else x
                df_clean = df_clean.applymap(_strip_val)
                return df_clean
            df_clean = clean_dataframe(df)
            st.write(f"Dimensione dati: {df_clean.shape[0]} righe × {df_clean.shape[1]} colonne")
            st.download_button(
                label="Scarica CSV pulito",
                data=df_clean.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"{uploaded_file.name.rsplit('.',1)[0]}_pulito.csv",
                mime="text/csv",
            )
        except Exception as e:
            st.error(f"Impossibile convertire il testo in tabella: {e}")
            st.markdown("Mostro un'anteprima testuale dello stream decodificato per debug:")
            st.code(decoded_full[:2000], language="text")
else:
    st.info("Carica un file per iniziare.")
