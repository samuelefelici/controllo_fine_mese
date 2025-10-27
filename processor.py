import pandas as pd
from io import BytesIO
from datetime import datetime
from pathlib import Path

from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm

def load_codes_map(codes_csv_path):
    df = pd.read_csv(Path(codes_csv_path), dtype=str).fillna("")
    code_to_cat = {}
    categories_order = []
    for _, row in df.iterrows():
        cat = row.get("Category", "").strip()
        codes = row.get("Codes", "")
        if cat:
            categories_order.append(cat)
        for code in [c.strip() for c in codes.split(",") if c.strip()]:
            code_to_cat[code] = cat
    return code_to_cat, categories_order

def _read_xls_bytes(uploaded_file):
    # il file ha header -> leggilo con xlrd (per .xls)
    return pd.read_excel(uploaded_file, header=0, engine="xlrd")

def normalize_df_with_headers(df):
    # Normalizza i nomi colonne riducendoli a un set atteso (mappa)
    df = df.copy()
    # mappa colonne prese dall'esempio: some possono avere spazi o case diversi -> standardizziamo
    col_map = {}
    for c in df.columns:
        c_norm = str(c).strip().lower()
        if "matric" in c_norm:
            col_map[c] = "Mat"
        elif "cognome" in c_norm:
            col_map[c] = "Cognome"
        elif "nome" in c_norm:
            col_map[c] = "Nome"
        elif c_norm == "data" or "data" in c_norm:
            col_map[c] = "Data_raw"
        elif "turno" in c_norm and c_norm.endswith("e"):  # TurnoE
            col_map[c] = "TurnoE"
        elif "turnoc" in c_norm:
            col_map[c] = "TurnoC"
        elif "valore" in c_norm and c_norm.endswith("c"):
            col_map[c] = "ValoreC"
        elif "valore" in c_norm and c_norm.endswith("e"):
            col_map[c] = "ValoreE"
        else:
            # lasciare altre colonne come sono (es. Residenza, Gruppo, InizioE, FineE, Indennità)
            col_map[c] = c
    df = df.rename(columns=col_map)

    # assicuriamoci che ci siano le colonne principali
    for required in ["Mat","Cognome","Nome","Data_raw","TurnoE"]:
        if required not in df.columns:
            # Non trovata: la colonna potrebbe avere nome diverso -> l'utente dovrà verificare
            pass

    # Mat come stringa
    if "Mat" in df.columns:
        df['Mat'] = df['Mat'].astype(str).str.strip()

    # TurnoE -> prendi primo token se ci sono più valori (es. "M158 13:05")
    if "TurnoE" in df.columns:
        df['TurnoE'] = df['TurnoE'].astype(str).str.strip()
        # se il campo contiene più elementi separati da spazio, prendiamo il primo token
        df['TurnoE_simple'] = df['TurnoE'].apply(lambda v: v.split()[0] if isinstance(v, str) and v.strip() != "nan" else "")

    return df

def map_category_from_turnoe(turnoe_value, code_to_cat):
    # tenta la mappatura esatta sul token semplice
    if not turnoe_value:
        return None
    # se è multi-codice, il caller dovrebbe aver già semplificato
    if turnoe_value in code_to_cat:
        return code_to_cat[turnoe_value]
    # case-insensitive fallback
    t = str(turnoe_value).upper()
    for k,v in code_to_cat.items():
        if k.upper() == t:
            return v
    return None

def build_full_date_from_day(day_value, month, year):
    # day_value può essere int o stringa; month e year obbligatori per costruire la data
    try:
        day = int(str(day_value).strip())
    except:
        return pd.NaT
    try:
        return datetime(year=year, month=month, day=day)
    except:
        return pd.NaT

def infer_month_string_from_dates(df):
    months_it = ["Gennaio","Febbraio","Marzo","Aprile","Maggio","Giugno","Luglio","Agosto","Settembre","Ottobre","Novembre","Dicembre"]
    valid = df['Data'].dropna()
    if valid.empty:
        return None
    first = valid.iloc[0]
    m = first.month
    y = first.year
    return f"{months_it[m-1]} {y}"

def process_workbook(uploaded_file, code_to_cat, infer_month=False, month_for_days=None, year_for_days=None):
    raw = _read_xls_bytes(uploaded_file)
    df = normalize_df_with_headers(raw)

    # Se la colonna Data_raw contiene date complete, usiamo quelle; altrimenti Data_raw è giorno numerico
    data_col = None
    if 'Data_raw' in df.columns:
        # controlliamo il tipo
        sample = df['Data_raw'].dropna().head(10)
        is_full_date = False
        for v in sample:
            try:
                pd.to_datetime(v, dayfirst=True)
                is_full_date = True
                break
            except:
                is_full_date = False
        if is_full_date:
            df['Data'] = pd.to_datetime(df['Data_raw'], dayfirst=True, errors='coerce')
        else:
            # Data_raw è solo giorno numero -> serve month_for_days e year_for_days
            if month_for_days is None or year_for_days is None:
                # se infer_month è True, proviamo a inferire l'anno e il mese dal file (non sempre possibile)
                if infer_month:
                    # tentiamo di cercare una colonna 'Anno' o simile
                    candidate_cols = [c for c in df.columns if 'anno' in str(c).lower() or 'year' in str(c).lower()]
                    inferred_month = None
                    inferred_year = None
                    if candidate_cols:
                        try:
                            inferred_year = int(df[candidate_cols[0]].dropna().iloc[0])
                        except:
                            inferred_year = None
                    # non abbiamo mese/anno sufficienti -> lasceremo date NaT
                    df['Data'] = pd.NaT
                else:
                    df['Data'] = pd.NaT
            else:
                # costruisci la data combinando giorno + month_for_days + year_for_days
                df['Data'] = df['Data_raw'].apply(lambda x: build_full_date_from_day(x, month_for_days, year_for_days))

    else:
        df['Data'] = pd.NaT

    # scegli il campo TurnoE_simple per la mappatura se presente, altrimenti TurnoE
    turno_field = 'TurnoE_simple' if 'TurnoE_simple' in df.columns else ('TurnoE' if 'TurnoE' in df.columns else None)
    if turno_field is None:
        df['Category'] = None
    else:
        df['Category'] = df[turno_field].apply(lambda x: map_category_from_turnoe(x, code_to_cat))

    # manteniamo solo righe con categoria riconosciuta
    df_valid = df[df['Category'].notnull()].copy()

    # ordina per Category, Mat numeric (se possibile), Data
    def try_int(x):
        try:
            return int(x)
        except:
            return x
    df_valid['Mat_sort_key'] = df_valid['Mat'].apply(try_int)
    df_valid = df_valid.sort_values(by=['Category','Mat_sort_key','Data'])

    # aggrega per Category e matricola
    def dates_agg(x):
        dates = x.dropna().dt.strftime('%d/%m/%Y').tolist()
        seen = []
        for d in dates:
            if d not in seen:
                seen.append(d)
        return ", ".join(seen)

    grouped = df_valid.groupby(['Category','Mat','Cognome','Nome'], sort=False).agg(
        Dates = ('Data', dates_agg),
        DaysCount = ('Data', lambda x: x.nunique())
    ).reset_index()

    grouped['Mat_sort_key'] = grouped['Mat'].apply(try_int)
    grouped = grouped.sort_values(by=['Category','Mat_sort_key'])

    # infer month string
    month_string = None
    if infer_month:
        month_string = infer_month_string_from_dates(df_valid)

    # pulizia colonne ausiliarie
    for c in ['Mat_sort_key']:
        if c in grouped.columns:
            grouped = grouped.drop(columns=[c])
        if c in df_valid.columns:
            df_valid = df_valid.drop(columns=[c])

    return grouped, df, month_string

# to_pdf_bytes rimane quasi identico alla versione precedente
def to_pdf_bytes(grouped_df, month_string=""):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=18*mm, leftMargin=18*mm, topMargin=18*mm, bottomMargin=18*mm)
    styles = getSampleStyleSheet()
    style_h = styles['Heading1']
    style_h.alignment = 0
    style_cat = ParagraphStyle('catstyle', parent=styles['Heading2'], spaceAfter=6)
    style_normal = styles['Normal']

    story = []

    title = "Resoconto assenze"
    if month_string:
        title = f"{title} - {month_string}"
    story.append(Paragraph(title, style_h))
    story.append(Spacer(1,6))

    categories = grouped_df['Category'].unique().tolist()
    for idx, cat in enumerate(categories):
        story.append(Paragraph(f"{cat}", style_cat))
        df_cat = grouped_df[grouped_df['Category'] == cat].copy()
        if df_cat.empty:
            story.append(Paragraph("Nessun elemento per questa categoria.", style_normal))
            continue

        data = [["Mat", "Cognome Nome", "Giorni", "Date"]]
        for _, row in df_cat.iterrows():
            cognome_nome = f"{row['Cognome']} {row['Nome']}".strip()
            giorni = str(row.get('DaysCount', ""))
            dates = row.get('Dates', "")
            data.append([row['Mat'], cognome_nome, giorni, dates])

        table = Table(data, colWidths=[30*mm, 70*mm, 20*mm, 60*mm])
        tbl_style = TableStyle([
            ('BACKGROUND',(0,0),(-1,0), colors.HexColor("#d3d3d3")),
            ('TEXTCOLOR',(0,0),(-1,0), colors.black),
            ('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),
            ('FONTSIZE',(0,0),(-1,0),9),
            ('ALIGN',(0,0),(0,-1),'CENTER'),
            ('VALIGN',(0,0),(-1,-1),'TOP'),
            ('GRID',(0,0),(-1,-1),0.25, colors.grey),
            ('FONTNAME',(0,1),(-1,-1),'Helvetica'),
            ('FONTSIZE',(0,1),(-1,-1),8),
        ])
        table.setStyle(tbl_style)
        story.append(table)
        if idx != len(categories)-1:
            story.append(PageBreak())

    doc.build(story)
    pdf = buffer.getvalue()
    buffer.close()
    return pdf
