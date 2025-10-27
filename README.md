```markdown
# controlli_fine_mese

App Streamlit per generare resoconti PDF delle assenze/turni a partire da file .xls (formato BDROP/interfaccia paghe).

Caratteristiche
- Legge file .xls (senza intestazione) con colonne: Mat, Cognome, Nome, Qualifica, Data, Giorno, Turno, Minuti.
- Usa `codes.csv` nella repository per mappare i codici di "Turno" alle categorie di assenza. Modifica `codes.csv` per aggiornare le categorie.
- Genera un PDF ordinato per categoria -> matricola -> data, dove per ogni matricola sono elencati i giorni associati a quella tipologia.
- Interfaccia semplice con Streamlit e pulsante per scaricare il PDF.

Requisiti
- Python 3.8+
- Pacchetti: vedi `requirements.txt`. Nota: per leggere .xls è richiesto `xlrd==1.2.0`.

Installazione
1. Clona la repo:
   git clone <repo-url>
2. Crea e attiva un virtualenv:
   python -m venv .venv
   source .venv/bin/activate   (Linux/Mac) o .venv\Scripts\activate (Windows)
3. Installa dipendenze:
   pip install -r requirements.txt

Esecuzione
- Avvia l'app Streamlit:
  streamlit run app.py

Uso
- Carica il file .xls tramite l'interfaccia.
- L'app inferisce (opzionalmente) il mese di riferimento dalle date del file.
- Clicca "Scarica PDF resoconto" per ottenere il file ordinato per categoria e matricola.

Personalizzazione
- Modifica `codes.csv` per aggiungere/modificare codici di mapping.
- Se desideri modificare il layout del PDF (font, logo, intestazioni) modifica `processor.to_pdf_bytes`.

Note tecniche
- La conversione .xls usa `xlrd` (versione 1.2.0).
- Per un output Excel invece che PDF si può aggiungere una funzione che esporta in .xlsx.
```
