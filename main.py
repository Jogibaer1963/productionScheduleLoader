import sys
import logging
from pathlib import Path


from pymongo import MongoClient
import pandas as pd
from uuid import uuid4


client = MongoClient("mongodb://localhost:27017/")

# Connect to a specific database
db = client['production']

# Connect to a collection (similar to a SQL table)
collection = db['productionSchedule']

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    filename=LOG_DIR / "app.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    encoding="utf-8"
)

INPUT = Path('C:/files/productionSchedule.xlsx')

desired_columns = [
    'machine', 'st_1', 'st_2', 'st_3', 'st_4', 'engine_merge', 'bay_2', 'bay_3', 'bay_4', 'bay_5', 'bay_6',
    'bay_7', 'bay_8', 'bay_9', 'bay_10', 'test_1', 'test_2', 'bay_14', 'bay_15', 'bay_16', 'bay_17',
    'bay_18', 'bay_19', 'bay_19_sap', 'leaving_date', 'terraTrack', 'duals', '4wd', 'mtu', 'salesOrder',
    'productionOrder', 'sequenz', 'ecn', 'released', 'printed', 'space', 'month', 'week',
]

def main():
    try:
        # 2) Robust einlesen - erste Zeile überspringen
        df = pd.read_excel(
            INPUT,
            engine="openpyxl", #needs to install with pip install openpyxl
            header=1,  # Zweite Zeile als Header verwenden (Erste Zeile überspringen)
            dtype=str,  # Alles als String → nichts "verschwindet"
            keep_default_na=False  # "NA"/"N/A" etc. bleiben Strings, nicht NaN
        )

        # Header bereinigen und anwenden
        df.columns = desired_columns
        logging.info(f"DataFrame eingelesen mit {len(df)} Zeilen und bereinigte Spalten: {list(df.columns)}")

        # Spalte 'sequenz' normalisieren und eindeutige Werte erzwingen
        if 'sequenz' in df.columns:
            # Trim whitespace; keep empty strings for "no sequenz"
            df['sequenz'] = df['sequenz'].astype(str).str.strip()
            # Mark duplicates only where sequenz is non-empty
            dup_mask = (df['sequenz'] != '') & df.duplicated(subset=['sequenz'], keep='first')
            removed = int(dup_mask.sum())
            if removed:
                logging.info(f"{removed} Zeilen mit doppelter 'sequenz' entfernt (nur erste Vorkommen beibehalten)")
            df = df[~dup_mask].copy()

        records = df.to_dict("records")

        for r in records:
            r['_id'] = str(uuid4())  # statt ObjectId: plain String
            # Add activeList="true" for rows where 'sequenz' is present (non-empty after trimming)
            seq_val = r.get('sequenz')
            if isinstance(seq_val, str):
                r['activeList'] = 'true'
                r['activeEngine'] = 'true'
                r['activeBayFCB_1'] = 'false'
                r['activeBayFCB_2'] = 'false'
                r['activeBayRearAxle'] = 'false'
                r['activeBayThreshingFront'] = 'false'
                r['activeBayThreshing'] = 'false'
                r['activeBayFrontAxle'] = 'false'
                r['activeBay_2'] = 'false'
                r['activeBay_3'] = 'false'
                r['activeBay_4'] = 'false'
                r['activeBay_5'] = 'false'
                r['activeBay_6'] = 'false'
                r['activeBay_7'] = 'false'
                r['activeBay_8'] = 'false'
                r['activeBay_9'] = 'false'
                r['activeBay_10'] = 'false'
                r['activeTestBay_1'] = 'false'
                r['activeTestBay_2'] = 'false'
                r['activeTestBay_3'] = 'false'
                r['activeTestBay_4'] = 'false'
                r['activeBay_14'] = 'false'
                r['activeBay_15'] = 'false'
                r['activeBay_16'] = 'false'
                r['activeBay_17'] = 'false'
                r['activeBay_18'] = 'false'
                r['activeBay_19'] = 'false'
                r['activeBay_19_sap'] = 'false'


            # Load a machine-specific config file and attach as array 'config'
            try:
                machine = str(r.get('machine') or '').strip()
                config_values = []
                if machine:
                    config_dir = Path('C:/files/config')
                    # Try the standard file name pattern with suffix _00
                    candidates = [config_dir / f"{machine}_00.txt"]
                    target_file = None
                    for p in candidates:
                        if p.exists() and p.is_file():
                            target_file = p
                            break
                    if target_file:
                        try:
                            with target_file.open('r', encoding='utf-8', errors='ignore') as f:
                                raw_lines = [line.strip() for line in f]
                            # Use ';' as an element separator across the whole file
                            # 1) Remove empty lines, join with ';' to form a single string
                            joined = ';'.join([ln for ln in raw_lines if ln != ''])
                            # 2) Split by ';', trim tokens, and drop empty tokens
                            tokens = [tok.strip() for tok in joined.split(';') if tok.strip() != '']
                            # 3) Ignore the first 11 values
                            config = tokens[11:] if len(tokens) > 11 else []
                            config_values.append = [{config[i]: config[i + 1]} for i in range(0, len(config), 2)]
                            # Add the second element as {"machine": "xxyyzz"} placeholder based on tokens[1]
                            if len(tokens) >= 2:
                                config_values.append = [{"machine": tokens[1]}]
                        except Exception as e:
                            logging.error(f"Fehler beim Lesen der Config-Datei für Maschine '{machine}': {e}")
                            config_values = []
                    else:
                        # Datei nicht gefunden, leeres Array belegen
                        config_values = []
                r['config'] = config_values
            except Exception as e:
                logging.error(f"Unerwarteter Fehler beim Verarbeiten der Maschinen-Config: {e}")
                r['config'] = []

        if records:
            # Nur Datensätze einfügen, deren 'Sequenz' noch nicht in MongoDB existiert (und nicht leer ist)
            # 1) Alle nicht-leeren 'sequenz'-Werte aus den gelesenen Records sammeln
            seq_values = {
                (r.get('sequenz') or '').strip()
                for r in records
                if isinstance(r.get('sequenz'), str) and r.get('sequenz').strip()
            }

            # 2) Bereits vorhandene 'Sequenz' aus Mongo ermitteln (ein Aufruf)
            existing_seq = set()
            if seq_values:
                try:
                    existing_seq = set(collection.distinct('sequenz', {'sequenz': {'$in': list(seq_values)}}))
                except Exception as e:
                    logging.error(f"Fehler beim Abfragen vorhandener 'sequenz' in MongoDB: {e}")
                    existing_seq = set()

            # 3) Records filtern: nur solche mit nicht-leerer 'sequenz', die noch nicht existiert
            new_records = [
                r for r in records
                if isinstance(r.get('sequenz'), str) and r.get('sequenz').strip() and r.get('sequenz') not in existing_seq
            ]

            skipped_no_seq = sum(1 for r in records if not (isinstance(r.get('sequenz'), str) and r.get('sequenz').strip()))
            skipped_existing = sum(1 for r in records if isinstance(r.get('sequenz'), str) and r.get('sequenz').strip() and r.get('sequenz') in existing_seq)

            if new_records:
                # Explizite Typ-Konvertierung für MongoDB
                mongo_records = [{str(k): v for k, v in record.items()} for record in new_records]
                result = collection.insert_many(mongo_records)
                logging.info(f"{len(result.inserted_ids)} neue Datensätze in MongoDB gespeichert (übersprungen: {skipped_existing} bereits vorhanden, {skipped_no_seq} ohne 'sequenz')")
            else:
                logging.info(f"Keine neuen Datensätze zum Speichern (übersprungen: {skipped_existing} bereits vorhanden, {skipped_no_seq} ohne 'sequenz')")
        else:
            logging.warning("Keine Daten zum Speichern gefunden")

    except Exception as e:
        logging.error(f"Fehler beim Verarbeiten der Excel-Datei: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()