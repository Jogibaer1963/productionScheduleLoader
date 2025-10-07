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
    format="%(asctime)s %(levelname)s %(message)s"
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
            engine="openpyxl",
            header=1,  # Zweite Zeile als Header verwenden (Erste Zeile überspringen)
            dtype=str,  # Alles als String → nichts "verschwindet"
            keep_default_na=False  # "NA"/"N/A" etc. bleiben Strings, nicht NaN
        )

        # Header bereinigen und anwenden
        df.columns = desired_columns
        logging.info(f"DataFrame eingelesen mit {len(df)} Zeilen und bereinigte Spalten: {list(df.columns)}")
        records = df.to_dict("records")

        for r in records:
            r['_id'] = str(uuid4())  # statt ObjectId: plain String

        if records:
            # Explizite Typ-Konvertierung für MongoDB
            mongo_records = [{str(k): v for k, v in record.items()} for record in records]
            result = collection.insert_many(mongo_records)
            logging.info(f"{len(result.inserted_ids)} Datensätze erfolgreich in MongoDB gespeichert")
        else:
            logging.warning("Keine Daten zum Speichern gefunden")

    except Exception as e:
        logging.error(f"Fehler beim Verarbeiten der Excel-Datei: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()