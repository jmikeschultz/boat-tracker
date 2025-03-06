import threading
import time
import sqlite3
import logging
from google.cloud import firestore
from google.api_core.exceptions import GoogleAPICallError

class FirestoreDatabaseWriter(threading.Thread):
    """Uploads GPS data from SQLite to Firestore."""

    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name
        self.db = firestore.Client()
        self.running = True

    def run(self):
        conn = sqlite3.connect(self.db_name, check_same_thread=False)
        c = conn.cursor()

        while self.running:
            c.execute("SELECT * FROM gps_data WHERE uploaded = 0")
            rows = c.fetchall()

            for row in rows:
                doc = {
                    "tz_offset": row[1],
                    "utc_shifted_tstamp": row[2],
                    "latitude": row[3],
                    "longitude": row[4],
                    "altitude": row[5],
                    "rpm": row[6],
                }
                try:
                    self.db.collection("gps_data1").add(doc)
                    logging.info(f"Uploaded to Firestore: {row}")
                    c.execute("UPDATE gps_data SET uploaded = 1 WHERE id = ?", (row[0],))
                    conn.commit()
                except GoogleAPICallError as e:
                    logging.error(f"Failed to upload record {row[0]}: {e}")

            time.sleep(30)

        conn.close()
