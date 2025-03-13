import threading
import time
import sqlite3
import logging
from google.cloud import firestore
from google.api_core.exceptions import GoogleAPICallError

BATCH_SIZE = 10  # Process 10 records per cycle
UPLOAD_INTERVAL = 30  # Check for new data every 30 seconds

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
            try:
                # Select only a limited batch of unuploaded records
                c.execute("SELECT * FROM gps_data WHERE uploaded = 0 LIMIT ?", (BATCH_SIZE,))
                rows = c.fetchall()

                if not rows:
                    logging.info("No new data to upload. Sleeping...")
                    time.sleep(UPLOAD_INTERVAL)
                    continue

                batch = self.db.batch()  # Start a Firestore batch transaction

                for row in rows:
                    doc = {
                        "tz_offset": row[1],
                        "utc_shifted_tstamp": row[2],
                        "latitude": row[3],
                        "longitude": row[4],
                        "altitude": row[5],
                        "rpm": row[6],
                        "engine_hours": row[7],
                        "coolant_temp": row[8],
                        "alternator_voltage": row[9]
                    }
                    try:
                        doc_ref = self.db.collection("gps_data1").document()
                        batch.set(doc_ref, doc)  # Add to batch
                        logging.info(f"Queued for upload: {row}")
                    except GoogleAPICallError as e:
                        logging.error(f"Failed to queue record {row[0]}: {e}")

                batch.commit()  # Execute batch upload
                logging.info(f"Uploaded {len(rows)} records to Firestore.")

                # Mark uploaded records in SQLite
                row_ids = [row[0] for row in rows]
                c.executemany("UPDATE gps_data SET uploaded = 1 WHERE id = ?", [(row_id,) for row_id in row_ids])
                conn.commit()

            except Exception as e:
                logging.error(f"Firestore upload failed: {e}")

            time.sleep(UPLOAD_INTERVAL)  # Sleep before next batch

        conn.close()
