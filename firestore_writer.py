import threading
import time
import sqlite3
import logging
from google.cloud import firestore
from google.api_core.exceptions import GoogleAPICallError

BATCH_SIZE = 50  # Ensure this does not exceed Firestore's 500-limit
UPLOAD_INTERVAL = 30  # Interval to check for new data
MAX_RETRIES = 3  # Retry failed uploads

class FirestoreDatabaseWriter(threading.Thread):
    """Uploads GPS data from SQLite to Firestore."""

    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name
        self.running = True
        self.stop_event = threading.Event()

    def run(self):
        """Main loop to upload data to Firestore."""
        while self.running and not self.stop_event.is_set():
            try:
                # Open SQLite connection for each cycle
                conn = sqlite3.connect(self.db_name, check_same_thread=False)
                c = conn.cursor()

                # Fetch batch of unuploaded records
                c.execute("SELECT * FROM gps_data WHERE uploaded = 0 LIMIT ?", (BATCH_SIZE,))
                rows = c.fetchall()

                if not rows:
                    logging.info("No new data to upload. Sleeping...")
                    conn.close()
                    time.sleep(UPLOAD_INTERVAL)
                    continue

                logging.info(f"Found {len(rows)} records to upload.")
                
                # Initialize Firestore client inside loop to handle dropped connections
                db = firestore.Client()

                success = self.upload_to_firestore(db, rows)

                if success:
                    # Only update SQLite if Firestore commit was successful
                    row_ids = [(row[0],) for row in rows]
                    c.executemany("UPDATE gps_data SET uploaded = 1 WHERE id = ?", row_ids)
                    conn.commit()
                    logging.info(f"Marked {len(rows)} records as uploaded in SQLite.")

                conn.close()

            except Exception as e:
                logging.error(f"Firestore upload loop error: {e}")

            time.sleep(UPLOAD_INTERVAL)

    def upload_to_firestore(self, db, rows):
        """Uploads data to Firestore with error handling."""
        for attempt in range(MAX_RETRIES):
            try:
                batch = db.batch()  # Start Firestore batch operation

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
                    doc_ref = db.collection("gps_data").document()
                    batch.set(doc_ref, doc)  # Add to batch

                batch.commit()  # Execute batch upload
                logging.info(f"Uploaded {len(rows)} records to Firestore.")
                return True

            except GoogleAPICallError as e:
                logging.error(f"Firestore upload failed (Attempt {attempt+1}/{MAX_RETRIES}): {e}")
                time.sleep(2**attempt)  # Exponential backoff

        logging.error("Max retries reached. Skipping batch.")
        return False

    def stop(self):
        """Signal thread to stop gracefully."""
        self.running = False
        self.stop_event.set()
