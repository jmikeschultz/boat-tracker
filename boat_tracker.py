import threading
import time
import sqlite3
import gps
import logging
from google.cloud import firestore
from google.api_core.exceptions import GoogleAPICallError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_sqlite(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS gps_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            latitude REAL,
            longitude REAL,
            altitude REAL,
            speed REAL,
            rpm REAL,  -- New column
            uploaded INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()

# Local Database Writer Class
class LocalDatabaseWriter(threading.Thread):
    def __init__(self, db_name, write_interval_slow, write_interval_fast, speed_threshold):
        super().__init__()
        self.db_name = db_name
        self.write_interval_slow = write_interval_slow
        self.write_interval_fast = write_interval_fast
        self.speed_threshold = speed_threshold
        self.gpsd = gps.gps(mode=gps.WATCH_ENABLE | gps.WATCH_NEWSTYLE)
        self.running = True
        self.last_write_time = 0

    def run(self):
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()

        while self.running:
            gps_data = self.gpsd.next()
            if gps_data['class'] == 'TPV':
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                latitude = getattr(gps_data, 'lat', None)
                longitude = getattr(gps_data, 'lon', None)
                altitude = getattr(gps_data, 'alt', None)
                speed = getattr(gps_data, 'speed', 0) or 0
                rpm = 0  # Placeholder for now

                current_time = time.time()
                interval = self.write_interval_fast if speed > self.speed_threshold else self.write_interval_slow

                if current_time - self.last_write_time >= interval:
                    c.execute(
                        "INSERT INTO gps_data (timestamp, latitude, longitude, altitude, speed, rpm) VALUES (?, ?, ?, ?, ?, ?)",
                        (timestamp, latitude, longitude, altitude, speed, rpm)
                    )
                    conn.commit()
                    self.last_write_time = current_time
                    logging.info(f"Local DB Write: {timestamp}, {latitude}, {longitude}, {altitude}, {speed}, {rpm}")

            time.sleep(1)

        conn.close()

# Firestore Database Writer Class
class FirestoreDatabaseWriter(threading.Thread):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name
        self.db = firestore.Client()
        self.running = True

    def run(self):
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()

        while self.running:
            c.execute("SELECT * FROM gps_data WHERE uploaded = 0")
            rows = c.fetchall()

            for row in rows:
                doc = {
                    "timestamp": row[1],
                    "latitude": row[2],
                    "longitude": row[3],
                    "altitude": row[4],
                    "speed": row[5],
                    "rpm": row[6]
                }
                try:
                    self.db.collection("gps_data").add(doc)
                    logging.info(f"Uploaded to Firestore: {row}")
                    c.execute("UPDATE gps_data SET uploaded = 1 WHERE id = ?", (row[0],))
                    conn.commit()
                except Exception as e:
                    logging.error(f"Failed to upload record {row[0]}: {e}")

            time.sleep(30)

        conn.close()

if __name__ == "__main__":
    db_name = "boat_gps_data.db"

    # Ensure the SQLite table exists before starting threads
    initialize_sqlite(db_name)

    write_interval_slow = 5  # seconds
    write_interval_fast = 60  # seconds
    speed_threshold = 1  # knots

    local_writer = LocalDatabaseWriter(db_name, write_interval_slow, write_interval_fast, speed_threshold)
    firestore_writer = FirestoreDatabaseWriter(db_name)

    local_writer.start()
    firestore_writer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping threads...")
        local_writer.running = False
        firestore_writer.running = False

        local_writer.join()
        firestore_writer.join()

    logging.info("Stopped.")
