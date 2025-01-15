import threading
import time
import mytime
import sqlite3
import gpsd
import pytz
import re
import os
import logging
from google.cloud import firestore
from google.api_core.exceptions import GoogleAPICallError
from datetime import datetime, timezone, timedelta
from timezonefinder import TimezoneFinder
from geopy.distance import geodesic

MIN_DISTANCE = 0.1  # miles
HEARTBEAT_SECS = 30  # 30 seconds for testing, adjust as needed

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_sqlite(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS gps_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tz_offset TEXT,
            utc_shifted_tstamp REAL,
            latitude REAL,
            longitude REAL,
            altitude REAL,
            rpm REAL,
            uploaded INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()

def is_update_worthy(gps_data, utc_shifted_tstamp, conn):
    """Determine if the GPS data should be saved."""
    if gps_data.mode == 0 or (gps_data.lat == 0 and gps_data.lon == 0 and gps_data.alt == 0 and gps_data.hspeed == 0):
        logging.info(f"GPS data skipped: No valid fix available.")
        return False

    # Get the last saved record from the database
    c = conn.cursor()
    c.execute("SELECT latitude, longitude, utc_shifted_tstamp FROM gps_data ORDER BY utc_shifted_tstamp DESC LIMIT 1")
    last_record = c.fetchone()

    if not last_record:
        # Always save the first record
        return True

    last_lat, last_lon, last_utc_shifted_tstamp = last_record
    distance = calculate_distance(gps_data.lat, gps_data.lon, last_lat, last_lon)
    time_diff_secs = (utc_shifted_tstamp - last_utc_shifted_tstamp)

    # Save if distance exceeds the threshold or if enough time has passed
    return distance > MIN_DISTANCE or time_diff_secs > HEARTBEAT_SECS

def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculate the distance between two lat/lon points in miles."""
    return geodesic((lat1, lon1), (lat2, lon2)).miles

class LocalDatabaseWriter(threading.Thread):
    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name
        self.running = True

    def run(self):
        gpsd.connect()
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()

        while self.running:
            try:
                gps_data = gpsd.get_current()
                latitude = gps_data.lat
                longitude = gps_data.lon
                altitude = gps_data.alt
                # speed = gps_data.hspeed or 0 we don't trust speed from the rpi hat
                tz_offset = mytime.get_tz_offset(latitude, longitude)
                rpm = 0

                if tz_offset == "Unknown":
                    logging.warning("Skipping record due to unknown time zone.")
                    continue
    
                local_tz = mytime.get_timezone(tz_offset)
                utc_shifted_tstamp = mytime.get_shifted_timestamp(local_tz)
                if is_update_worthy(gps_data, utc_shifted_tstamp, conn):
                    c.execute(
                        "INSERT INTO gps_data (tz_offset, utc_shifted_tstamp, latitude, longitude, altitude, rpm) VALUES (?, ?, ?, ?, ?, ?)",
                        (tz_offset, utc_shifted_tstamp, latitude, longitude, altitude, rpm)
                    )
                    conn.commit()
                    local_tstamp = mytime.unshift_timestamp(utc_shifted_tstamp, local_tz)
                    logging.info(
                        f"Local DB Write: {tz_offset}, shifted:{datetime.fromtimestamp(utc_shifted_tstamp, timezone.utc)} local:{datetime.fromtimestamp(local_tstamp, local_tz)}, "
                        f"lat:{latitude}, lon:{longitude}, alt:{altitude}, rpm:{rpm}"
                    )
            except Exception as e:
                logging.error(f"Failed to store GPS data: {e}")

            time.sleep(1)

        conn.close()

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
                    "tz_offset": row[1],
                    "utc_shifted_tstamp": row[2],  # UTC epoch time                    
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

if __name__ == "__main__":
    db_name = "boat_tracker.db"

    # Ensure the SQLite table exists before starting threads
    initialize_sqlite(db_name)

    local_writer = LocalDatabaseWriter(db_name)
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
