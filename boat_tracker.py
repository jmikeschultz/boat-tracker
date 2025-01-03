import threading
import time
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

def parse_tz_offset(tz_offset: str) -> timedelta:
    """
    Parses a timezone offset string in the format 'UTC±HH:MM' and returns a timedelta.

    Args:
        tz_offset: Timezone offset string (e.g., 'UTC-05:00', 'UTC+03:30')

    Returns:
        A timedelta representing the offset from UTC
    """
    # Regex to parse timezone offset
    match = re.match(r"^UTC(?P<sign>[+-])(?P<hours>\d{2}):(?P<minutes>\d{2})$", tz_offset)
    if not match:
        raise ValueError(f"Invalid timezone offset format: {tz_offset}")

    sign = -1 if match.group('sign') == '-' else 1
    hours = int(match.group('hours'))
    minutes = int(match.group('minutes'))
    return timedelta(hours=sign * hours, minutes=sign * minutes)

def initialize_sqlite(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS gps_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER, 
            tz_offset TEXT,
            latitude REAL,
            longitude REAL,
            altitude REAL,
            speed REAL,
            rpm REAL,
            gmt_timestamp INTEGER,  -- zone-adjusted UTC time
            uploaded INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()

def get_tz_offset(latitude: float, longitude: float) -> str:
    """
    Determine the UTC offset (in 'UTC±HH:MM' format) based on latitude and longitude.

    Args:
        latitude: Latitude of the location.
        longitude: Longitude of the location.

    Returns:
        UTC offset string in the format 'UTC±HH:MM' or 'Unknown' if it cannot be determined.
    """
    try:
        # Determine the timezone
        tf = TimezoneFinder()
        time_zone_name = tf.timezone_at(lat=latitude, lng=longitude)
        
        if not time_zone_name:
            return "Unknown"

        # Get the current time in the identified timezone
        timezone = pytz.timezone(time_zone_name)
        local_time = datetime.now(timezone)

        # Calculate UTC offset
        offset_seconds = local_time.utcoffset().total_seconds()
        hours, remainder = divmod(abs(offset_seconds), 3600)
        minutes = remainder // 60
        sign = '+' if offset_seconds >= 0 else '-'

        return f"UTC{sign}{int(hours):02d}:{int(minutes):02d}"
    except Exception as e:
        logging.error(f"Failed to determine UTC offset: {e}")
        return "Unknown"

def get_gmt_timestamp(timestamp: int, tz_offset: str) -> int:
    """
    Given a timestamp that shows a certain time in the input timezone,
    returns a timestamp that shows the same digits in GMT.

    Args:
        timestamp: Unix timestamp in seconds
        tz_offset: Timezone offset in format 'UTC-HH:MM' or 'UTC+HH:MM'

    Returns:
        Unix timestamp that shows the same time in GMT
    """
    timedelta = parse_tz_offset(tz_offset)
    return timestamp + int(timedelta.total_seconds())

def is_update_worthy(gps_data, timestamp, conn):
    """Determine if the GPS data should be saved."""
    if gps_data.mode == 0 or (gps_data.lat == 0 and gps_data.lon == 0 and gps_data.alt == 0 and gps_data.hspeed == 0):
        logging.info(f"GPS data skipped: No valid fix available.")
        return False

    # Get the last saved record from the database
    c = conn.cursor()
    c.execute("SELECT latitude, longitude, timestamp FROM gps_data ORDER BY timestamp DESC LIMIT 1")
    last_record = c.fetchone()

    if not last_record:
        # Always save the first record
        return True

    last_lat, last_lon, last_timestamp = last_record
    distance = calculate_distance(gps_data.lat, gps_data.lon, last_lat, last_lon)
    time_diff_secs = (timestamp - last_timestamp)

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
                timestamp = int(datetime.utcnow().timestamp())  # UTC epoch secs
                latitude = gps_data.lat
                longitude = gps_data.lon
                altitude = gps_data.alt
                speed = gps_data.hspeed or 0
                rpm = 0
                tz_offset = get_tz_offset(latitude, longitude)

                if tz_offset == "Unknown":
                    logging.warning("Skipping record due to unknown time zone.")
                    continue

                gmt_timestamp = get_gmt_timestamp(timestamp, tz_offset)
                print('hey gmt', gmt_timestamp)

                if is_update_worthy(gps_data, timestamp, conn):
                    c.execute(
                        "INSERT INTO gps_data (timestamp, tz_offset, latitude, longitude, altitude, speed, rpm, gmt_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (timestamp, tz_offset, latitude, longitude, altitude, speed, rpm, gmt_timestamp)
                    )
                    conn.commit()
                    logging.info(
                        f"Local DB Write: UTC {timestamp}, ISO {datetime.utcfromtimestamp(timestamp).isoformat()}, "
                        f"TZ_Offset {tz_offset}, GMTTimestamp {gmt_timestamp}, {latitude}, {longitude}, {altitude}, {speed}, {rpm}"
                    )
            except Exception as e:
                logging.error(f"Failed to retrieve GPS data: {e}")

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
                    "timestamp": row[1],  # UTC epoch time
                    "tz_offset": row[2],
                    "latitude": row[3],
                    "longitude": row[4],
                    "altitude": row[5],
                    "speed": row[6],
                    "rpm": row[7],
                    "gmt_timestamp": row[8]
                }
                try:
                    self.db.collection("gps_data").add(doc)
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
