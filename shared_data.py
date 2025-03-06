import sqlite3
import threading
from geopy.distance import geodesic

# Shared dictionary for CAN bus data
latest_canbus_data = {}
canbus_lock = threading.Lock()

def initialize_sqlite(db_name):
    """Initializes the SQLite database with required tables."""
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

def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculate the distance between two lat/lon points in miles."""
    return geodesic((lat1, lon1), (lat2, lon2)).miles
