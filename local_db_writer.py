import threading
import time
import sqlite3
import gpsd
import logging
import traceback
from datetime import datetime, timezone
from shared_data import latest_canbus_data, canbus_lock, initialize_sqlite, calculate_distance, db_expected_fields
import mytime
import json
import os

logger = logging.getLogger(__name__)

HEARTBEAT_SECS = 3600 * 1
CANBUS_TIMEOUT = 10  # Stale data timeout
GPS_LOOP_SECS = 1
GPS_OUTPUT_PATH = "/home/mike/.cache/boat/current_position.json"
LAST_RECORD = "SELECT latitude, longitude, rpm, utc_shifted_tstamp FROM gps_data WHERE uploaded != 2 ORDER BY utc_shifted_tstamp DESC LIMIT 1"

NOT_UPLOADED = 0
NODELTA_UPLOADED = 2

def write_current_location(lat, lon, secs_at_location):
    try:
        os.makedirs(os.path.dirname(GPS_OUTPUT_PATH), exist_ok=True)
        with open(GPS_OUTPUT_PATH + ".tmp", "w") as f:
            json.dump({
                "lat": lat,
                "lon": lon,
                "secs_at_location": secs_at_location,
                "ts": time.time()
            }, f)
        os.replace(GPS_OUTPUT_PATH + ".tmp", GPS_OUTPUT_PATH)
    except Exception as e:
        logger.warning(f"Failed to write GPS location for wlan1_manager: {e}")


def insert_row(conn, data):
    """
    Insert a new row into the given table using fixed expected fields.
    Missing fields are set to NULL. Uses try/finally for safe resource cleanup.
    """
    table_name = 'gps_data'
    columns = ", ".join(db_expected_fields)
    placeholders = ", ".join(f":{field}" for field in db_expected_fields)

    full_data = {field: data.get(field) for field in db_expected_fields}

    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    c = conn.cursor()
    try:
        c.execute(sql, full_data)
        conn.commit()
    finally:
        c.close()


def upsert_nodelta_row(conn, data):
    """
    Update the row where uploaded = 2, or insert a new one if none exists.
    (No 'updated' field.)
    """
    table_name = 'gps_data'
    set_clause = ", ".join(f"{field} = :{field}" for field in db_expected_fields)
    columns = ", ".join(db_expected_fields)
    placeholders = ", ".join(f":{field}" for field in db_expected_fields)
    
    full_data = {field: data.get(field) for field in db_expected_fields}
    
    c = conn.cursor()
    try:
        # First check if a row with uploaded=2 exists
        c.execute(f"SELECT id FROM {table_name} WHERE uploaded = 2 LIMIT 1")
        row = c.fetchone()

        if row:
            # UPDATE
            sql = f"UPDATE {table_name} SET {set_clause} WHERE uploaded = 2"
            c.execute(sql, full_data)
        else:
            # INSERT
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            c.execute(sql, full_data)

        conn.commit()
    finally:
        c.close()

def publish_nodelta_row(conn):
    """
    Set uploaded = 0 for row where uploaded = 2.
    """
    table_name = 'gps_data'
    sql = f"UPDATE {table_name} SET uploaded = {NOT_UPLOADED} WHERE uploaded = {NODELTA_UPLOADED}"
    c = conn.cursor()
    try:
        c.execute(sql)
        conn.commit()
    finally:
        c.close()
        
class LocalDatabaseWriter(threading.Thread):
    """Writes GPS and CAN data to the SQLite database."""

    def __init__(self, db_file):
        super().__init__()
        self.db_file = db_file
        self.running = True
        self.stop_event = threading.Event()

    def run(self):
        num_loops = 0
        """Main loop that collects GPS and CAN data and writes it to SQLite."""

        if not self.establish_gps_connection():
            logging.critical("GPSD is unavailable. Exiting thread.")
            return

        while self.running and not self.stop_event.is_set():
            try:
                gps_data = gpsd.get_current()
                num_loops += 1
                
                if not gps_data or gps_data.mode < 2: # mode = num satelites
                    if num_loops % 60 == 0:
                        logging.warning("No GPS fix. Skipping update.")
                    self.stop_event.wait(GPS_LOOP_SECS)
                    continue

                self.process(gps_data)
                self.stop_event.wait(GPS_LOOP_SECS)  # Allows immediate shutdown

            except Exception as e:
                logging.error(f"Error in main loop: {e}")
                traceback.print_exc()
                self.stop_event.wait(GPS_LOOP_SECS)

    def establish_gps_connection(self):
        """Attempts to establish a connection to GPSD once at startup."""
        try:
            gpsd.connect()
            logging.info("Connected to GPSD.")
            return True
        except Exception as e:
            logging.error(f"Failed to connect to GPSD: {e}")
            return False

    def process(self, gps_data):
        """Processes and writes GPS & CAN bus data to SQLite."""
        tz_offset = mytime.get_tz_offset_2(gps_data)
        if tz_offset == "Unknown":
            logging.warning("Skipping record due to unknown time zone.")
            return

        gps_dt = datetime.strptime(gps_data.time, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        gps_tstamp = gps_dt.timestamp()

        gps_knots = gps_data.speed() * 1.94384
        
        rpm = self.get_latest_canbus("RPM")
        utc_shifted_tstamp = mytime.shift_timestamp(gps_tstamp, mytime.get_timezone(tz_offset))
        utc_shifted_tstamp_old = mytime.get_shifted_timestamp(mytime.get_timezone(tz_offset))

        #print('gps timestamp', utc_shifted_tstamp)
        #print('legacy timestamp', utc_shifted_tstamp_old)
        #print('gps - leg', utc_shifted_tstamp - utc_shifted_tstamp_old)        

        is_delta, secs_diff = self.is_delta(gps_data, utc_shifted_tstamp, rpm)
        uploaded = NOT_UPLOADED if is_delta else NODELTA_UPLOADED
        
        sql_data = dict(tz_offset=tz_offset,
                        utc_shifted_tstamp=utc_shifted_tstamp,
                        latitude=gps_data.lat,
                        longitude=gps_data.lon,
                        altitude=gps_data.alt,
                        gps_knots=gps_knots,
                        rpm=rpm,
                        engine_hours=self.get_latest_canbus("Hours"),
                        coolant_temp=self.get_latest_canbus("CoolantTemp"),
                        alternator_voltage=self.get_latest_canbus("BatteryVoltage"),
                        is_delta=1 if is_delta else 0,
                        uploaded=uploaded)

        write_current_location(gps_data.lat, gps_data.lon, secs_diff)

        # Open SQLite connection per transaction
        conn = sqlite3.connect(self.db_file)
        try:
            if is_delta:
                publish_nodelta_row(conn)
                insert_row(conn, sql_data)
                logging.debug(f"Local DB Write: lat:{gps_data.lat}, lon:{gps_data.lon}, alt:{gps_data.alt} rpm:{rpm} gps_knots:{gps_knots}")
            else:
                upsert_nodelta_row(conn, sql_data)
                if secs_diff > HEARTBEAT_SECS:
                    publish_nodelta_row(conn)
                #logging.info('no delta')
        finally:
            conn.close()  # Ensure DB connection is closed properly

    def is_delta(self, gps_data, utc_shifted_tstamp, rpm) -> (bool, float):
        conn = sqlite3.connect(self.db_file)
        try:
            c = conn.cursor()
            c.execute(LAST_RECORD) # last record not the nodelta-record (when uploaded != 2)
            last_record = c.fetchone()

            if last_record is None:
                logging.info('UPDATE: Because no last record.')
                return True, 0.0

            last_lat, last_lon, last_rpm, last_utc_shifted_tstamp = last_record
            delta_miles = calculate_distance(gps_data.lat, gps_data.lon, last_lat, last_lon)
            delta_secs = utc_shifted_tstamp - last_utc_shifted_tstamp
            mph = delta_miles / (delta_secs / 3600.0)

            if mph < 0.10:
                MIN_MILES_DELTA = 0.10 # 540 ft
            else:
                MIN_MILES_DELTA = 0.006 # 32

            #print(f'delta_miles:{delta_miles} delta_secs:{delta_secs} mph:{mph} MIN_MILES_DELTA:{MIN_MILES_DELTA}')

            if delta_miles > MIN_MILES_DELTA:
                return True, delta_secs

            rpm = 0 if rpm is None else rpm
            last_rpm = 0 if last_rpm is None else last_rpm
            
            if abs(rpm - last_rpm) >= 50:
                return True, delta_secs

        finally:
            conn.close()

        # case of no delta
        # make the position be exactly the last position so we can recognize it easy
        gps_data.lat = last_lat
        gps_data.lon = last_lon
        return False, delta_secs            
        
    def get_latest_canbus(self, name):
        """Returns the most recent CAN bus value or None if stale."""
        with canbus_lock:
            data = latest_canbus_data.get(name)
            if data and (time.time() - data["timestamp"] <= CANBUS_TIMEOUT):
                val = float(data["value"])
                if val < 0:
                    return None
                return val
            return None  # Mark as unknown

    def stop(self):
        """Signals the thread to stop gracefully."""
        self.running = False
        self.stop_event.set()
