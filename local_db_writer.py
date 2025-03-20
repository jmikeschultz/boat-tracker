import threading
import time
import sqlite3
import gpsd
import logging
import traceback
from shared_data import latest_canbus_data, canbus_lock, initialize_sqlite, calculate_distance
import mytime

logger = logging.getLogger(__name__)

MIN_MILES_DELTA = 0.10  # miles
ENGINE_OFF_HEARTBEAT_SECS = 60 #3600
ENGINE_ON_HEARTBEAT_SECS = 30 #60
CANBUS_TIMEOUT = 10  # Stale data timeout
READ_LOOP_SLEEP_SECS = 5
LAST_UPLOADED_QUERY = "SELECT latitude, longitude, altitude, utc_shifted_tstamp FROM gps_data ORDER BY utc_shifted_tstamp DESC LIMIT 1"

class MyGPSData:
    def __init__(self, lat, lon, alt):
        self.lat = lat
        self.lon = lon
        self.alt = alt

class LocalDatabaseWriter(threading.Thread):
    """Writes GPS and CAN data to the SQLite database."""

    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name
        self.running = True
        self.stop_event = threading.Event()

    def run(self):
        """Main loop that collects GPS and CAN data and writes it to SQLite."""
        if not self.establish_gps_connection():
            logging.critical("GPSD is unavailable. Exiting thread.")
            return

        while self.running and not self.stop_event.is_set():
            try:
                gps_data = gpsd.get_current()
                
                if not gps_data or gps_data.mode < 2:
                    logging.warning("No GPS fix. Skipping update.")
                    self.stop_event.wait(READ_LOOP_SLEEP_SECS)
                    continue

                self.process(gps_data)
                self.stop_event.wait(READ_LOOP_SLEEP_SECS)  # Allows immediate shutdown

            except Exception as e:
                logging.error(f"Error in main loop: {e}")
                traceback.print_exc()
                self.stop_event.wait(READ_LOOP_SLEEP_SECS)

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

        rpm = self.get_latest_canbus("Engine RPM")
        utc_shifted_tstamp = mytime.get_shifted_timestamp(mytime.get_timezone(tz_offset))

        gps_data = self.get_updateable(gps_data, utc_shifted_tstamp, rpm)
        if gps_data is None:
            return

        engine_hours = self.get_latest_canbus("Engine Hours")
        coolant_temp = self.get_latest_canbus("Coolant Temperature")
        alternator_voltage = self.get_latest_canbus("Alternator Voltage")

        latitude, longitude, altitude = gps_data.lat, gps_data.lon, gps_data.alt

        # Open SQLite connection per transaction
        conn = sqlite3.connect(self.db_name, check_same_thread=False)
        try:
            c = conn.cursor()
            c.execute(
                """
                INSERT INTO gps_data
                (tz_offset, utc_shifted_tstamp, latitude, longitude, altitude, rpm,
                engine_hours, coolant_temp, alternator_voltage)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (tz_offset, utc_shifted_tstamp, latitude, longitude, altitude, rpm,
                 engine_hours, coolant_temp, alternator_voltage),
            )
            conn.commit()
            logging.info(f"Local DB Write: lat:{latitude}, lon:{longitude}, alt:{altitude}, "
                         f"rpm:{rpm}, engine_hours:{engine_hours}, coolant_temp:{coolant_temp}, "
                         f"alternator_voltage:{alternator_voltage}")
        finally:
            conn.close()  # Ensure DB connection is closed properly

    def get_updateable(self, gps_data, utc_shifted_tstamp, rpm):
        """Determines if new GPS data should be stored based on distance and time threshold."""
        engine_on = rpm is not None and rpm > 0
        heartbeat_secs = ENGINE_ON_HEARTBEAT_SECS if engine_on else ENGINE_OFF_HEARTBEAT_SECS

        conn = sqlite3.connect(self.db_name, check_same_thread=False)
        try:
            c = conn.cursor()
            c.execute(LAST_UPLOADED_QUERY)
            last_record = c.fetchone()

            if last_record is None:
                logging.info('UPDATE: Because no last record.')
                return gps_data

            last_lat, last_lon, last_alt, last_utc_shifted_tstamp = last_record
            distance = calculate_distance(gps_data.lat, gps_data.lon, last_lat, last_lon)
            time_diff_secs = utc_shifted_tstamp - last_utc_shifted_tstamp

            if distance > MIN_MILES_DELTA:
                logging.info(f'UPDATE: Distance threshold exceeded ({distance} miles).')
                return gps_data

            if time_diff_secs > heartbeat_secs:
                logging.info(f'UPDATE: Heartbeat threshold exceeded ({time_diff_secs} sec).')
                return MyGPSData(last_lat, last_lon, last_alt)

            logging.info(f'no_update: time_diff:{time_diff_secs} distance_delta_miles:{distance}')
            return None  # No update needed
        finally:
            conn.close()

    def get_latest_canbus(self, name):
        """Returns the most recent CAN bus value or None if stale."""
        with canbus_lock:
            data = latest_canbus_data.get(name)
            if data and (time.time() - data["timestamp"] <= CANBUS_TIMEOUT):
                return float(data["value"])
            return None  # Mark as unknown

    def stop(self):
        """Signals the thread to stop gracefully."""
        self.running = False
        self.stop_event.set()
