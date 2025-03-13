import threading
import time
import sqlite3
import gpsd
import logging
from shared_data import latest_canbus_data, canbus_lock, initialize_sqlite, calculate_distance
import mytime

# Configuration constants
MIN_DISTANCE = 0.1  # Minimum movement (miles) before storing a new record
HEARTBEAT_SECS = 60  # Time interval for forced GPS writes
CANBUS_TIMEOUT = 10  # Maximum allowed age of CAN bus data
READ_LOOP_SLEEP_SECS = 5  # Sleep duration between GPS reads

class LocalDatabaseWriter(threading.Thread):
    """Writes GPS and CAN data to the SQLite database efficiently."""

    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name
        self.running = True

    def run(self):
        gpsd.connect()
        conn = sqlite3.connect(self.db_name, check_same_thread=False)
        c = conn.cursor()

        last_write_time = time.time()

        while self.running:
            try:
                gps_data = gpsd.get_current()

                if gps_data.mode == 0:
                    logging.info("GPS data skipped: No valid fix available.")
                    time.sleep(READ_LOOP_SLEEP_SECS)
                    continue

                latitude = gps_data.lat
                longitude = gps_data.lon
                altitude = gps_data.alt
                tz_offset = mytime.get_tz_offset(latitude, longitude)
                utc_shifted_tstamp = mytime.get_shifted_timestamp(mytime.get_timezone(tz_offset))

                # Retrieve latest CAN bus values
                rpm               = self.get_latest_canbus("Engine RPM")
                engine_hours      = self.get_latest_canbus("Engine Hours")
                coolant_temp      = self.get_latest_canbus("Coolant Temperature")
                alternator_voltage = self.get_latest_canbus("Alternator Voltage")

                # Skip if timezone offset is unknown
                if tz_offset == "Unknown":
                    logging.warning("Skipping record due to unknown time zone.")
                    time.sleep(READ_LOOP_SLEEP_SECS)
                    continue

                # Check if this data is worth updating
                if self.should_update(latitude, longitude, utc_shifted_tstamp, rpm, conn, last_write_time):
                    c.execute(
                        """
                        INSERT INTO gps_data
                        (tz_offset, utc_shifted_tstamp, latitude, longitude, altitude, rpm, engine_hours, coolant_temp, alternator_voltage)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (tz_offset, utc_shifted_tstamp, latitude, longitude, altitude, rpm, engine_hours, coolant_temp, alternator_voltage),
                    )
                    conn.commit()

                    last_write_time = time.time()

                    # Log the recorded values for debugging
                    logging.info(
                        f"Local DB Write: lat:{latitude}, lon:{longitude}, "
                        f"alt:{altitude}, rpm:{rpm}, engine_hours:{engine_hours}, "
                        f"coolant_temp:{coolant_temp}, alternator_voltage:{alternator_voltage}"
                    )

            except Exception as e:
                logging.error(f"Failed to store GPS data: {e}")

            time.sleep(READ_LOOP_SLEEP_SECS)

        conn.close()

    def get_latest_canbus(self, name):
        """Returns the most recent CAN bus value or None if stale."""
        with canbus_lock:
            data = latest_canbus_data.get(name)
            if data and (time.time() - data["timestamp"] <= CANBUS_TIMEOUT):
                return float(data["value"])
            return None  # Mark as unknown

    def should_update(self, latitude, longitude, utc_shifted_tstamp, rpm, conn, last_write_time):
        """Determines if the GPS data should be stored to reduce redundant writes."""
        if time.time() - last_write_time > HEARTBEAT_SECS:
            logging.info("Forcing GPS write due to heartbeat interval.")
            return True  # Enforce periodic writes regardless of movement

        c = conn.cursor()
        c.execute("SELECT latitude, longitude, utc_shifted_tstamp FROM gps_data ORDER BY utc_shifted_tstamp DESC LIMIT 1")
        last_record = c.fetchone()

        if not last_record:
            return True  # No previous records, must store

        last_lat, last_lon, last_utc_shifted_tstamp = last_record
        distance = calculate_distance(latitude, longitude, last_lat, last_lon)
        time_diff_secs = utc_shifted_tstamp - last_utc_shifted_tstamp

        if distance > MIN_DISTANCE:
            logging.info(f"Storing new GPS record: Moved {distance:.2f} miles.")
            return True  # Store data if movement is significant

        return False  # No need to update

