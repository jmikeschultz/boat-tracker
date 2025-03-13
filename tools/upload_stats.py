#!/usr/bin/env python
import sqlite3
import sys
from datetime import datetime

def get_upload_info(db_file: str):
    """ Prints last upload time, number of records to upload, and time of the last unuploaded record. """
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Get the last upload time (uploaded = 1)
        cursor.execute(
            "SELECT utc_shifted_tstamp FROM gps_data WHERE uploaded = 1 ORDER BY utc_shifted_tstamp DESC LIMIT 1;"
        )
        last_upload = cursor.fetchone()
        last_upload_time = (
            datetime.utcfromtimestamp(last_upload[0]).strftime('%Y-%m-%d %H:%M:%S')
            if last_upload else "No uploaded records found."
        )

        # Get count of records that need to be uploaded (uploaded = 0)
        cursor.execute("SELECT COUNT(*) FROM gps_data WHERE uploaded = 0;")
        records_to_upload = cursor.fetchone()[0]

        # Get the time of the last unuploaded record (uploaded = 0)
        cursor.execute(
            "SELECT utc_shifted_tstamp FROM gps_data WHERE uploaded = 0 ORDER BY utc_shifted_tstamp DESC LIMIT 1;"
        )
        last_unuploaded = cursor.fetchone()
        last_unuploaded_time = (
            datetime.utcfromtimestamp(last_unuploaded[0]).strftime('%Y-%m-%d %H:%M:%S')
            if last_unuploaded else "No unuploaded records found."
        )

        print(f"Time of last uploaded record:\t{last_upload_time}")
        print(f"Number of records to upload:\t{records_to_upload}")
        print(f"Time of last record:\t\t{last_unuploaded_time}")

        conn.close()
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        script = sys.argv[0]
        print(f"Usage: {script} <db_file>")
        sys.exit(1)

    get_upload_info(sys.argv[1])
