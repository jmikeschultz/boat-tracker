#!/usr/bin/env python
import sqlite3
import sys
from datetime import datetime

def dump_sqlite_to_stdout(db_file: str, upload_filter: str = None):
    """
    Dumps all records from a SQLite database to stdout, formatting timestamps and sorting by time.

    Args:
        db_file: Path to the SQLite database file.
        upload_filter: Optional filter for 'uploaded' column.
    """
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Get the list of tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        if not tables:
            print("No tables found in the database.")
            return

        # Iterate through each table
        for table_name, in tables:
            if table_name != "gps_data":
                continue  # Process only gps_data table

            print(f"\nTable: {table_name}")
            print("-" * 40)

            # Construct query with sorting and optional filter
            query = f"SELECT * FROM {table_name}"
            if upload_filter is not None:
                query += f" WHERE uploaded = ?"
            query += " ORDER BY utc_shifted_tstamp ASC;"

            # Execute query
            if upload_filter is not None:
                cursor.execute(query, (upload_filter,))
            else:
                cursor.execute(query)

            rows = cursor.fetchall()
            if not rows:
                print("(No records)")
            else:
                # Fetch column names
                column_names = [description[0] for description in cursor.description]
                print("\t".join(column_names))

                # Find index of utc_shifted_tstamp
                time_idx = column_names.index("utc_shifted_tstamp")

                # Print formatted rows
                for row in rows:
                    row = list(row)  # Convert to mutable list
                    # Convert timestamp to human-readable time
                    row[time_idx] = datetime.utcfromtimestamp(row[time_idx]).strftime('%Y-%m-%d %H:%M:%S')
                    print("\t".join(map(str, row)))

        conn.close()

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python dump_sqlite_to_stdout.py <db_file> [isUploaded]")
        sys.exit(1)

    db_file = sys.argv[1]
    upload_filter = sys.argv[2] if len(sys.argv) == 3 else None
    dump_sqlite_to_stdout(db_file, upload_filter)
