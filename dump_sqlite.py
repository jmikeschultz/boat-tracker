import sqlite3
import sys

def dump_sqlite_to_stdout(db_file: str):
    """
    Dumps all records from a SQLite database to stdout.

    Args:
        db_file: Path to the SQLite database file.
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

        # Iterate through each table and dump its records
        for table_name, in tables:
            print(f"\nTable: {table_name}")
            print("-" * 40)

            # Fetch and print all records from the table
            cursor.execute(f"SELECT * FROM {table_name};")
            rows = cursor.fetchall()

            if not rows:
                print("(No records)")
            else:
                # Fetch and print column names
                column_names = [description[0] for description in cursor.description]
                print("\t".join(column_names))
                for row in rows:
                    print("\t".join(map(str, row)))

        conn.close()

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python dump_sqlite_to_stdout.py <db_file>")
        sys.exit(1)

    db_file = sys.argv[1]
    dump_sqlite_to_stdout(db_file)
