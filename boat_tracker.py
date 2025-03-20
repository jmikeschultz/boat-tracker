import threading
import time
import logging
from local_db_writer import LocalDatabaseWriter
from firestore_writer import FirestoreDatabaseWriter
from canbus_pipe_reader import CanbusPipeReader
from shared_data import initialize_sqlite
from threading import Event
import os

# Configure logging
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[logging.StreamHandler()]  # to stdout
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    db_name = "boat_tracker.db"
    
    # Ensure the SQLite table exists before starting threads
    initialize_sqlite(db_name)

    local_writer = LocalDatabaseWriter(db_name)
    firestore_writer = FirestoreDatabaseWriter(db_name)
    canbus_reader = CanbusPipeReader()

    local_writer.start()
    firestore_writer.start()
    canbus_reader.start()

    stop_event = Event()

    try:
        while not stop_event.wait(5):  # Only wake up every 5 seconds
            pass

    except KeyboardInterrupt:
        logging.info("Stopping threads...")
        local_writer.running = False
        firestore_writer.running = False
        canbus_reader.running = False

        local_writer.join()
        firestore_writer.join()
        canbus_reader.join()

    logging.info("Stopped.")
