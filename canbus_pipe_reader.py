import threading
import json
import time
import os
import logging
from shared_data import latest_canbus_data, canbus_lock

PIPE_PATH = "/tmp/canbus_pipe"

class CanbusPipeReader(threading.Thread):
    """Reads messages from the named pipe and updates shared CAN bus data."""

    def __init__(self):
        super().__init__()
        self.running = True

    def run(self):
        """Continuously reads CAN bus messages and updates shared memory."""
        if not os.path.exists(PIPE_PATH):
            os.mkfifo(PIPE_PATH)

        while self.running:
            try:
                with open(PIPE_PATH, "r") as fifo:
                    while self.running:
                        line = fifo.readline().strip()
                        if not line:
                            continue

                        message = json.loads(line)
                        timestamp = message.get("timestamp", time.time())

                        with canbus_lock:
                            if "PGNname" in message:
                                latest_canbus_data[message["PGNname"]] = {
                                    "value": message["value"],
                                    "timestamp": timestamp,
                                }
            except Exception as e:
                logging.error(f"Error reading from CAN bus pipe: {e}")

            time.sleep(1)
