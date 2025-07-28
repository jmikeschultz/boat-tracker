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
        if not os.path.exists(PIPE_PATH):
            os.mkfifo(PIPE_PATH)

        while self.running:
            try:
                with open(PIPE_PATH, "r") as fifo:
                    while self.running:
                        line = fifo.readline().strip()
                        if not line:
                            time.sleep(0.1)
                            continue

                        try:
                            message = json.loads(line)
                            timestamp = message.get("timestamp", time.time())

                            # Legacy PGNname format
                            if "PGNname" in message and "value" in message:
                                name = message["PGNname"]
                                val = message["value"]
                                with canbus_lock:
                                    latest_canbus_data[name] = {
                                        "value": val,
                                        "timestamp": timestamp
                                    }

                            # Tolerant full-packet support
                            else:
                                for key, val in message.items():
                                    if key == "timestamp":
                                        continue
                                    if isinstance(val, (int, float, str)):
                                        with canbus_lock:
                                            latest_canbus_data[key] = {
                                                "value": val,
                                                "timestamp": timestamp
                                            }

                        except json.JSONDecodeError as e:
                            logging.warning(f"Invalid JSON from pipe: {e}")
                        except Exception as e:
                            logging.error(f"Failed to process pipe message: {e}")

            except Exception as e:
                logging.error(f"Error reading from CAN bus pipe: {e}")
                time.sleep(1)
