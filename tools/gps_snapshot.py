#!/usr/bin/env python
import socket
import json
import time

MAX_TRIES = 4  # Max seconds to attempt getting the best fix

def get_best_gps_fix(max_tries=MAX_TRIES):
    """
    Tries for up to `max_tries` seconds to get the best GPS fix with the most complete data.
    Returns the best available GPS fix.
    """
    best_fix = None
    start_time = time.time()

    try:
        # Connect to GPSD
        gpsd_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        gpsd_socket.connect(("localhost", 2947))
        gpsd_socket.sendall(b'?WATCH={"enable":true,"json":true}\n')

        while time.time() - start_time < max_tries:
            data = gpsd_socket.recv(1024)
            if not data:
                continue

            try:
                json_data = json.loads(data.decode("utf-8"))

                # Always capture the latest valid data, prioritize best fix
                if "mode" in json_data:
                    fix_mode = json_data["mode"]
                    if best_fix is None or fix_mode > best_fix["mode"]:  # Prioritize higher fix mode
                        best_fix = json_data

                # If we already have a 3D fix (best case), stop early
                if best_fix and best_fix["mode"] == 3:
                    break

            except json.JSONDecodeError:
                continue

    except Exception as e:
        print(f"Error connecting to GPSD: {e}")
        return None  # Indicate failure

    return best_fix  # Return the best fix found

def main():
    """
    Main function to get the best GPS fix and print the data.
    """
    best_fix = get_best_gps_fix()

    if not best_fix:
        print("No GPS data received!")
        return

    # Map fix mode to human-readable status
    fix_mode = best_fix.get("mode", 0)
    fix_status = {1: "No Fix", 2: "2D Fix", 3: "3D Fix"}.get(fix_mode, "Unknown Fix")

    print(f"Fix Status: {fix_status}")

    # Print GPS data if available
    if "lat" in best_fix and "lon" in best_fix:
        print(f"Latitude: {best_fix['lat']}, Longitude: {best_fix['lon']}")
    if "alt" in best_fix:
        print(f"Altitude: {best_fix['alt']} m")
    if "speed" in best_fix:
        print(f"Speed: {best_fix['speed']} knots")
    if "track" in best_fix:
        print(f"Heading: {best_fix['track']}°")
    if "time" in best_fix:
        print(f"Timestamp: {best_fix['time']}")

if __name__ == "__main__":
    main()
