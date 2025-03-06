1) boat_tracker.py, uses adafruit gps hat.  Expects mostly to not have internet, writes positions to local sqlite db, and when online syncronizes records to firebase db.  TODO: add engine rpm sensor column.  Another app will do stuff with that data.

2) Do this:
- add service file to /etc/systemd/system/
- sudo systemctl daemon-reload
- sudo systemctl enable boat-tracker.service
- sudo systemctl start boat-tracker.service
- sudo systemctl status boat-tracker.service

Make Sure the Serial Port Is Not Blocked
By default, Raspberry Pi reserves UART for console login, which blocks GPS communication.

sudo raspi-config
Go to: Interfacing Options → Serial
Disable the login shell over serial (No)
Enable serial port hardware (Yes)
Exit and reboot:

cgps -s
