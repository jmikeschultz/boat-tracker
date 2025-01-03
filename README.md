1) boat_tracker.py, uses adafruit gps hat.  Expects mostly to not have internet, writes positions to local sqlite db, and when online syncronizes records to firebase db.  TODO: add engine rpm sensor column.  Another app will do stuff with that data.

2) Do this:
- add service file to /etc/systemd/system/
- sudo systemctl daemon-reload
- sudo systemctl enable boat-tracker.service
- sudo systemctl start boat-tracker.service
- sudo systemctl status boat-tracker.service



3) uses duckdns to have a fixed domain on the internet, currently x46.duckdns.org

4) TODO: set up Mumble server so that even without internet, two iphones can connect and chat for anchoring.