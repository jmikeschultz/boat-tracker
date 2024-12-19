This repo is about services to run on a boat raspberry pi, some code mostly notes.

1) boat_tracker.py, uses adafruit gps hat.  Expects mostly to not have internet, writes positions to local sqlite db, and when online syncronizes records to firebase db.  TODO: add engine rpm sensor column.  Another app will do stuff with that data.

2) expects a manual install of zwave-js-ui, pi has a zooz zwave fob. Talks mqtt to my home assistant.  For some reason I can't reach the zwave-js-ui from chrome but safari works.
  - a ring contact sensor for the hatch
  - TODO: tempurature sensor

3) uses duckdns to have a fixed domain on the internet, currently x46.duckdns.org

4) TODO: set up Mumble server so that even without internet, two iphones can connect and chat for anchoring.