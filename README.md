# 🚤 Boat Tracker

## **Overview**
`boat_tracker.py` is designed to:
- Use an **Adafruit GPS HAT** to log position data.
- Run **offline most of the time**, writing GPS data to a **local SQLite database**.
- When **online**, it **syncs records to Firestore**.
- TODO: **Add an engine RPM sensor column** for tracking engine data.

---

## **1️⃣ Setting Up the Boat Tracker Service**
### **Step 1: Add the Systemd Service File**
Copy your service file to `/etc/systemd/system/`:
```sh
sudo cp boat-tracker.service /etc/systemd/system/
```

### **Step 2: Enable and Start the Service**
```sh
sudo systemctl daemon-reload
sudo systemctl enable boat-tracker.service
sudo systemctl start boat-tracker.service
```

### **Step 3: Check the Service Status**
```sh
sudo systemctl status boat-tracker.service
```
✅ If the service is running, you should see **"Active: running"** in the output.

---

## **2️⃣ Ensure Serial Port is Available for GPS**
By default, **Raspberry Pi reserves the UART serial port**, which blocks GPS communication.

### **Step 1: Disable Serial Login**
Run:
```sh
sudo raspi-config
```
Then:
1. Go to **Interfacing Options → Serial**.
2. **Disable login shell over serial** (`No`).
3. **Enable serial port hardware** (`Yes`).
4. Exit and **reboot** the system.

### **Step 2: Verify GPS Communication**
After reboot, test GPS data:
```sh
cgps -s
```
✅ If working, you should see **GPS coordinates updating** in real-time.

---

## **3️⃣ Firestore Authentication**
The Raspberry Pi **uses `gcloud auth`** to authenticate with **Google Cloud Firestore**.

### **Check Authentication Status**
```sh
gcloud auth list
```
✅ This will show the active Google Cloud account.

### **Revoke Authentication (If Needed)**
```sh
gcloud auth revoke
```
⚠️ **Revoking will break Firestore access!**

### **Alternative: Using a Service Account JSON Key**
Instead of `gcloud auth`, you can authenticate Firestore using a **service account key**.

1. **Download a service account JSON key from Google Cloud.**
2. Set the environment variable:
   ```sh
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
   ```
3. Verify Firestore connection:
   ```sh
   python3 -c "from google.cloud import firestore; db = firestore.Client(); print(db.project)"
   ```

### **Where `gcloud` Stores Auth Credentials**
```sh
ls ~/.config/gcloud
```
✅ This directory contains authentication data for Firestore and Google Cloud.

### **Check Active Project**
```sh
gcloud config list project
```
✅ This should return `boat-crumbs`, confirming the correct Firestore project.

---

## **🔥 Summary**
| **Feature** | **Command** | **Expected Output** |
|------------|------------|----------------------|
| **Start Boat Tracker** | `sudo systemctl start boat-tracker.service` | Service starts ✅ |
| **Check GPS Data** | `cgps -s` | GPS coordinates updating ✅ |
| **Check Firestore Authentication** | `gcloud auth list` | Active Google account ✅ |
| **Check Firestore Project** | `gcloud config list project` | Should be `boat-crumbs` ✅ |
