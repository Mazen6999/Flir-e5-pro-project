################################### AUTO THERMAL PIPELINE (FINAL MASTER - DUPLICATE PROOF) ####################
import os
import shutil
import json
import subprocess
import time
import base64
import io
import re
import flyr
import pandas as pd
import logging
import threading
import atexit
import requests
from datetime import datetime
from logging.handlers import RotatingFileHandler
from sqlalchemy import create_engine
from sqlalchemy.types import String, DateTime, Integer, Float, Text
from urllib.parse import quote_plus
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv

# --- HEADLESS MODE (Prevents crashes on servers) ---
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# --- LOAD CONFIGURATION ---
load_dotenv()
INPUT_FOLDER = os.getenv("INPUT_FOLDER", "flir ignite sync")
ARCHIVE_FOLDER = os.getenv("ARCHIVE_FOLDER", "flir ignite sync/flir_processed_archive")
EXIFTOOL_PATH = os.getenv("EXIFTOOL_PATH", "flir ignite sync/exiftool-12.35.exe")
BATCH_SIZE = 50

# --- DATABASE CREDENTIALS ---
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_TABLE = "ThermalReadings"

# --- LOCATION CONFIG (Alexandria, Egypt) ---
ALEX_LAT = 31.2001
ALEX_LON = 29.9187

TRIGGER_EVENT = threading.Event()

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%d-%m-%Y %H:%M:%S',
    handlers=[
        RotatingFileHandler("history.log", maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ==============================================================================
# SECTION 1: HELPER FUNCTIONS & DB SETUP
# ==============================================================================

def validate_environment():
    """Checks that all tools and folders exist before starting."""
    if not os.path.exists(INPUT_FOLDER):
        logging.error(f"❌ Input folder missing: {INPUT_FOLDER}")
        return False

    if not os.path.exists(ARCHIVE_FOLDER):
        try:
            os.makedirs(ARCHIVE_FOLDER)
        except OSError:
            logging.error(f"❌ Could not create archive folder: {ARCHIVE_FOLDER}")
            return False

    if not shutil.which(EXIFTOOL_PATH) and not os.path.exists(EXIFTOOL_PATH):
        logging.error(f"❌ ExifTool not found at: {EXIFTOOL_PATH}")
        return False

    if not DB_PASS:
        logging.error("❌ DB_PASS missing from .env file.")
        return False

    return True


def init_db_engine():
    encoded_pass = quote_plus(DB_PASS)
    db_url = f"mssql+pyodbc://{DB_USER}:{encoded_pass}@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
    return create_engine(db_url, fast_executemany=True)


def get_existing_signatures(engine, start_date_str):
    """
    Checks DB for existing records to prevent duplicates.
    Signature: (Asset Name, Timestamp, Camera Serial)
    """
    try:
        query = f"SELECT Asset_Name, Timestamp, Camera_Serial FROM {DB_TABLE} WHERE Timestamp >= '{start_date_str}'"
        df = pd.read_sql(query, engine)

        if not df.empty:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='mixed')

            # Create a set of tuples: (Asset, Timestamp_String, Serial)
            signatures = set(zip(
                df['Asset_Name'],
                df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S'),
                df['Camera_Serial']
            ))
            return signatures
        return set()
    except Exception as e:
        logging.warning(f"⚠️ DB Warning: {e}")
        return set()


def get_metadata(folder):
    """
    Run exiftool recursively on the root folder and
    filter out anything inside ARCHIVE_FOLDER.
    """
    cmd = [
        EXIFTOOL_PATH,
        '-j', '-n', '-r',
        '-DateTimeOriginal',
        '-CameraSerialNumber',
        '-ImageDescription',
        '-Emissivity',
        '-ObjectDistance',
        '-ext', 'jpg',
        folder
    ]
    try:
        flags = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            creationflags=flags,
            timeout=15,
            stdin=subprocess.DEVNULL
        )

        if not result.stdout.strip():
            return []

        meta_list = json.loads(result.stdout)

        archive_abs = os.path.abspath(ARCHIVE_FOLDER)
        cleaned = []
        for m in meta_list:
            src = m.get("SourceFile")
            if not src:
                continue
            if os.path.abspath(src).startswith(archive_abs):
                continue
            cleaned.append(m)
        return cleaned

    except Exception as e:
        logging.error(f"Metadata scan failed: {e}")
        return []


def clean_asset_code(raw_input):
    if not raw_input:
        return None
    # Keep only Alphanumeric and uppercase
    clean = re.sub(r'[^A-Z0-9]', '', str(raw_input).upper())
    if not clean:
        return None
    return clean


# --- CONFIG ---
# (Your existing config here)
ALEX_LAT = 31.2001
ALEX_LON = 29.9187

def get_alexandria_weather(dt_obj):
    """
    Fetches the temperature in Alexandria for the specific hour (No Interpolation).
    """
    try:
        date_str = dt_obj.strftime("%Y-%m-%d")
        hour_idx = dt_obj.hour  # e.g., 11:43 -> 11
        
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": ALEX_LAT,
            "longitude": ALEX_LON,
            "hourly": "temperature_2m",
            "start_date": date_str,
            "end_date": date_str,
            "timezone": "auto"
        }
        
        response = requests.get(url, params=params, timeout=3)

        if response.status_code == 200:
            data = response.json()
            if "hourly" in data and "temperature_2m" in data["hourly"]:
                temps = data["hourly"]["temperature_2m"]
                
                # Directly grab the temp for this hour index
                if 0 <= hour_idx < len(temps):
                    return float(temps[hour_idx])
                    
        return None
    except Exception as e:
        logging.error(f"⚠️ Weather API Error: {e}")
        return None
    

def process_image(filepath, metadata_entry):
    filename = os.path.basename(filepath)
    raw_note = metadata_entry.get("ImageDescription")
    asset_str = clean_asset_code(raw_note)

    if not asset_str:
        return None

    try:
        serial_int = int(metadata_entry.get("CameraSerialNumber", 0))
        
        # Raw string: "2026:01:21 09:44:47.158+02:00"
        raw_ts = str(metadata_entry.get("DateTimeOriginal", ""))
        
        # --- SIMPLE PARSING (No Math, No Conversions) ---
        # We just take the first 19 characters: "2026:01:21 09:44:47"
        clean_ts = raw_ts[:19]
        
        try:
            # Create a datetime object for 9:44
            dt_obj = datetime.strptime(clean_ts, "%Y:%m:%d %H:%M:%S")
        except ValueError:
            try:
                dt_obj = datetime.strptime(clean_ts, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                logging.warning(f"⚠️ Date Parse Failed: {raw_ts}. Skipping.")
                return None

        # 1. Database String: "2026-01-21 09:44:47" (Exactly as seen)
        ts_str_db = dt_obj.strftime("%Y-%m-%d %H:%M:%S")

        # 2. Weather Check: Uses 9:44 (Correct)
        weather_temp = get_alexandria_weather(dt_obj)

        # --------------------------------

        thermogram = flyr.unpack(filepath)
        celsius = thermogram.celsius

        h, w = celsius.shape
        cy, cx = h // 2, w // 2

        row = {
            "Timestamp": ts_str_db,       # Sent to Dremio as 09:44
            "Filename": filename,
            "Camera_Serial": serial_int,
            "Asset_Name": asset_str,
            "Max_Temp_C": round(celsius.max(), 1),
            "Min_Temp_C": round(celsius.min(), 1),
            "Avg_Temp_C": round(celsius.mean(), 1),
            "Center_Temp_C": round(celsius[cy-1:cy+2, cx-1:cx+2].mean(), 1),
            "Delta_Temp_C": round(celsius.max() - celsius.min(), 1),
            "Emissivity": float(metadata_entry.get("Emissivity", 0.95)),
            "Distance": round(float(metadata_entry.get("ObjectDistance", 1.0)), 1),
            "weather_temp": weather_temp,
            "Image_Base64": ""
        }

        # Generate JPEG Buffer
        buffer = io.BytesIO()
        plt.imsave(buffer, celsius, cmap='inferno', format='jpeg')
        buffer.seek(0)
        row["Image_Base64"] = f"data:image/jpeg;base64,{base64.b64encode(buffer.getvalue()).decode('utf-8')}"
        buffer.close()

        return row
    except Exception as e:
        logging.error(f"Error processing {filename}: {e}")
        return None



# ==============================================================================
# SECTION 2: FILE OPERATIONS
# ==============================================================================

def is_file_locked(filepath):
    if not os.path.exists(filepath):
        return False
    try:
        with open(filepath, 'ab'):
            pass
        return False
    except IOError:
        return True


def iter_all_jpgs(root_folder):
    root_abs = os.path.abspath(root_folder)
    archive_abs = os.path.abspath(ARCHIVE_FOLDER)

    for dirpath, dirnames, filenames in os.walk(root_abs):
        if os.path.abspath(dirpath).startswith(archive_abs):
            continue
        for fname in filenames:
            if fname.lower().endswith(".jpg"):
                yield os.path.join(dirpath, fname)


def wait_for_folder_stability(root_folder, timeout=15):
    start_time = time.time()
    root_abs = os.path.abspath(root_folder)
    archive_abs = os.path.abspath(ARCHIVE_FOLDER)

    while (time.time() - start_time) < timeout:
        locked_files = []
        for dirpath, dirnames, filenames in os.walk(root_abs):
            if os.path.abspath(dirpath).startswith(archive_abs):
                continue
            for fname in filenames:
                if not fname.lower().endswith(".jpg"):
                    continue
                full_path = os.path.join(dirpath, fname)
                try:
                    if time.time() - os.path.getmtime(full_path) > 60:
                        continue
                except OSError:
                    continue
                if is_file_locked(full_path):
                    locked_files.append(full_path)

        if not locked_files:
            return True

        logging.info("⏳ Waiting for locks: %s...", [os.path.basename(p) for p in locked_files[:3]])
        time.sleep(1)

    return False


def move_to_archive(filepath):
    filename = os.path.basename(filepath)
    src = filepath
    dst = os.path.join(ARCHIVE_FOLDER, filename)
    try:
        if os.path.exists(dst):
            base, ext = os.path.splitext(filename)
            dst = os.path.join(ARCHIVE_FOLDER, f"{base}_{int(time.time())}{ext}")
        shutil.move(src, dst)
        return True
    except Exception as e:
        logging.error(f"❌ Archive Error: {e}")
        return False


# ==============================================================================
# SECTION 3: PIPELINE LOGIC (WITH DUPLICATE FIX)
# ==============================================================================

def run_pipeline(db_engine):
    logging.info("🔄 Starting Pipeline Run...")

    meta_list = get_metadata(INPUT_FOLDER)
    if not meta_list:
        logging.info("   ℹ️ No readable images found.")
        return

    # 1. Map Metadata
    meta_dict = {}
    timestamps = []
    for m in meta_list:
        if 'SourceFile' in m:
            src = m['SourceFile']
            full_src = os.path.abspath(src)
            meta_dict[full_src] = m
            if 'DateTimeOriginal' in m:
                timestamps.append(str(m['DateTimeOriginal']).replace(":", "-", 2))

    if not timestamps:
        return

    # 2. Check Database for Duplicates
    oldest = min(timestamps)
    query_start = oldest[:10] + " 00:00:00"

    try:
        existing = get_existing_signatures(db_engine, query_start)
    except Exception as e:
        logging.error(f"❌ DB Query Failed: {e}")
        return

    # 3. Collect files (Checking against DB AND Current Batch)
    files_to_process = []

    for fpath in iter_all_jpgs(INPUT_FOLDER):
        full_path = os.path.abspath(fpath)
        m_data = meta_dict.get(full_path, {})

        raw_asset = m_data.get("ImageDescription", "")
        asset = clean_asset_code(raw_asset)

        ts = str(m_data.get("DateTimeOriginal", "")).replace(":", "-", 2)[:19]

        try:
            serial = int(m_data.get("CameraSerialNumber", 0))
        except Exception:
            serial = 0

        # Define the unique signature for this image
        current_sig = (asset, ts, serial)

        # CHECK SIGNATURE: Is it in DB? OR Is it in current processing batch?
        if asset and ts and current_sig in existing:
            logging.info(f"   ⚠️ Duplicate: {os.path.basename(fpath)} -> Archiving...")
            move_to_archive(full_path)
        else:
            files_to_process.append(full_path)
            
            # --- CRITICAL FIX: Add to 'existing' IMMEDIATELY ---
            # This prevents the loop from accepting a copy of this same file 
            # (e.g. "FLIR0030 (1).jpg") later in the same batch.
            if asset and ts:
                existing.add(current_sig)

    if not files_to_process:
        logging.info("✅ No new data to upload.")
        return

    logging.info(f"🚀 Processing {len(files_to_process)} NEW images...")

    # 4. Process, Upload, and Archive
    total_uploaded = 0

    sql_types = {
        "Timestamp": DateTime(),
        "Filename": String(255),
        "Camera_Serial": Integer(),
        "Asset_Name": String(255),
        "Max_Temp_C": Float(),
        "Min_Temp_C": Float(),
        "Center_Temp_C": Float(),
        "Avg_Temp_C": Float(),
        "Delta_Temp_C": Float(),
        "Emissivity": Float(),
        "Distance": Float(),
        "weather_temp": Float(),
        "Image_Base64": Text()
    }

    for i in range(0, len(files_to_process), BATCH_SIZE):
        chunk = files_to_process[i: i + BATCH_SIZE]
        new_rows = []
        uploaded_paths = []

        for fpath in chunk:
            m_data = meta_dict.get(os.path.abspath(fpath), {})
            row = process_image(fpath, m_data)

            if row:
                new_rows.append(row)
                uploaded_paths.append(fpath)
            else:
                logging.warning(f"⚠️ No Asset Note found for {os.path.basename(fpath)}. Archiving without upload.")
                move_to_archive(fpath)

        if new_rows:
            df = pd.DataFrame(new_rows)
            cols = [
                "Timestamp", "Filename", "Camera_Serial", "Asset_Name",
                "Max_Temp_C", "Min_Temp_C", "Center_Temp_C", "Avg_Temp_C",
                "Delta_Temp_C", "Emissivity", "Distance", "weather_temp", "Image_Base64"
            ]
            df = df[cols]
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='mixed').dt.tz_localize(None)

            try:
                df.to_sql(DB_TABLE, db_engine, if_exists='append', index=False, dtype=sql_types)
                total_uploaded += len(df)
                for fpath in uploaded_paths:
                    move_to_archive(fpath)
            except Exception as e:
                logging.error(f"❌ Chunk Upload Failed: {e}")

    if total_uploaded > 0:
        logging.info(f"🎉 SUCCESS: Uploaded {total_uploaded} records & Archived.")


# ==============================================================================
# SECTION 4: EVENT LISTENERS & MAIN LOOP
# ==============================================================================

class FileTrigger(FileSystemEventHandler):
    def _trigger(self, path):
        if not path.lower().endswith(".jpg"):
            return
        if os.path.abspath(path).startswith(os.path.abspath(ARCHIVE_FOLDER)):
            return
        print("-------------------------------------------------------")
        logging.info("⏳ New file detected.")
        TRIGGER_EVENT.set()

    def on_created(self, event):
        if not event.is_directory:
            self._trigger(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._trigger(event.dest_path)


def keyboard_listener():
    while True:
        try:
            input()
            print("-------------------------------------------------------")
            logging.info("⌨️ Manual trigger detected.")
            TRIGGER_EVENT.set()
        except EOFError:
            break


def shutdown_handler(db_engine):
    logging.info("🛑 Shutting down gracefully...")
    try:
        db_engine.dispose()
    except Exception:
        pass
    logging.info("👋 Goodbye.")


if __name__ == "__main__":
    print("-------------------------------------------------------")
    logging.info("🛠️  Performing Startup Health Check...")

    if not validate_environment():
        print("❌ Startup Failed. Check logs/config.")
        exit()

    logging.info(f"✅ WATCHING ROOT: '{INPUT_FOLDER}'")
    print("-------------------------------------------------------")

    try:
        logging.info(f"🔌 Connecting to Database: {DB_SERVER}")
        db_engine = init_db_engine()
        atexit.register(shutdown_handler, db_engine)

        with db_engine.connect() as conn:
            logging.info(f"✅ Database Connected to Table: {DB_NAME}.{DB_TABLE}")
    except Exception as e:
        logging.error(f"❌ CRITICAL DB ERROR: {e}")
        exit()

    run_pipeline(db_engine)

    observer = Observer()
    observer.schedule(FileTrigger(), INPUT_FOLDER, recursive=True)
    observer.start()

    kb_thread = threading.Thread(target=keyboard_listener, daemon=True)
    kb_thread.start()

    try:
        while True:
            if TRIGGER_EVENT.wait(timeout=1):
                time.sleep(1)
                TRIGGER_EVENT.clear()
                if wait_for_folder_stability(INPUT_FOLDER):
                    run_pipeline(db_engine)
                else:
                    logging.error("❌ Folder locked. Skipping.")
    except KeyboardInterrupt:
        observer.stop()
    observer.join()