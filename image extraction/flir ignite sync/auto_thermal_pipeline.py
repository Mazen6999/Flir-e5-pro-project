################################### AUTO THERMAL PIPELINE (FINAL MASTER) #################### 
import os
import shutil
import json
import subprocess
import time
import base64
import io
import flyr
import pandas as pd
import logging
import threading
import atexit
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
INPUT_FOLDER   = os.getenv("INPUT_FOLDER", "flir e5 photodump")
ARCHIVE_FOLDER = os.getenv("ARCHIVE_FOLDER", "flir_processed")
EXIFTOOL_PATH  = os.getenv("EXIFTOOL_PATH", "exiftool-12.35.exe")
BATCH_SIZE     = 50 

# --- DATABASE CREDENTIALS ---
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME   = os.getenv("DB_NAME")
DB_USER   = os.getenv("DB_USER")
DB_PASS   = os.getenv("DB_PASS")
DB_TABLE  = "ThermalReadings"

TRIGGER_EVENT = threading.Event()

# --- LOGGING SETUP (Rotates logs to save space) ---
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
    # Runs Exiftool with stdin=DEVNULL to prevent conflict with input()
    cmd = [EXIFTOOL_PATH, '-j', '-n', '-r', '-DateTimeOriginal', '-CameraSerialNumber', 
           '-ImageDescription', '-Emissivity', '-ObjectDistance', '-ext', 'jpg', folder]
    try:
        flags = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        result = subprocess.run(cmd, capture_output=True, text=True, creationflags=flags, timeout=15, stdin=subprocess.DEVNULL)
        return json.loads(result.stdout)
    except Exception as e:
        logging.error(f"Metadata scan failed: {e}")
        return []

def process_image(filepath, metadata_entry):
    filename = os.path.basename(filepath)
    asset_name = metadata_entry.get("ImageDescription")
    asset_str = str(asset_name).strip() if asset_name else ""
    if not asset_str: return None

    try:
        serial_int = int(metadata_entry.get("CameraSerialNumber", 0))
        ts_str = str(metadata_entry["DateTimeOriginal"]).replace(":", "-", 2)
        thermogram = flyr.unpack(filepath)
        celsius = thermogram.celsius
        
        # Calculate Stats
        h, w = celsius.shape
        cy, cx = h // 2, w // 2
        
        row = {
            "Timestamp": ts_str, 
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
# SECTION 2: FILE OPERATIONS (LOCKS & ARCHIVING)
# ==============================================================================

def is_file_locked(filepath):
    if not os.path.exists(filepath): return False
    try:
        with open(filepath, 'ab'): pass
        return False
    except IOError: return True

def wait_for_folder_stability(folder, timeout=15):
    """
    Checks if files are ready. 
    OPTIMIZATION: Ignores files older than 60s to speed up large folder scans.
    """
    start_time = time.time()
    while (time.time() - start_time) < timeout:
        locked_files = []
        try:
            files = [f for f in os.listdir(folder) if f.lower().endswith(".jpg")]
        except: return False 
        
        if not files: return True

        for f in files:
            full_path = os.path.join(folder, f)
            try:
                # If file is old, it's stable. Skip lock check.
                if time.time() - os.path.getmtime(full_path) > 60: continue 
            except OSError: continue 
            
            if is_file_locked(full_path): locked_files.append(f)
        
        if not locked_files: return True 
        logging.info(f"⏳ Waiting for locks: {locked_files[:3]}...")
        time.sleep(1)
    return False

def move_to_archive(filename):
    """Moves processed files to keep INPUT_FOLDER clean and fast."""
    src = os.path.join(INPUT_FOLDER, filename)
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
# SECTION 3: PIPELINE LOGIC
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
            fname = os.path.basename(m['SourceFile'])
            meta_dict[fname] = m
            if 'DateTimeOriginal' in m:
                timestamps.append(str(m['DateTimeOriginal']).replace(":", "-", 2))
    
    if not timestamps: return 

    # 2. Check Database for Duplicates (Time + Asset + Serial)
    oldest = min(timestamps)
    query_start = oldest[:10] + " 00:00:00"
    
    try:
        existing = get_existing_signatures(db_engine, query_start)
    except Exception as e:
        logging.error(f"❌ DB Query Failed: {e}")
        return

    # 3. Filter Files
    files = [f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith(".jpg")]
    files_to_process = []
    
    for f in files:
        m_data = meta_dict.get(f, {})
        
        asset = str(m_data.get("ImageDescription", "")).strip()
        ts = str(m_data.get("DateTimeOriginal", "")).replace(":", "-", 2)[:19]
        
        # Safe extraction of Serial
        try:
            serial = int(m_data.get("CameraSerialNumber", 0))
        except:
            serial = 0
        
        # CHECK SIGNATURE: (Asset, Time, Serial)
        if asset and ts and (asset, ts, serial) in existing:
            logging.info(f"   ⚠️ Duplicate: {f} -> Archiving...")
            move_to_archive(f)
        else:
            files_to_process.append(f)

    if not files_to_process:
        logging.info("✅ No new data to upload.")
        return

    logging.info(f"🚀 Processing {len(files_to_process)} NEW images...")

    # 4. Process, Upload, and Archive
    total_uploaded = 0
    
    # Define SQL Types explicitly to prevent "Right Truncation" errors
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
        "Image_Base64": Text() # Maps to VARCHAR(MAX)
    }

    for i in range(0, len(files_to_process), BATCH_SIZE):
        chunk = files_to_process[i : i + BATCH_SIZE]
        new_rows = []
        uploaded_filenames = []
        
        for f in chunk:
            row = process_image(os.path.join(INPUT_FOLDER, f), meta_dict.get(f, {}))
            if row: 
                new_rows.append(row)
                uploaded_filenames.append(f)
            
        if new_rows:
            df = pd.DataFrame(new_rows)
            # Ensure Cols
            cols = ["Timestamp", "Filename", "Camera_Serial", "Asset_Name", 
                    "Max_Temp_C", "Min_Temp_C", "Center_Temp_C", "Avg_Temp_C", 
                    "Delta_Temp_C", "Emissivity", "Distance", "Image_Base64"]
            df = df[cols]
            
            # Clean Timestamp (Remove Timezone for SQL)
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='mixed').dt.tz_localize(None)
            
            try:
                df.to_sql(DB_TABLE, db_engine, if_exists='append', index=False, dtype=sql_types)
                
                total_uploaded += len(df)
                # Only archive if DB upload was successful
                for f in uploaded_filenames: move_to_archive(f)
            except Exception as e:
                logging.error(f"❌ Chunk Upload Failed: {e}")

    if total_uploaded > 0:
        logging.info(f"🎉 SUCCESS: Uploaded {total_uploaded} records & Archived.")

# ==============================================================================
# SECTION 4: EVENT LISTENERS & MAIN LOOP
# ==============================================================================

class FileTrigger(FileSystemEventHandler):
    def _trigger(self, path):
        if path.lower().endswith(".jpg"):
            print("-------------------------------------------------------")
            logging.info("⏳ New file detected.")
            TRIGGER_EVENT.set() 
    def on_created(self, event):
        if not event.is_directory: self._trigger(event.src_path)
    def on_moved(self, event):
        if not event.is_directory: self._trigger(event.dest_path)

def keyboard_listener():
    while True:
        try:
            input() 
            print("-------------------------------------------------------")
            logging.info("⌨️ Manual trigger detected.")
            TRIGGER_EVENT.set()
        except EOFError: break 

def shutdown_handler(db_engine):
    logging.info("🛑 Shutting down gracefully...")
    try: db_engine.dispose()
    except: pass
    logging.info("👋 Goodbye.")

if __name__ == "__main__":
    print("-------------------------------------------------------")
    logging.info("🛠️  Performing Startup Health Check...")
    
    if not validate_environment():
        print("❌ Startup Failed. Check logs/config.")
        exit()
        
    logging.info(f"✅ WATCHING: '{INPUT_FOLDER}'")
    print("-------------------------------------------------------")

    try:
        logging.info("🔌 Connecting to Database...")
        db_engine = init_db_engine()
        # Register cleanup to run when script exits
        atexit.register(shutdown_handler, db_engine)
        
        with db_engine.connect() as conn:
            logging.info("✅ Database Connection Established!")
    except Exception as e:
        logging.error(f"❌ CRITICAL DB ERROR: {e}")
        exit()

    # Initial run to clear backlog
    run_pipeline(db_engine)

    observer = Observer()
    observer.schedule(FileTrigger(), INPUT_FOLDER, recursive=False)
    observer.start()

    kb_thread = threading.Thread(target=keyboard_listener, daemon=True)
    kb_thread.start()

    try:
        while True:
            # Wait for file drop or manual trigger
            if TRIGGER_EVENT.wait(timeout=1): 
                # Debounce: wait for batch copy to finish
                time.sleep(1) 
                TRIGGER_EVENT.clear()
                
                if wait_for_folder_stability(INPUT_FOLDER):
                    run_pipeline(db_engine)
                else:
                    logging.error("❌ Folder locked. Skipping.")
            
    except KeyboardInterrupt:
        observer.stop()
    
    observer.join()