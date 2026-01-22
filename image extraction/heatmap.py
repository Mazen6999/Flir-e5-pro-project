################################### AUTO THERMAL PIPELINE (SMART SVG EDITION) #################### 
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
from logging.handlers import RotatingFileHandler
from sqlalchemy import create_engine
from sqlalchemy.types import String, DateTime, Integer, Float, Text 
from urllib.parse import quote_plus
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv 

# --- NEW IMPORTS FOR SVG GENERATION ---
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as colors

# --- LOAD CONFIGURATION ---
load_dotenv()
INPUT_FOLDER = os.getenv("INPUT_FOLDER", "flir e5 photodump")
ARCHIVE_FOLDER = os.getenv("ARCHIVE_FOLDER", "flir_processed")
EXIFTOOL_PATH = os.getenv("EXIFTOOL_PATH", "exiftool-12.35.exe")
BATCH_SIZE = 50 

# --- DATABASE CREDENTIALS ---
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_TABLE = "ThermalReadings"

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
# SECTION 1: HELPER FUNCTIONS
# ==============================================================================

def validate_environment():
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
    try:
        query = f"SELECT Asset_Name, Timestamp, Camera_Serial FROM {DB_TABLE} WHERE Timestamp >= '{start_date_str}'"
        df = pd.read_sql(query, engine)
        if not df.empty:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='mixed')
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
    cmd = [EXIFTOOL_PATH, '-j', '-n', '-r', '-DateTimeOriginal', '-CameraSerialNumber', 
           '-ImageDescription', '-Emissivity', '-ObjectDistance', '-ext', 'jpg', folder]
    try:
        flags = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        result = subprocess.run(cmd, capture_output=True, text=True, creationflags=flags, timeout=15, stdin=subprocess.DEVNULL)
        return json.loads(result.stdout)
    except Exception as e:
        logging.error(f"Metadata scan failed: {e}")
        return []

def clean_asset_code(raw_input):
    if not raw_input: return None
    clean = re.sub(r'[^A-Z0-9]', '', str(raw_input).upper())
    return clean if clean else None

# --- NEW FUNCTION: GENERATE SMART SVG ---
# --- NEW FUNCTION: GENERATE SMART SVG (FIXED) ---
def generate_interactive_svg(celsius_matrix):
    """
    Converts the 2D temperature matrix into an SVG string with tooltips.
    """
    height, width = celsius_matrix.shape
    
    # 1. Setup Colors (Inferno is great for thermal)
    norm = colors.Normalize(vmin=celsius_matrix.min(), vmax=celsius_matrix.max())
    
    # --- FIX: Use new Matplotlib syntax ---
    # Old way: cmap = cm.get_cmap('inferno')
    cmap = matplotlib.colormaps['inferno'] 
    
    # 2. Start SVG String
    svg_parts = [f'<svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" shape-rendering="crispEdges" style="width:100%; height:100%;">']
    
    # 3. Build the Grid
    step = 8 
    
    for y in range(0, height, step):
        for x in range(0, width, step):
            temp = celsius_matrix[y, x]
            
            # Get Color
            rgba = cmap(norm(temp))
            hex_color = colors.to_hex(rgba)
            
            rect = f'<rect x="{x}" y="{y}" width="{step}" height="{step}" fill="{hex_color}"><title>{temp:.1f}°C</title></rect>'
            svg_parts.append(rect)
            
    svg_parts.append('</svg>')
    return "".join(svg_parts)
def process_image(filepath, metadata_entry):
    filename = os.path.basename(filepath)
    
    raw_note = metadata_entry.get("ImageDescription")
    asset_str = clean_asset_code(raw_note)
    
    if not asset_str: 
        return None

    try:
        serial_int = int(metadata_entry.get("CameraSerialNumber", 0))
        ts_str = str(metadata_entry["DateTimeOriginal"]).replace(":", "-", 2)
        thermogram = flyr.unpack(filepath)
        celsius = thermogram.celsius
        
        # --- GENERATE SMART SVG ---
        smart_svg_string = generate_interactive_svg(celsius)
        # --------------------------

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
            
            # WE SAVE THE SVG INTO THE 'Image_Base64' COLUMN
            # This avoids adding new columns. It just works.
            "Image_Base64": smart_svg_string 
        }
        
        return row
    except Exception as e:
        logging.error(f"Error processing {filename}: {e}")
        return None 

# ==============================================================================
# SECTION 2 & 3 & 4 (REMAIN UNCHANGED)
# ==============================================================================
# (Copy the rest of your previous script here: is_file_locked, run_pipeline, main loop, etc.)
# I will strictly omit them to save space, as they are identical to the 'Final Master' script provided earlier.
# Just make sure to include the helper functions and the __main__ block at the bottom.

def is_file_locked(filepath):
    if not os.path.exists(filepath): return False
    try:
        with open(filepath, 'ab'): pass
        return False
    except IOError: return True

def wait_for_folder_stability(folder, timeout=15):
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
                if time.time() - os.path.getmtime(full_path) > 60: continue 
            except OSError: continue 
            if is_file_locked(full_path): locked_files.append(f)
        if not locked_files: return True 
        logging.info(f"⏳ Waiting for locks: {locked_files[:3]}...")
        time.sleep(1)
    return False

def move_to_archive(filename):
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

def run_pipeline(db_engine):
    logging.info("🔄 Starting Pipeline Run...")
    meta_list = get_metadata(INPUT_FOLDER)
    if not meta_list:
        logging.info("   ℹ️ No readable images found.")
        return
    meta_dict = {}
    timestamps = []
    for m in meta_list:
        if 'SourceFile' in m:
            fname = os.path.basename(m['SourceFile'])
            meta_dict[fname] = m
            if 'DateTimeOriginal' in m:
                timestamps.append(str(m['DateTimeOriginal']).replace(":", "-", 2))
    if not timestamps: return 
    oldest = min(timestamps)
    query_start = oldest[:10] + " 00:00:00"
    try:
        existing = get_existing_signatures(db_engine, query_start)
    except Exception as e:
        logging.error(f"❌ DB Query Failed: {e}")
        return
    files = [f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith(".jpg")]
    files_to_process = []
    for f in files:
        m_data = meta_dict.get(f, {})
        raw_asset = m_data.get("ImageDescription", "")
        asset = clean_asset_code(raw_asset)
        ts = str(m_data.get("DateTimeOriginal", "")).replace(":", "-", 2)[:19]
        try: serial = int(m_data.get("CameraSerialNumber", 0))
        except: serial = 0
        if asset and ts and (asset, ts, serial) in existing:
            logging.info(f"   ⚠️ Duplicate: {f} -> Archiving...")
            move_to_archive(f)
        else:
            files_to_process.append(f)
    if not files_to_process:
        logging.info("✅ No new data to upload.")
        return
    logging.info(f"🚀 Processing {len(files_to_process)} NEW images...")
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
        "Image_Base64": Text() 
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
            cols = ["Timestamp", "Filename", "Camera_Serial", "Asset_Name", 
                    "Max_Temp_C", "Min_Temp_C", "Center_Temp_C", "Avg_Temp_C", 
                    "Delta_Temp_C", "Emissivity", "Distance", "Image_Base64"]
            df = df[cols]
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='mixed').dt.tz_localize(None)
            try:
                df.to_sql(DB_TABLE, db_engine, if_exists='append', index=False, dtype=sql_types)
                total_uploaded += len(df)
                for f in uploaded_filenames: move_to_archive(f)
            except Exception as e:
                logging.error(f"❌ Chunk Upload Failed: {e}")
    if total_uploaded > 0:
        logging.info(f"🎉 SUCCESS: Uploaded {total_uploaded} records & Archived.")

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
    observer.schedule(FileTrigger(), INPUT_FOLDER, recursive=False)
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