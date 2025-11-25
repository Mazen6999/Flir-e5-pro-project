import os
import csv
import json
import subprocess
import numpy as np
import contextlib
import sys
from flirimageextractor import FlirImageExtractor

# --- CONFIGURATION ---

# 1. FIX SYSTEM32 ISSUE: Get the folder where THIS .py file actually lives
if getattr(sys, 'frozen', False):
    # If this is compiled as an exe
    base_dir = os.path.dirname(sys.executable)
else:
    # If this is a script (.py)
    base_dir = os.path.dirname(os.path.abspath(__file__))

input_folder = os.path.join(base_dir, "flir ignite sync", "100_FLIR")
output_csv = os.path.join(base_dir, "Batch_Thermal_Report.csv")

# Relative path for the metadata extraction (local file)
exiftool_filename = "exiftool-12.35.exe"
exiftool_path = os.path.join(base_dir, exiftool_filename)

# --- HELPER FUNCTIONS ---

def get_metadata_with_exiftool(file_path):
    if not os.path.exists(exiftool_path):
        tool_cmd = "exiftool"
    else:
        tool_cmd = exiftool_path

    try:
        cmd = [
            tool_cmd,
            "-DateTimeOriginal",
            "-CameraModel",
            "-CameraSerialNumber",
            "-Emissivity",
            "-ObjectDistance",
            "-j",
            file_path
        ]
        
        creation_flags = 0
        if os.name == 'nt': 
            creation_flags = subprocess.CREATE_NO_WINDOW
            
        result = subprocess.run(cmd, capture_output=True, text=True, creationflags=creation_flags)
        
        if result.stdout:
            return json.loads(result.stdout)[0]
    except Exception:
        pass
    return {}

def get_processed_files(csv_path):
    processed = set()
    if not os.path.exists(csv_path):
        return processed
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row:
                    processed.add(row[0])
    except Exception:
        pass 
    return processed

def process_folder_incremental():
    headers = [
        "File Name", "Timestamp", "Camera Model", "Serial Number", 
        "Center Temp (°C)", "Max Temp (°C)", "Min Temp (°C)", 
        "Avg Temp (°C)", "Delta Temp (°C)", 
        "Emissivity", "Distance", "Status"
    ]
    
    # Validation Checks
    if not os.path.exists(input_folder):
        print(f"Error: Input folder not found at:\n{input_folder}")
        print(f"(Looking in base dir: {base_dir})")
        return

    # 1. IDENTIFY WORK
    all_files = [f for f in os.listdir(input_folder) if f.lower().endswith(".jpg")]
    processed_files = get_processed_files(output_csv)
    files_to_process = [f for f in all_files if f not in processed_files]
    
    print(f"Total Images: {len(all_files)}")
    print(f"Already Done: {len(processed_files)}")
    print(f"New to Process: {len(files_to_process)}")
    
    if not files_to_process:
        print("Nothing new to add. Exiting.")
        return

    # 2. INITIALIZE FLIR TOOL (SILENTLY)
    print("Initializing FLIR Extractor...", end="\r")
    try:
        with contextlib.redirect_stdout(None):
            flir = FlirImageExtractor()
    except Exception as e:
        print(f"\nError initializing FLIR extractor: {e}")
        return

    # 3. PREPARE CSV (Handle Permission Errors)
    file_exists = os.path.exists(output_csv)
    
    try:
        f = open(output_csv, mode='a', newline='', encoding='utf-8')
    except PermissionError:
        print(f"\n\nERROR: Permission Denied for '{output_csv}'")
        print(">>> PLEASE CLOSE THE CSV FILE IN EXCEL AND TRY AGAIN. <<<")
        return
    except Exception as e:
        print(f"\nError opening CSV: {e}")
        return

    # Use 'with' to ensure file closes even if script crashes
    with f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(headers)
        
        # 4. PROCESS LOOP
        for i, filename in enumerate(files_to_process):
            full_path = os.path.join(input_folder, filename)
            
            # Progress Bar
            print(f"Processing [{i+1}/{len(files_to_process)}]: {filename}" + " "*20, end="\r")
            
            try:
                # --- A. Thermal Data ---
                with contextlib.redirect_stdout(None):
                    flir.process_image(full_path)
                    
                thermal_data = flir.get_thermal_np()
                
                min_t = np.min(thermal_data)
                max_t = np.max(thermal_data)
                avg_t = np.mean(thermal_data)
                delta_t = max_t - min_t
                h, w = thermal_data.shape
                center_t = thermal_data[h//2, w//2]
                
                # --- B. Metadata ---
                meta = get_metadata_with_exiftool(full_path)
                
                serial = meta.get("CameraSerialNumber", "Unknown")
                timestamp = meta.get("DateTimeOriginal", "Unknown")
                camera_model = meta.get("CameraModel", "Unknown")
                emissivity = meta.get("Emissivity", "N/A")
                distance = meta.get("ObjectDistance", "N/A")
                
                # --- C. Write Row ---
                writer.writerow([
                    filename, timestamp, camera_model, serial, 
                    f"{center_t:.1f}", f"{max_t:.1f}", f"{min_t:.1f}", 
                    f"{avg_t:.1f}", f"{delta_t:.1f}", 
                    emissivity, distance, "Success"
                ])
                
                # Force save immediately so data isn't lost if script crashes
                f.flush()
                
            except Exception as e:
                writer.writerow([filename, "", "", "", "", "", "", "", "", "", "", f"Error: {str(e)}"])

    print(f"\n\nDone! Added {len(files_to_process)} rows to: {output_csv}")

if __name__ == "__main__":
    try:
        process_folder_incremental()
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
    
    input("Press Enter to exit...")