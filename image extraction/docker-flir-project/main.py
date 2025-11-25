import os
import csv
import json
import subprocess
import numpy as np
import contextlib
import sys
from flirimageextractor import FlirImageExtractor

# --- CONFIGURATION FOR DOCKER ---
# In Docker, we will map your folder to '/app/images'
input_folder = "/app/images"
output_csv = "/app/images/Batch_Thermal_Report.csv"

# In Docker (Linux), we use the system installed exiftool
exiftool_path = "exiftool"

# --- HELPER FUNCTIONS ---

def get_metadata_with_exiftool(file_path):
    # In Docker, exiftool is on the system path, so we just call it directly
    try:
        cmd = [
            exiftool_path,
            "-DateTimeOriginal",
            "-CameraModel",
            "-CameraSerialNumber",
            "-Emissivity",
            "-ObjectDistance",
            "-j",
            file_path
        ]
        
        # No creation_flags needed for Linux/Docker
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.stdout:
            return json.loads(result.stdout)[0]
    except Exception as e:
        print(f"Metadata Error: {e}")
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
        "Center Temp (C)", "Max Temp (C)", "Min Temp (C)", 
        "Avg Temp (C)", "Delta Temp (C)", 
        "Emissivity", "Distance", "Status"
    ]
    
    if not os.path.exists(input_folder):
        print(f"Error: Input folder not found at: {input_folder}")
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

    # 2. INITIALIZE FLIR TOOL
    print("Initializing FLIR Extractor...")
    try:
        # We pass the system exiftool path explicitly to the library
        flir = FlirImageExtractor(exiftool_path=exiftool_path)
    except Exception as e:
        print(f"Error initializing FLIR extractor: {e}")
        return

    # 3. PREPARE CSV
    file_exists = os.path.exists(output_csv)
    
    try:
        with open(output_csv, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(headers)
            
            # 4. PROCESS LOOP
            for i, filename in enumerate(files_to_process):
                full_path = os.path.join(input_folder, filename)
                print(f"Processing [{i+1}/{len(files_to_process)}]: {filename}")
                
                try:
                    # --- A. Thermal Data ---
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
                    f.flush() # Ensure write
                    
                except Exception as e:
                    print(f"Error on {filename}: {e}")
                    writer.writerow([filename, "", "", "", "", "", "", "", "", "", "", f"Error: {str(e)}"])

        print(f"Done! Added {len(files_to_process)} rows.")

    except PermissionError:
        print(f"ERROR: Permission Denied for '{output_csv}'")
    except Exception as e:
        print(f"Error opening CSV: {e}")

if __name__ == "__main__":
    process_folder_incremental()