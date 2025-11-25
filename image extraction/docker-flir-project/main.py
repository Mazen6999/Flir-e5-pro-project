import os
import csv
import json
import subprocess
import numpy as np
from flirimageextractor import FlirImageExtractor

# --- DOCKER CONFIGURATION ---
# We map the windows folder to '/app/images' inside the container
input_folder = "/app/images"
output_csv = "/app/images/Batch_Thermal_Report.csv"

# In Docker (Linux), exiftool is installed in /usr/bin/
exiftool_path = "/usr/bin/exiftool"

# --- HELPER FUNCTIONS ---

def get_metadata(file_path):
    """Extracts text data using ExifTool"""
    # Check if exiftool exists (Linux path check)
    if not os.path.exists(exiftool_path):
        print(f"Warning: Exiftool not found at {exiftool_path}")
        return {}
    
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
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if not result.stdout: return {}
        return json.loads(result.stdout)[0]
    except Exception as e:
        print(f"Metadata Error: {e}")
        return {}

def get_processed_files(csv_path):
    """Reads the CSV to find files that are already done."""
    processed = set()
    if not os.path.exists(csv_path):
        return processed
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None) # Skip header
            for row in reader:
                if row:
                    processed.add(row[0]) 
    except Exception:
        pass 
    return processed

def process_folder_incremental():
    headers = [
        "File Name", "Timestamp","Camera Model" ,"Serial Number", 
        "Center Temp (C)", "Max Temp (C)", "Min Temp (C)", 
        "Avg Temp (C)", "Delta Temp (C)", 
        "Emissivity", "Distance", "Status"
    ]
    
    if not os.path.exists(input_folder):
        print(f"Error: Folder not found: {input_folder}")
        return

    # 1. IDENTIFY WORK TO BE DONE
    all_files = [f for f in os.listdir(input_folder) if f.lower().endswith(".jpg")]
    processed_files = get_processed_files(output_csv)
    
    files_to_process = [f for f in all_files if f not in processed_files]
    
    print(f"Total Images: {len(all_files)}")
    print(f"Already Done: {len(processed_files)}")
    print(f"New to Process: {len(files_to_process)}")
    
    if not files_to_process:
        print("Nothing new to add. Exiting.")
        return

    # 2. PREPARE CSV
    file_exists = os.path.exists(output_csv)
    
    with open(output_csv, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(headers)
        
        # 3. PROCESS LOOP
        for i, filename in enumerate(files_to_process):
            full_path = os.path.join(input_folder, filename)
            print(f"Processing [{i+1}/{len(files_to_process)}]: {filename}")
            
            try:
                # --- A. Thermal Data ---
                flir = FlirImageExtractor(exiftool_path=exiftool_path)
                flir.process_image(full_path)
                thermal_data = flir.get_thermal_np()
                
                min_t = np.min(thermal_data)
                max_t = np.max(thermal_data)
                avg_t = np.mean(thermal_data)
                delta_t = max_t - min_t
                
                h, w = thermal_data.shape
                center_t = thermal_data[h//2, w//2]
                
                # --- B. Metadata ---
                meta = get_metadata(full_path)
                timestamp = meta.get("DateTimeOriginal", "Unknown")
                camera_model = meta.get("CameraModel", "Unknown")
                serial = meta.get("CameraSerialNumber", "Unknown")
                emissivity = meta.get("Emissivity", "N/A")
                distance = meta.get("ObjectDistance", "N/A")
                
                # --- C. Write Row ---
                writer.writerow([
                    filename, timestamp, camera_model ,serial, 
                    f"{center_t:.1f}", f"{max_t:.1f}", f"{min_t:.1f}", 
                    f"{avg_t:.1f}", f"{delta_t:.1f}", 
                    emissivity, distance, "Success"
                ])
                
            except Exception as e:
                print(f"Error on {filename}: {e}")
                writer.writerow([filename,'' ,"", "", "", "", "", "", "", "", "", f"Error: {str(e)}"])

    print(f"\nDone! Added {len(files_to_process)} rows.")

if __name__ == "__main__":
    process_folder_incremental()