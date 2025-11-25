import os
import csv
import json
import subprocess
import numpy as np
from flirimageextractor import FlirImageExtractor

# --- CONFIGURATION ---
input_folder = r"C:\flir e5 pro\image extraction\flir ignite sync\100_FLIR"
output_csv = "Batch_Thermal_Report.csv"

# Your specific ExifTool path
exiftool_path = r"c:\Users\st1ahmedma\AppData\Local\Programs\Python\Python311\Lib\site-packages\dji_executables\dji_thermal_sdk_v1.7\exiftool-12.35.exe"

# --- HELPER FUNCTIONS ---

def get_metadata(file_path):
    """Extracts text data using ExifTool"""
    if not os.path.exists(exiftool_path):
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
    except Exception:
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
                    processed.add(row[0]) # Column 0 is File Name
    except Exception:
        pass # If file is corrupt or empty, we just start fresh
    return processed

def process_folder_incremental():
    headers = [
        "File Name", "Timestamp","Camera Model" ,"Serial Number", 
        "Center Temp (°C)", "Max Temp (°C)", "Min Temp (°C)", 
        "Avg Temp (°C)", "Delta Temp (°C)", 
        "Emissivity", "Distance", "Status"
    ]
    
    if not os.path.exists(input_folder):
        print(f"Error: Folder not found: {input_folder}")
        return

    # 1. IDENTIFY WORK TO BE DONE
    all_files = [f for f in os.listdir(input_folder) if f.lower().endswith(".jpg")]
    processed_files = get_processed_files(output_csv)
    
    # Calculate list difference
    files_to_process = [f for f in all_files if f not in processed_files]
    
    print(f"Total Images: {len(all_files)}")
    print(f"Already Done: {len(processed_files)}")
    print(f"New to Process: {len(files_to_process)}")
    
    if not files_to_process:
        print("Nothing new to add. Exiting.")
        return

    # 2. PREPARE CSV (Create if new, Append if exists)
    file_exists = os.path.exists(output_csv)
    
    # Open in 'a' (Append) mode
    with open(output_csv, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Only write headers if the file didn't exist before
        if not file_exists:
            writer.writerow(headers)
        
        # 3. PROCESS LOOP
        for i, filename in enumerate(files_to_process):
            full_path = os.path.join(input_folder, filename)
            print(f"Processing [{i+1}/{len(files_to_process)}]: {filename}", end="\r")
            
            try:
                # --- A. Thermal Data ---
                flir = FlirImageExtractor()
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
                serial = (
                    meta.get("CameraSerialNumber") or 
                    meta.get("SerialNumber") or 
                    meta.get("DeviceSerial") or 
                    "Unknown"
                )
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
                writer.writerow([filename,'' ,"", "", "", "", "", "", "", "", "", f"Error: {str(e)}"])

    print(f"\n\nDone! Added {len(files_to_process)} new rows to: {output_csv}")

if __name__ == "__main__":
    process_folder_incremental()
    input("Press Enter to exit...")