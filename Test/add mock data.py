import random
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import io
import base64
import matplotlib.pyplot as plt
import numpy as np
import calendar

# --- CONFIGURATION ---
DB_SERVER = "PSQLAPPEG297-01"
DB_NAME = "Flir"
DB_USER = "Flir"
DB_PASS = "Prom@2025"
DB_TABLE = "ThermalReadings"

# Year to generate data for
YEAR = 2025

# --- ASSET DEFINITIONS ---
# Using the EXACT codes you provided
ASSETS = [
    {"code": "TestCodeBuilding-", "name": "TestName-build",  "trend": "stable",         "base_temp": 35},
    {"code": "TestCodeCuring-",   "name": "TestName-curing", "trend": "critical_spike", "base_temp": 50},
    {"code": "TestCodeMixer-",    "name": "TestName-mixer",  "trend": "steady_warming", "base_temp": 40},
    {"code": "TestCodeSemi-",     "name": "TestName-semi",   "trend": "steady_cooling", "base_temp": 80},
]

def get_db_engine():
    encoded_pass = quote_plus(DB_PASS)
    db_url = f"mssql+pyodbc://{DB_USER}:{encoded_pass}@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
    return create_engine(db_url, fast_executemany=True)

def generate_dummy_image(temp_val):
    """Generates a tiny heat map image string."""
    data = np.random.rand(10, 10) * temp_val 
    buffer = io.BytesIO()
    plt.imsave(buffer, data, cmap='inferno', format='jpeg')
    buffer.seek(0)
    return f"data:image/jpeg;base64,{base64.b64encode(buffer.getvalue()).decode('utf-8')}"

def calculate_temp(asset, date_obj):
    """Calculates specific temperature based on the asset's assigned trend."""
    day_of_year = date_obj.timetuple().tm_yday
    season_factor = 1 + (0.15 * np.sin((day_of_year - 100) / 365 * 2 * np.pi))
    
    base = asset['base_temp']
    trend = asset['trend']
    noise = random.uniform(-1.0, 1.0)

    final_temp = base
    
    if trend == 'stable':
        final_temp = (base * season_factor) + noise
        
    elif trend == 'critical_spike':
        # Spikes huge in July (Month 7)
        if date_obj.month == 7:
            final_temp = 75 + random.uniform(0, 5) # Trigger Critical (>70)
        else:
            final_temp = (base * season_factor) + noise

    elif trend == 'steady_warming':
        # Increases by ~2 degrees every month
        increase = date_obj.month * 2.0 
        final_temp = base + increase + noise

    elif trend == 'steady_cooling':
        # Decreases by ~2 degrees every month
        decrease = date_obj.month * 2.5
        final_temp = base - decrease + noise

    return round(final_temp, 1)

def generate_mock_weather(date_obj):
    month = date_obj.month
    if month in [12, 1, 2]: return round(random.uniform(10, 15), 1)
    if month in [3, 4, 11]: return round(random.uniform(16, 22), 1)
    if month in [5, 10]:    return round(random.uniform(23, 27), 1)
    return round(random.uniform(28, 35), 1)

def run_mock_generator():
    print(f"🚀 Generating MONTHLY Mock Data for {YEAR}...")
    
    engine = get_db_engine()
    rows = []
    
    # Loop through Months 1 to 12
    for month in range(1, 13):
        # Pick a random day in that month (e.g., between 10th and 20th to seem regular)
        day = random.randint(10, 20) 
        # Pick a random time (9 AM - 2 PM)
        hour = random.randint(9, 14)
        minute = random.randint(0, 59)
        
        # Create Timestamp object
        timestamp = datetime(YEAR, month, day, hour, minute)
        
        # DB Timestamp (UTC: -2 hours from Mock Local Time)
        db_timestamp = timestamp - timedelta(hours=2)
        ts_str = db_timestamp.strftime("%Y-%m-%d %H:%M:%S")

        print(f"   Processing Month: {timestamp.strftime('%B')}...", end='\r')

        for asset in ASSETS:
            center_temp = calculate_temp(asset, timestamp)
            max_temp = center_temp + random.uniform(2, 5)
            min_temp = center_temp - random.uniform(2, 5)
            avg_temp = center_temp - random.uniform(0, 1)
            
            row = {
                "Timestamp": ts_str,
                "Filename": f"MOCK_{asset['code']}_{timestamp.strftime('%Y%m%d')}.jpg",
                "Camera_Serial": 999999,
                "Asset_Name": asset['code'], 
                "Max_Temp_C": round(max_temp, 1),
                "Min_Temp_C": round(min_temp, 1),
                "Center_Temp_C": center_temp,
                "Avg_Temp_C": round(avg_temp, 1),
                "Delta_Temp_C": round(max_temp - min_temp, 1),
                "Emissivity": 0.95,
                "Distance": 2.0,
                "weather_temp": generate_mock_weather(timestamp),
                "Image_Base64": generate_dummy_image(center_temp) 
            }
            rows.append(row)

    print(f"\n📦 Inserting {len(rows)} rows into database...")
    
    df = pd.DataFrame(rows)
    try:
        df.to_sql(DB_TABLE, engine, if_exists='append', index=False)
        print("✅ Success! Database populated with monthly data.")
    except Exception as e:
        print(f"❌ Error inserting data: {e}")

if __name__ == "__main__":
    run_mock_generator()