import streamlit as st
import pandas as pd
import base64
from PIL import Image
import io
import re

# --- CONFIGURATION ---
CSV_FILE = "Thermal_Data_Log_Base64.csv" 

st.set_page_config(page_title="Thermal Analysis Dashboard", layout="wide")

# --- HELPER FUNCTIONS ---
@st.cache_data
def load_data():
    """Loads the CSV and ensures Timestamp is a datetime object."""
    try:
        df = pd.read_csv(CSV_FILE)
        
        # --- FIX: Handle Exif Date Format (YYYY:MM:DD HH:MM:SS) ---
        # ExifTool often outputs colons in dates. We replace the first two colons with dashes.
        # We use df (not df_notes_only)
        df['Timestamp'] = df['Timestamp'].astype(str).str.replace(
            r'^(\d{4}):(\d{2}):(\d{2})', 
            r'\1-\2-\3', 
            regex=True
        )
        
        # Convert to actual datetime objects
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
        
        # Sort by time
        df = df.sort_values(by='Timestamp', ascending=False)
        return df
    except FileNotFoundError:
        return None

def decode_image(base64_string):
    """Converts the Base64 string back into an image for display."""
    try:
        if pd.isna(base64_string) or base64_string == "":
            return None
        img_data = base64.b64decode(base64_string)
        return Image.open(io.BytesIO(img_data))
    except Exception:
        return None

# --- MAIN APP ---
def main():
    st.title("🔥 FLIR E5 Pro Data Visualizer")

    # 1. Load Data
    df = load_data()
    
    if df is None:
        st.error(f"Could not find {CSV_FILE}. Please run the extraction script first.")
        return

    # 2. Sidebar Filters
    st.sidebar.header("Filters")
    
    # Filter by Serial Number
    if 'Serial Number' in df.columns:
        all_sns = df['Serial Number'].unique()
        selected_sn = st.sidebar.multiselect("Select Camera Serial", all_sns, default=all_sns)
        
        if not selected_sn:
            st.warning("Please select at least one serial number.")
            return

        # Apply Filter
        filtered_df = df[df['Serial Number'].isin(selected_sn)]
    else:
        st.warning("Serial Number column missing, showing all data.")
        filtered_df = df
    
    st.sidebar.markdown(f"**Total Records:** {len(filtered_df)}")

    # 3. High-Level Metrics (Top Row)
    if not filtered_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Highest Max Temp", f"{filtered_df['Max Temp (C)'].max()} °C")
        col2.metric("Lowest Min Temp", f"{filtered_df['Min Temp (C)'].min()} °C")
        col3.metric("Avg Delta", f"{filtered_df['Delta Temp (C)'].mean():.1f} °C")
        # Handle case where timestamp might be NaT (Not a Time)
        if pd.notnull(filtered_df['Timestamp'].iloc[0]):
            col4.metric("Latest Scan", filtered_df['Timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M'))

    st.markdown("---")

    # 4. Timeline Chart
    st.subheader("📈 Temperature Trend (Max Temp)")
    if 'Timestamp' in filtered_df.columns and not filtered_df.empty:
        chart_data = filtered_df[['Timestamp', 'Max Temp (C)', 'Avg Temp (C)']].set_index('Timestamp')
        st.line_chart(chart_data)

    st.markdown("---")

    # 5. Image Gallery
    st.subheader("🖼️ Thermal Image Gallery")
    
    for index, row in filtered_df.iterrows():
        with st.container():
            c1, c2 = st.columns([1, 2]) 
            
            # Left Column: Image
            with c1:
                if "Image_Base64" in row:
                    img = decode_image(row["Image_Base64"])
                    if img:
                        # Updated to remove deprecation warning
                        st.image(img, caption=row['File Name'])
                    else:
                        st.text("No Image Data")
                elif "Heatmap Image Path" in row:
                    st.image(row["Heatmap Image Path"], caption=row['File Name'])

            # Right Column: Data
            with c2:
                st.write(f"**Timestamp:** {row['Timestamp']}")
                
                m1, m2, m3 = st.columns(3)
                m1.metric("Max Temp", f"{row['Max Temp (C)']} °C")
                m2.metric("Center Temp", f"{row['Center Temp (C)']} °C")
                m3.metric("Delta", f"{row['Delta Temp (C)']} °C")
                
                if row['Notes']:
                    st.info(f"📝 **Note:** {row['Notes']}")
                
                with st.expander("See Metadata"):
                    st.write(f"**Emissivity:** {row['Emissivity']}")
                    st.write(f"**Distance:** {row['Distance']}")
                    st.write(f"**Camera:** {row['Camera Model']} ({row['Serial Number']})")
            
            st.divider()

if __name__ == "__main__":
    main()