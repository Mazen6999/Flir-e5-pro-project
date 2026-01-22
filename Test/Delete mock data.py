from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# --- CONFIG ---
DB_SERVER = "PSQLAPPEG297-01"
DB_NAME = "Flir"
DB_USER = "Flir"
DB_PASS = "Prom@2025"
DB_TABLE = "ThermalReadings"

# --- DATE RANGE TO DELETE (Inclusive) ---
DELETE_FROM_DATE = "2025-01-1 00:00:00"
DELETE_END_DATE  = "2026-01-11 23:59:59"

def delete_range_records():
    # 1. Setup Connection
    encoded_pass = quote_plus(DB_PASS)
    db_url = f"mssql+pyodbc://{DB_USER}:{encoded_pass}@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(db_url)

    try:
        with engine.connect() as conn:
            # 2. Check how many records match the range
            count_query = text(f"""
                SELECT COUNT(*) FROM {DB_TABLE} 
                WHERE Timestamp >= :start_date AND Timestamp <= :end_date
            """)
            
            params = {"start_date": DELETE_FROM_DATE, "end_date": DELETE_END_DATE}
            result = conn.execute(count_query, params).scalar()

            if result == 0:
                print(f"✅ No records found between {DELETE_FROM_DATE} and {DELETE_END_DATE}.")
                return

            print(f"⚠️  WARNING: You are about to DELETE {result} records.")
            print(f"   📅 Range: {DELETE_FROM_DATE}  -->  {DELETE_END_DATE}")
            confirm = input("Type 'DELETE' to confirm: ")

            if confirm == "DELETE":
                # 3. Perform Deletion
                delete_query = text(f"""
                    DELETE FROM {DB_TABLE} 
                    WHERE Timestamp >= :start_date AND Timestamp <= :end_date
                """)
                conn.execute(delete_query, params)
                conn.commit()
                print(f"🗑️  Success: {result} records deleted.")
            else:
                print("❌ Operation cancelled.")

    except Exception as e:
        print(f"❌ Database Error: {e}")

if __name__ == "__main__":
    delete_range_records()