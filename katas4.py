import sqlite3
import pandas as pd
import os
import datetime

# --- Configuration ---
CSV_FILE = 'global hourly data.csv'
DB_FILE = 'weather_data.db'
REPORT_FILE = 'pipeline_report.md'

# Extracting files 
def extract(file_path, chunksize=1000):
    """Generator for memory-efficient reading of large CSV files."""
    for chunk in pd.read_csv(file_path, chunksize=chunksize, low_memory=False):
        yield chunk

# Data transformation
def transform(chunk):
    """Cleans data, performs type conversion, and adds calculated fields."""
    cols = ['STATION', 'DATE', 'NAME', 'TMP', 'DEW']
    cols = [c for c in cols if c in chunk.columns]
    df = chunk[cols].copy()
    
    # Validation: Drop rows with missing keys
    df = df.dropna(subset=['STATION', 'DATE'])
    
    # Type Conversion: Date
    df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df = df.dropna(subset=['DATE'])
    
    # Parsing NOAA encoded values (format: +0150,5 -> value, quality)
    def parse_noaa_val(val):
        try:
            if pd.isna(val) or not isinstance(val, str): return None
            numeric_part = float(val.split(',')[0])
            return numeric_part / 10.0 if numeric_part != 9999 else None
        except (ValueError, IndexError):
            return None

    df['temp_c'] = df['TMP'].apply(parse_noaa_val)
    df['dew_point_c'] = df['DEW'].apply(parse_noaa_val)
    
    # Calculated Field: Fahrenheit
    df['temp_f'] = df['temp_c'].apply(lambda x: (x * 9/5) + 32 if x is not None else None)
    
    return df.drop(columns=['TMP', 'DEW'])

def load(df, db_path):
    """Loads data into SQLite using an idempotent upsert logic."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS hourly_weather (
            STATION TEXT, DATE TEXT, NAME TEXT,
            temp_c REAL, dew_point_c REAL, temp_f REAL,
            PRIMARY KEY (STATION, DATE)
        )
    ''')
    
    # Use a staging table to handle the 'UPSERT' (Insert or Replace)
    df.to_sql('staging', conn, if_exists='replace', index=False)
    cursor.execute('INSERT OR REPLACE INTO hourly_weather SELECT * FROM staging')
    cursor.execute('DROP TABLE staging')
    
    conn.commit()
    conn.close()

def generate_report(db_path, report_path):
    """Generates a summary report in Markdown format."""
    conn = sqlite3.connect(db_path)
    stats = pd.read_sql_query('SELECT COUNT(*) as total, AVG(temp_c) as avg_c FROM hourly_weather', conn)
    stations = pd.read_sql_query('SELECT NAME, COUNT(*) as count FROM hourly_weather GROUP BY NAME', conn)
    conn.close()
    
    with open(report_path, 'w') as f:
        f.write("# Data Pipeline Report\n")
        f.write(f"Processed {stats['total'][0]} records. Average Temp: {stats['avg_c'][0]:.2f}°C\n\n")
        f.write("## Station Summary\n" + stations.to_markdown(index=False))

# --- Execution ---
for chunk in extract(CSV_FILE):
    load(transform(chunk), DB_FILE)

generate_report(DB_FILE, REPORT_FILE)

