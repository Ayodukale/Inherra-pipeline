import pandas as pd
import json
import snowflake.connector
import sys
from pathlib import Path
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# --- CONFIGURATION ---
ENRICHMENT_OUTPUT_DIR = '/Users/ayoodukale/Documents/Inherra/Python/Inherra scraper/HCAD Tax Enrichment'

# --- SNOWFLAKE CONNECTION DETAILS ---
SNOWFLAKE_USER = 'INHERRA_SERVICE_USER'
SNOWFLAKE_PRIVATE_KEY_PATH = '~/.ssh/inherra_key.p8'
SNOWFLAKE_ACCOUNT = 'aec94635.us-east-1'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

# Note: The database and schema are set explicitly after connection now
SNOWFLAKE_TARGET_DATABASE = 'RAW_DATA_DB'
SNOWFLAKE_TARGET_SCHEMA = 'PROBATE'
SNOWFLAKE_TABLE = 'PROBATE_FILINGS_ENRICHED'


def get_most_recent_file(directory: str) -> str:
    """Finds the path of the most recently modified file in a given directory."""
    try:
        directory_path = Path(directory)
        files = [p for p in directory_path.iterdir() if p.is_file()]
        if not files:
            return None
        latest_file = max(files, key=lambda p: p.stat().st_mtime)
        return str(latest_file)
    except FileNotFoundError:
        return None

def prepare_data_for_upload(df: pd.DataFrame, source_filename: str) -> pd.DataFrame:
    """Takes the raw DataFrame and restructures it to match our Snowflake table."""
    print("Preparing data for upload...")
    upload_df = pd.DataFrame()
    upload_df['PROBATE_CASE_NUMBER'] = df['probate_lead_case_number']
    upload_df['HCAD_ACCOUNT_ID'] = df['hcad_account']
    upload_df['SCRAPE_TIMESTAMP'] = pd.Timestamp.now(tz='UTC')
    upload_df['SOURCE_FILENAME'] = source_filename
    upload_df['HCAD_LOT_SQFT_TOTAL'] = df['hcad_lot_sqft_total']

    
    print("Packing raw records into dictionary format...")
    # --- FINAL FIX based on AI feedback ---
    # Using .to_dict() creates a native Python dictionary, which the Snowflake
    # connector handles perfectly when loading into a VARIANT column.
    upload_df['RAW_RECORD'] = df.apply(lambda row: row.to_dict(), axis=1)
    # --- END FINAL FIX ---

    return upload_df

def main():
    """Main function to run the upload process."""
    print(f"Searching for the latest file in directory: {ENRICHMENT_OUTPUT_DIR}")
    source_file_path = get_most_recent_file(ENRICHMENT_OUTPUT_DIR)
    
    if not source_file_path:
        print(f"❌ ERROR: No files found in directory '{ENRICHMENT_OUTPUT_DIR}'.")
        sys.exit(1)
        
    print(f"Found most recent file to upload: {source_file_path}")
    
    try:
        source_df = pd.read_csv(source_file_path)
        print(f"Successfully read {len(source_df)} rows from the source file.")
    except Exception as e:
        print(f"❌ ERROR: Could not read the CSV file. Details: {e}")
        sys.exit(1)

    upload_df = prepare_data_for_upload(source_df, source_file_path)

    print("Reading private key for Snowflake connection...")
    private_key_path = Path(SNOWFLAKE_PRIVATE_KEY_PATH).expanduser()
    with open(private_key_path, "rb") as key:
        p_key = serialization.load_pem_private_key(
            key.read(),
            password=None,
            backend=default_backend()
        )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            private_key=pkb,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE
        )
        print("Connection to Snowflake successful.")
        print("Setting session context...")
        conn.cursor().execute(f"USE DATABASE {SNOWFLAKE_TARGET_DATABASE}")
        conn.cursor().execute(f"USE SCHEMA {SNOWFLAKE_TARGET_SCHEMA}")
    except Exception as e:
        print(f"❌ ERROR: Could not connect to Snowflake. Details: {e}")
        sys.exit(1)

    print(f"Uploading data to table: {SNOWFLAKE_TABLE}...")
    from snowflake.connector.pandas_tools import write_pandas
    success, nchunks, nrows, _ = write_pandas(
    conn,
    upload_df,
    SNOWFLAKE_TABLE,
    use_logical_type=True)

    conn.close()
    
    if success:
        print(f"✅ Success! Uploaded {nrows} rows to Snowflake.")
    else:
        print("❌ Upload failed.")

if __name__ == '__main__':
    main()