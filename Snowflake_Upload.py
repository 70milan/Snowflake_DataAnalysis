import snowflake.connector
import os, sys
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
STAGE_NAME = os.getenv('STAGE_NAME', 'BORDER_STAGE')
TABLE_NAME = os.getenv('TABLE_NAME', 'BRONZE_BORDER')

LOCAL_FILE_PATH = os.getenv('LOCAL_FILE_PATH')
TRUNCATE_BEFORE_LOAD = os.getenv('TRUNCATE_BEFORE_LOAD', 'true').lower() in ('1', 'true', 'yes')

# --- Validation ---
required = [
    ('SNOWFLAKE_USER', SNOWFLAKE_USER),
    ('SNOWFLAKE_PASSWORD', SNOWFLAKE_PASSWORD),
    ('SNOWFLAKE_ACCOUNT', SNOWFLAKE_ACCOUNT),
    ('LOCAL_FILE_PATH', LOCAL_FILE_PATH),
]
missing = [name for name, val in required if not val]
if missing:
    sys.exit(f"Missing required environment variable(s): {', '.join(missing)}")

def upload_json_to_snowflake():
    conn = None
    try:
        # 1. Establish Connection
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cur = conn.cursor()
        print("Connection established successfully.")

        # Prepare variables
        local_posix = LOCAL_FILE_PATH.replace('\\', '/')
        filename_clean = os.path.basename(local_posix) # e.g., 'border_crossing_dataset.json'
        fully_qualified_stage = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{STAGE_NAME}"

        # 2. Cleanup Old Files (Optional but Recommended)
        # We remove the .gz version to avoid loading the wrong file
        print(f"\nCleaning up old .gz files for {filename_clean}...")
        cur.execute(f"REMOVE @{fully_qualified_stage}/{filename_clean}.gz")

        # 3. Stage the JSON File (PUT command)
        # We use AUTO_COMPRESS=FALSE to keep it as .json so we can debug easily
        put_uri = f"file://{local_posix}"
        put_command = f"PUT '{put_uri}' @{fully_qualified_stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        
        print(f"\nExecuting PUT command: {put_command}")
        cur.execute(put_command)
        
        # Verify Staging
        print("Staging Results:")
        for row in cur.fetchall():
            print(row)

        # 4. Truncate Table (Optional)
        if TRUNCATE_BEFORE_LOAD:
            print(f"\nTruncating table {TABLE_NAME}...")
            cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")

        # 5. Copy Data (The Critical Fix)
        # STRIP_OUTER_ARRAY = TRUE splits the JSON array into individual rows.
        # FILES = ('...') explicitly picks the file we just uploaded.
        copy_command = f"""
        COPY INTO {TABLE_NAME} (CONTENT, SOURCE_FILE, INGESTION_TIME)
        FROM (
            SELECT 
                $1, 
                METADATA$FILENAME, 
                CURRENT_TIMESTAMP() 
            FROM @{fully_qualified_stage}
        )
        FILES = ('{filename_clean}')
        FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
        ON_ERROR = 'CONTINUE';
        """
        
        print(f"\nExecuting COPY INTO command...")
        cur.execute(copy_command)
        
        # Display copy results and check for failures
        print("COPY INTO Results:")
        rows = cur.fetchall()
        for row in rows:
            print(row)
            # row[1] is the status column. If it says LOAD_FAILED, we alert the user.
            if row[1] == 'LOAD_FAILED':
                print(f"!!! CRITICAL ERROR: Load Failed. Reason: {row[6]}")
            else:
                print(f"\nSUCCESS: Data loaded. Rows loaded: {row[3]}")
        
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        
    finally:
        if conn:
            conn.close()
            print("\nConnection closed.")

if __name__ == "__main__":
    upload_json_to_snowflake()