import snowflake.connector
import os, sys
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
STAGE_NAME = os.getenv('STAGE_NAME', 'BORDER_STAGE')
TABLE_NAME = os.getenv('TABLE_NAME', 'BRONZE_BORDER')
FILE_FORMAT_NAME = os.getenv('FILE_FORMAT_NAME', 'JSON_FMT')

LOCAL_FILE_PATH = os.getenv('LOCAL_FILE_PATH')

required = [
    ('SNOWFLAKE_USER', SNOWFLAKE_USER),
    ('SNOWFLAKE_PASSWORD', SNOWFLAKE_PASSWORD),
    ('SNOWFLAKE_ACCOUNT', SNOWFLAKE_ACCOUNT),
    ('LOCAL_FILE_PATH', LOCAL_FILE_PATH),
]
missing = [name for name, val in required if not val]
if missing:
    sys.exit(f"Missing required environment variable(s): {', '.join(missing)}\nPlease add them to a .env file or set them in your environment.")

def upload_json_to_snowflake():
    """Connects to Snowflake, stages the file, and copies the data into the table."""
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
       # 2. Stage the JSON File (PUT command)
        local_posix = LOCAL_FILE_PATH.replace('\\', '/')
        put_uri = f"file://{local_posix}"
        # *** FIX: Use fully qualified stage name ***
        fully_qualified_stage = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{STAGE_NAME}"
        put_command = f"PUT '{put_uri}' @{fully_qualified_stage}"
        print(f"\nExecuting PUT command to stage file: {put_command}")
        cur.execute(put_command)
        # Display staging results (optional)
        print("Staging Results:")
        for row in cur.fetchall():
            print(row)

        # 3. Copy Data into the Table (COPY INTO command)
        filename_on_stage = os.path.basename(LOCAL_FILE_PATH)
        
        copy_command = f"""
        COPY INTO {TABLE_NAME} (CONTENT, SOURCE_FILE, INGESTION_TIME)
        FROM (
            SELECT 
                $1,                                 -- Selects the entire raw JSON record (into CONTENT VARIANT)
                METADATA$FILENAME,                  -- Selects the source file name (into SOURCE_FILE VARCHAR)
                CURRENT_TIMESTAMP()                 -- Selects the current timestamp (into INGESTION_TIME TIMESTAMP)
            FROM @{STAGE_NAME}/{filename_on_stage}
        )
        FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT_NAME}')
        ON_ERROR = 'CONTINUE'; 
        """
        print(f"\nExecuting COPY INTO command...")
        cur.execute(copy_command)
        
        # Display copy results
        print("COPY INTO Results:")
        for row in cur.fetchall():
            print(row)
            
        print("\nData successfully loaded into Snowflake.")
        
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        
    finally:
        if conn:
            conn.close()
            print("\nConnection closed.")

if __name__ == "__main__":
    upload_json_to_snowflake()