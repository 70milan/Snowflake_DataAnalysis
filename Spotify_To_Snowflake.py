import requests
import os,sys
from spotipy.oauth2 import SpotifyOAuth
import snowflake.connector
import json
import boto3
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv('SP_CREDS_CLIENT_ID')
client_secret = os.getenv('SP_CREDS_CLIENT_SECRET')
username = os.getenv('SP_CREDS_USERNAME')
scope = "user-library-read"
redirect_uri = os.getenv('SPOTIPY_REDIRECT_URI', "http://127.0.0.1:8888/callback")

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
bucket_name = os.getenv('S3_BUCKET_NAME', 's3numerone')
project_dir = os.getenv('PROJECT_DIR', 'project_sp_yt/')

# --- Configuration ---
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = 'ELT_PROJECT'
SNOWFLAKE_SCHEMA = 'SPOTIFY' 
STAGE_NAME = 'SPOTIFY_STAGE'
TABLE_NAME = 'BRONZE_SP_ALL_ITEMS'
TRUNCATE_BEFORE_LOAD = os.getenv('TRUNCATE_BEFORE_LOAD', 'true').lower() in ('1', 'true', 'yes')


sp_oauth = SpotifyOAuth(
  client_id=client_id,
  client_secret=client_secret,
  redirect_uri=redirect_uri, 
  scope=scope
 )



#Authentication

token_info = sp_oauth.get_access_token(as_dict=True)
access_token = (token_info['access_token'])
print(access_token)
headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }
response = requests.get('https://api.spotify.com/v1/me/tracks', headers=headers).json()
total = response['total']
print(f"Total tracks: {total}")


# Extraction with progress indicator
all_items = []
limit = 20
if total == 0:
    print("No tracks to fetch.")
else:
    for offset in range(0, total, limit):
        url = f"https://api.spotify.com/v1/me/tracks?offset={offset}&limit={limit}"
        response1 = requests.get(url, headers=headers).json()
        items = response1.get('items', [])
        all_items.extend(items)

        fetched = len(all_items)
        percent = min(100, int(fetched / total * 100)) if total > 0 else 100
        sys.stdout.write(f'\rFetching tracks: {fetched}/{total} ({percent}%)')
        sys.stdout.flush()
    sys.stdout.write('\n')


def fetch_artist_list_exploded(all_items):
    artists_by_id = {}
    for j in all_items:
        artists = j['track']['album']['artists']
        for artist in artists:
            if artist['id'] not in artists_by_id:
                artists_by_id[artist['id']] = [artist['name']]
            elif artist['name'] not in artists_by_id[artist['id']]:
                artists_by_id[artist['id']].append(artist['name'])

    artist_list_exploded = list(artists_by_id.keys())
    return artist_list_exploded


#artist_list_exploded = fetch_artist_list_exploded(all_items)


# Fetch genres for each artist with progress indicator
def fetch_genre_by_artists(artist_list_exploded):
    genre_by_artists = []
    url2 = "https://api.spotify.com/v1/artists/"
    total_artists = len(artist_list_exploded)
    if total_artists == 0:
        print("No artists to fetch genres for.")
        return genre_by_artists

    for idx, artist_id in enumerate(artist_list_exploded, start=1):
        genre_url = url2 + artist_id
        response2 = requests.get(genre_url, headers=headers)
        if response2.status_code == 200:
            artist = response2.json()
            genre_by_artists.append({
                'artist_id': artist_id,
                'genres': artist.get('genres', [])
            })
        else:
            genre_by_artists.append({
                'artist_id': artist_id,
                'genres': []
            })

        percent = int(idx / total_artists * 100)
        sys.stdout.write(f'\rFetching genres: {idx}/{total_artists} ({percent}%)')
        sys.stdout.flush()

    sys.stdout.write('\n')
    return genre_by_artists


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
        
        # Define local file names
        ALL_ITEMS_FILE = "all_items.json"
        GENRE_BY_ARTISTS_FILE = "genre_by_artists.json"

        # Save the JSON data locally
        with open(ALL_ITEMS_FILE, "w", encoding="utf-8") as f:
            json.dump(all_items, f, indent=2)
        with open(GENRE_BY_ARTISTS_FILE, "w", encoding="utf-8") as f:
            json.dump(genre_by_artists, f, indent=2)

        # Prepare fully qualified stage name
        fully_qualified_stage = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{STAGE_NAME}"

        # ---------------------------------------------------------
        # DEFINING THE ROUTING LOGIC (File -> Table)
        # ---------------------------------------------------------
        file_mapping = {
            ALL_ITEMS_FILE: "BRONZE_SP_ALL_ITEMS",
            GENRE_BY_ARTISTS_FILE: "BRONZE_SP_ARTIST_GENRE"
        }

        for local_file, target_table in file_mapping.items():
            local_posix = os.path.abspath(local_file).replace("\\", "/")
            filename_clean = os.path.basename(local_posix)
            
            # 1. PUT the file into the stage
            put_command = f"PUT 'file://{local_posix}' @{fully_qualified_stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            print(f"\nUploading {filename_clean} to Snowflake stage...")
            cur.execute(put_command)
            
            # 2. COPY INTO the SPECIFIC target table
            # We use STRIP_OUTER_ARRAY = TRUE to explode the list into individual rows
            copy_cmd = f"""
            COPY INTO {target_table} (item_data, load_timestamp)
            FROM (
                SELECT 
                    $1,                  -- The individual JSON item
                    CURRENT_TIMESTAMP()  -- The separate timestamp field
                FROM @{fully_qualified_stage}
            )
            FILES = ('{filename_clean}')
            FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE) 
            ON_ERROR = 'CONTINUE';
            """
            
            print(f"\nExecuting COPY for {filename_clean} into table {target_table}...")
            cur.execute(copy_cmd)
            
            # Check results
            rows = cur.fetchall()
            for row in rows:
                # row[3] is typically the number of rows parsed/loaded
                print(f"File: {filename_clean} -> Table: {target_table} | Status: {row[1]} | Rows Loaded: {row[3]}")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
        
    finally:
        if conn:
            conn.close()
            print("\nConnection closed.")

if __name__ == "__main__":
    # (re)run the extraction-to-objects pipeline and then upload
    artist_list_exploded = fetch_artist_list_exploded(all_items)
    genre_by_artists = fetch_genre_by_artists(artist_list_exploded)
    upload_json_to_snowflake()



# def upload_json_to_snowflake():
#     conn = None
#     try:
#         # 1. Establish Connection
#         conn = snowflake.connector.connect(
#             user=SNOWFLAKE_USER,
#             password=SNOWFLAKE_PASSWORD,
#             account=SNOWFLAKE_ACCOUNT,
#             warehouse=SNOWFLAKE_WAREHOUSE,
#             database=SNOWFLAKE_DATABASE,
#             schema=SNOWFLAKE_SCHEMA
#         )
#         cur = conn.cursor()
#         print("Connection established successfully.")

#         # Prepare variables
#         local_posix = LOCAL_FILE_PATH.replace('\\', '/')
#         filename_clean = os.path.basename(local_posix) # e.g., 'border_crossing_dataset.json'
#         fully_qualified_stage = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{STAGE_NAME}"

#         # 2. Cleanup Old Files (Optional but Recommended)
#         # We remove the .gz version to avoid loading the wrong file
#         print(f"\nCleaning up old .gz files for {filename_clean}...")
#         cur.execute(f"REMOVE @{fully_qualified_stage}/{filename_clean}.gz")

#         # 3. Stage the JSON File (PUT command)
#         # We use AUTO_COMPRESS=FALSE to keep it as .json so we can debug easily
#         put_uri = f"file://{local_posix}"
#         put_command = f"PUT '{put_uri}' @{fully_qualified_stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        
#         print(f"\nExecuting PUT command: {put_command}")
#         cur.execute(put_command)
        
#         # Verify Staging
#         print("Staging Results:")
#         for row in cur.fetchall():
#             print(row)

#         # 4. Truncate Table (Optional)
#         if TRUNCATE_BEFORE_LOAD:
#             print(f"\nTruncating table {TABLE_NAME}...")
#             cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")

#         # 5. Copy Data (The Critical Fix)
#         # STRIP_OUTER_ARRAY = TRUE splits the JSON array into individual rows.
#         # FILES = ('...') explicitly picks the file we just uploaded.
#         copy_command = f"""
#         COPY INTO {TABLE_NAME} (CONTENT, SOURCE_FILE, INGESTION_TIME)
#         FROM (
#             SELECT 
#                 $1, 
#                 METADATA$FILENAME, 
#                 CURRENT_TIMESTAMP() 
#             FROM @{fully_qualified_stage}
#         )
#         FILES = ('{filename_clean}')
#         FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
#         ON_ERROR = 'CONTINUE';
#         """
        
#         print(f"\nExecuting COPY INTO command...")
#         cur.execute(copy_command)
        
#         # Display copy results and check for failures
#         print("COPY INTO Results:")
#         rows = cur.fetchall()
#         for row in rows:
#             print(row)
#             # row[1] is the status column. If it says LOAD_FAILED, we alert the user.
#             if row[1] == 'LOAD_FAILED':
#                 print(f"!!! CRITICAL ERROR: Load Failed. Reason: {row[6]}")
#             else:
#                 print(f"\nSUCCESS: Data loaded. Rows loaded: {row[3]}")
        
#     except Exception as e:
#         print(f"\nAn error occurred: {e}")
        
#     finally:
#         if conn:
#             conn.close()
#             print("\nConnection closed.")

   
# # date_str = datetime.now().strftime("%Y%m%d")
# # output_path = rf'D:\projects_de\dagster_ETL_SP\SP_Dagster_ETL\spotify_export\all_sp_items_{date_str}.json'

# # with open(output_path, 'w') as f:
# #     json.dump({
# #         "all_items": all_items,
# #         "genre_by_artists": genre_by_artists
# #     }, f, indent=2)

# # print(f"Exported {len(all_items)} items and {len(genre_by_artists)} artists to {output_path}")
 


# #Upload stuff to S3

# s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
# all_items_json = json.dumps(all_items, indent=2)
# genre_by_artists_json = json.dumps(genre_by_artists, indent=2)

# s3.put_object(Bucket=bucket_name, Key=f'{project_dir}all_items.json', Body=all_items_json)
# s3.put_object(Bucket=bucket_name, Key=f'{project_dir}genre_by_artists.json', Body=genre_by_artists_json)

# print("Finished")

