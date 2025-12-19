import requests
import os
import sys
from spotipy.oauth2 import SpotifyOAuth
import snowflake.connector
import json
import boto3
from dotenv import load_dotenv
import concurrent.futures


load_dotenv()

# Configuration
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = 'ELT_PROJECT'
SNOWFLAKE_SCHEMA = 'SPOTIFY'
STAGE_NAME = 'SPOTIFY_STAGE'
TABLE_NAME = 'BRONZE_SP_ALL_ITEMS'
TRUNCATE_BEFORE_LOAD = os.getenv('TRUNCATE_BEFORE_LOAD', 'false').lower() in ('1', 'true', 'yes')

client_id = os.getenv('SP_CREDS_CLIENT_ID')
client_secret = os.getenv('SP_CREDS_CLIENT_SECRET')
username = os.getenv('SP_CREDS_USERNAME')
scope = "user-library-read"
redirect_uri = os.getenv('SPOTIPY_REDIRECT_URI', "http://127.0.0.1:8888/callback")

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
bucket_name = os.getenv('S3_BUCKET_NAME', 's3numerone')
project_dir = os.getenv('PROJECT_DIR', 'project_sp_yt/')

sp_oauth = SpotifyOAuth(
    client_id=client_id,
    client_secret=client_secret,
    redirect_uri=redirect_uri,
    scope=scope
)

# Authentication
token_info = sp_oauth.get_access_token(as_dict=True)
access_token = token_info['access_token']
print(access_token)
headers = {
    'Authorization': f'Bearer {access_token}'
}
response = requests.get('https://api.spotify.com/v1/me/tracks', headers=headers).json()
total = response['total']
print(f"Total tracks: {total}")

def fetch_all_tracks(total, headers):
    all_items = []
    limit = 20
    if total == 0:
        print("No tracks to fetch.")
        return all_items

    def fetch_page(offset):
        url = f"https://api.spotify.com/v1/me/tracks?offset={offset}&limit={limit}"
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            return data.get('items', [])
        else:
            print(f"Error fetching at offset {offset}: {resp.status_code}")
            return []

    offsets = list(range(0, total, limit))
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_page, offset) for offset in offsets]
        for future in concurrent.futures.as_completed(futures):
            all_items.extend(future.result())

    print(f"Fetched {len(all_items)} tracks.")
    return all_items

def fetch_artist_list_exploded(all_items):
    artists_by_id = {}
    for item in all_items:
        track = item.get('track', {})
        for artist in track.get('album', {}).get('artists', []):
            artists_by_id[artist['id']] = artist['name']
    artist_list_exploded = list(artists_by_id.keys())
    print(f"Extracted {len(artist_list_exploded)} unique artists.")
    return artist_list_exploded

def fetch_genre_by_artists(artist_list_exploded, headers):
    genre_by_artists = []
    url2 = "https://api.spotify.com/v1/artists/"

    def fetch_artist(artist_id):
        genre_url = f"{url2}{artist_id}"
        try:
            response2 = requests.get(genre_url, headers=headers)
            if response2.status_code == 200:
                artist = response2.json()
                return {
                    'id': artist_id,
                    'name': artist.get('name', None),
                    'genres': artist.get('genres', [])
                }
            else:
                return {
                    'id': artist_id,
                    'name': None,
                    'genres': []
                }
        except Exception as e:
            print(f"Error fetching artist {artist_id}: {e}")
            return {
                'id': artist_id,
                'name': None,
                'genres': []
            }

    total_artists = len(artist_list_exploded)
    if total_artists == 0:
        print("No artists to fetch genres for.")
        return genre_by_artists

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_artist, artist_id) for artist_id in artist_list_exploded]
        for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            genre_by_artists.append(future.result())
            if idx % 5 == 0 or idx == total_artists:
                percent = int(idx / total_artists * 100)
                sys.stdout.write(f'\rFetching genres: {idx}/{total_artists} ({percent}%)')
                sys.stdout.flush()
    sys.stdout.write('\n')
    return genre_by_artists

def upload_json_to_snowflake(all_items, genre_by_artists, stage_name):
    conn = None
    try:
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

        ALL_ITEMS_FILE = "all_items.json"
        GENRE_BY_ARTISTS_FILE = "genre_by_artists.json"

        with open(ALL_ITEMS_FILE, "w", encoding="utf-8") as f:
            json.dump(all_items, f, indent=2)
        with open(GENRE_BY_ARTISTS_FILE, "w", encoding="utf-8") as f:
            json.dump(genre_by_artists, f, indent=2)

        fully_qualified_stage = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{stage_name}"

        file_mapping = {
            ALL_ITEMS_FILE: "BRONZE_SP_ALL_ITEMS",
            GENRE_BY_ARTISTS_FILE: "BRONZE_SP_ARTIST_GENRE"
        }

        for local_file, target_table in file_mapping.items():
            local_posix = os.path.abspath(local_file).replace("\\", "/")
            filename_clean = os.path.basename(local_posix)

            put_command = f"PUT 'file://{local_posix}' @{fully_qualified_stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            print(f"Uploading {filename_clean} to stage {fully_qualified_stage}...")
            cur.execute(put_command)
            for row in cur.fetchall():
                print(row)

            # Always truncate before load
            print(f"Truncating target table {target_table}...")
            cur.execute(f"TRUNCATE TABLE {target_table}")

            copy_cmd = f"""
            COPY INTO {target_table} (item_data, load_timestamp)
            FROM (
                SELECT $1, CURRENT_TIMESTAMP()
                FROM @{fully_qualified_stage}/{filename_clean}
            )
            FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
            ON_ERROR = 'CONTINUE';
            """
            print(f"Executing COPY INTO {target_table} from {filename_clean}...")
            cur.execute(copy_cmd)
            result = cur.fetchall()
            for row in result:
                status = row[1] if len(row) > 1 else row[0]
                rows_loaded = row[3] if len(row) > 3 else None
                print(f" -> {status}, rows_loaded={rows_loaded}")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
    finally:
        if conn:
            conn.close()
            print("Connection closed.")

if __name__ == "__main__":
    all_items = fetch_all_tracks(total, headers)
    artist_list_exploded = fetch_artist_list_exploded(all_items)
    genre_data = fetch_genre_by_artists(artist_list_exploded, headers)
    upload_json_to_snowflake(all_items, genre_data, STAGE_NAME)
    print("PIPELINE COMPLETE.")