import os
import random
import string
import subprocess
import tempfile
import io
from faker import Faker
import psycopg2
from google.cloud import storage

fake = Faker()

DB_CONFIG = {
    "host": os.environ.get("PGHOST", "localhost"),
    "user": os.environ.get("PGUSER", "syscrack"),
    "password": os.environ.get("PGPASSWORD", "syscrack"),
    "dbname": os.environ.get("PGDATABASE", "syscrack"),
    "port": os.environ.get("PGPORT", "5432"),
}

BUCKET_NAME = "ganymede-seed-data"
DUMP_PATH = "url-shortener/urls.dump"
LOCAL_DUMP = os.path.join(tempfile.gettempdir(), "urls.dump")


def create_table(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS urls (
                code VARCHAR(8) PRIMARY KEY,
                original TEXT NOT NULL,
                hit_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """)
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        raise


def generate_unique_codes(n):
    """Generates exactly n unique 6-character alphanumeric codes."""
    chars = string.ascii_letters + string.digits
    unique_codes = set()
    
    print(f"Generating {n:,} unique short codes...")
    while len(unique_codes) < n:
        needed = n - len(unique_codes)
        unique_codes.update(''.join(random.choices(chars, k=6)) for _ in range(needed))
        
    return list(unique_codes)


def insert_urls(conn, n=1_000_000, batch_size=100_000):
    all_codes = generate_unique_codes(n)
    hot_codes = set(random.sample(all_codes, 1000))
    
    # OPTIMIZATION 1: Generate a pool of domains once, instead of 1M Faker calls
    print("Pre-computing domain pool to bypass Faker bottlenecks...")
    domains = [fake.domain_name() for _ in range(1000)]
    
    print("Starting database insertion using COPY in batches...")
    try:
        with conn.cursor() as cursor:
            for i in range(0, n, batch_size):
                batch_codes = all_codes[i : i + batch_size]
                
                # OPTIMIZATION 2: Use StringIO to create an in-memory CSV buffer
                csv_buffer = io.StringIO()
                
                for code in batch_codes:
                    # Construct URL via fast string formatting
                    url = f"https://{random.choice(domains)}/{''.join(random.choices(string.ascii_lowercase, k=8))}"
                    hit_count = random.randint(5000, 50000) if code in hot_codes else random.randint(0, 10)
                    
                    # Write tab-separated values to the buffer
                    csv_buffer.write(f"{code}\t{url}\t{hit_count}\n")
                
                # Reset buffer position to the beginning before reading
                csv_buffer.seek(0)
                
                # OPTIMIZATION 3: Postgres COPY is significantly faster than INSERT
                cursor.copy_from(csv_buffer, 'urls', columns=('code', 'original', 'hit_count'))
                conn.commit()
                
                print(f"Inserted {min(i + batch_size, n):,} / {n:,} rows...")
                
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Database error during insertion: {e}")
        raise
    print("Insertion complete!")


def pg_dump():
    print(f"Dumping database to {LOCAL_DUMP}...")
    
    # Pass the password securely via environment variables to the subprocess
    env = os.environ.copy()
    env["PGPASSWORD"] = DB_CONFIG["password"]
    
    try:
        subprocess.run(
            [
                "pg_dump", "-Fc", 
                "-U", DB_CONFIG["user"], 
                "-h", DB_CONFIG["host"], 
                "-p", DB_CONFIG["port"], 
                "-d", DB_CONFIG["dbname"], 
                "-f", LOCAL_DUMP
            ],
            env=env,
            check=True,  # Raises exception if pg_dump fails
            capture_output=True,
            text=True
        )
        print("Database dump successful.")
    except subprocess.CalledProcessError as e:
        print(f"pg_dump failed with error:\n{e.stderr}")
        raise


def pg_dump_internal():
    print(f"Dumping database to {LOCAL_DUMP}...")
    try:
        # Use pg_dump from postgres:15 container
        subprocess.run([
            "docker", "exec", "ganymede-local",
            "pg_dump", "-Fc", "-U", "syscrack", "-d", "syscrack",
            "-f", "/tmp/urls.dump"
        ], check=True)

        # Copy the dump out of the container
        subprocess.run([
            "docker", "cp", "ganymede-local:/tmp/urls.dump", LOCAL_DUMP
        ], check=True)

        print("Database dump successful.")
    except subprocess.CalledProcessError as e:
        print(f"pg_dump failed: {e}")
        raise

def upload_to_gcs():
    print("Uploading dump to Google Cloud Storage...")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(DUMP_PATH)
        blob.upload_from_filename(LOCAL_DUMP)
        print("Upload successful!")
    except Exception as e:
        print(f"Failed to upload to GCS: {e}")
        raise


if __name__ == "__main__":
    conn = None
    try:
        print("Connecting to the database...")
        conn = psycopg2.connect(**DB_CONFIG)
        
        #create_table(conn)
        #insert_urls(conn, n=1_000_000) 
        pg_dump_internal()
        upload_to_gcs()

    except Exception as e:
        print(f"\n[!] Pipeline aborted due to an error: {e}")
        
    finally:
        # CLEANUP: Always close the DB connection
        if conn is not None:
            conn.close()
            print("Database connection closed.")
            
        # CLEANUP: Remove the massive temp file to free up disk space
        if os.path.exists(LOCAL_DUMP):
            os.remove(LOCAL_DUMP)
            print(f"Cleaned up local dump file: {LOCAL_DUMP}")