import os
import random
import string
import subprocess
import tempfile
import time
import io
from faker import Faker
import psycopg2
from google.cloud import storage

fake = Faker()

# ──────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────
CONTAINER_NAME = "cloud9-seed-postgres"
PG_IMAGE = "postgres:15"
PG_USER = "syscrack"
PG_PASS = "syscrack"
PG_DB = "syscrack"
PG_PORT = "5433"  # Use non-default port to avoid clashing with any local postgres

BUCKET_NAME = os.environ.get("SEED_BUCKET_NAME", "cloud9-seed-data")
DUMP_PATH = "url-shortener/urls.dump"
LOCAL_DUMP = os.path.join(tempfile.gettempdir(), "urls.dump")

ROW_COUNT = 1_000_000
BATCH_SIZE = 100_000


# ──────────────────────────────────────────────────────────────────────
# Docker helpers
# ──────────────────────────────────────────────────────────────────────
def start_postgres():
    """Spins up a temporary Postgres container. Idempotent — removes any stale container first."""
    print(f"Starting temporary Postgres container ({CONTAINER_NAME})...")
    # Remove stale container if it exists
    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)

    subprocess.run([
        "docker", "run", "-d",
        "--name", CONTAINER_NAME,
        "-e", f"POSTGRES_USER={PG_USER}",
        "-e", f"POSTGRES_PASSWORD={PG_PASS}",
        "-e", f"POSTGRES_DB={PG_DB}",
        "-p", f"{PG_PORT}:5432",
        PG_IMAGE,
    ], check=True, capture_output=True)

    # Wait for postgres to be ready
    print("Waiting for Postgres to become ready...")
    for attempt in range(30):
        try:
            conn = psycopg2.connect(
                host="localhost", port=PG_PORT,
                user=PG_USER, password=PG_PASS, dbname=PG_DB
            )
            conn.close()
            print("Postgres is ready.")
            return
        except psycopg2.OperationalError:
            time.sleep(1)

    raise RuntimeError("Postgres did not become ready in 30 seconds")


def stop_postgres():
    """Stops and removes the temporary container."""
    print(f"Cleaning up container ({CONTAINER_NAME})...")
    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)


# ──────────────────────────────────────────────────────────────────────
# Database operations
# ──────────────────────────────────────────────────────────────────────
def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS urls (
                code        VARCHAR(8) PRIMARY KEY,
                original    TEXT NOT NULL,
                hit_count   INTEGER NOT NULL DEFAULT 0,
                created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
    conn.commit()
    print("Table 'urls' ready.")


def generate_unique_codes(n):
    """Generates exactly n unique 6-character alphanumeric codes."""
    chars = string.ascii_letters + string.digits
    codes = set()
    print(f"Generating {n:,} unique short codes...")
    while len(codes) < n:
        codes.update(''.join(random.choices(chars, k=6)) for _ in range(n - len(codes)))
    return list(codes)


def insert_urls(conn, n=ROW_COUNT, batch_size=BATCH_SIZE):
    all_codes = generate_unique_codes(n)
    hot_codes = set(random.sample(all_codes, 1000))

    print("Pre-computing domain pool...")
    domains = [fake.domain_name() for _ in range(1000)]

    print("Inserting rows using COPY...")
    with conn.cursor() as cur:
        for i in range(0, n, batch_size):
            batch = all_codes[i : i + batch_size]
            buf = io.StringIO()
            for code in batch:
                url = f"https://{random.choice(domains)}/{''.join(random.choices(string.ascii_lowercase, k=8))}"
                hits = random.randint(5000, 50000) if code in hot_codes else random.randint(0, 10)
                buf.write(f"{code}\t{url}\t{hits}\n")
            buf.seek(0)
            cur.copy_from(buf, "urls", columns=("code", "original", "hit_count"))
            conn.commit()
            print(f"  {min(i + batch_size, n):,} / {n:,}")

    print(f"Insertion complete — {n:,} rows.")


# ──────────────────────────────────────────────────────────────────────
# Dump & Upload
# ──────────────────────────────────────────────────────────────────────
def pg_dump():
    """Runs pg_dump inside the Docker container and streams the output locally."""
    print(f"Dumping 'urls' table to {LOCAL_DUMP}...")
    with open(LOCAL_DUMP, "wb") as f:
        subprocess.run([
            "docker", "exec", CONTAINER_NAME,
            "pg_dump", "-Fc", "-U", PG_USER, "-d", PG_DB, "-t", "urls",
        ], stdout=f, check=True)

    size = os.path.getsize(LOCAL_DUMP)
    print(f"Dump successful — {size:,} bytes.")
    if size < 1000:
        raise RuntimeError(f"Dump is suspiciously small ({size} bytes). Aborting upload.")


def upload_to_gcs():
    print(f"Uploading to gs://{BUCKET_NAME}/{DUMP_PATH}...")
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(DUMP_PATH)
    blob.upload_from_filename(LOCAL_DUMP)
    print("Upload successful!")


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        start_postgres()

        print("Connecting to temporary Postgres...")
        conn = psycopg2.connect(
            host="localhost", port=PG_PORT,
            user=PG_USER, password=PG_PASS, dbname=PG_DB
        )

        create_table(conn)
        insert_urls(conn)
        conn.close()
        print("Database connection closed.")

        pg_dump()
        upload_to_gcs()

        print("\n=== Done! Every future deployment will be seeded with ~1M URLs. ===")

    except Exception as e:
        print(f"\n[!] Pipeline aborted: {e}")

    finally:
        stop_postgres()
        if os.path.exists(LOCAL_DUMP):
            os.remove(LOCAL_DUMP)
            print(f"Cleaned up {LOCAL_DUMP}")
