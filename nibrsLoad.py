from google.cloud import storage
import psycopg2
import csv
import os

# Initialize GCS client
storage_client = storage.Client()
bucket_name = "fbi_nibrs"
bucket = storage_client.get_bucket(bucket_name)

# List all files
blobs = bucket.list_blobs()
csv_files = [blob.name for blob in blobs if blob.name.endswith(".csv")]

# Connect to Postgres
conn = psycopg2.connect(
    host="10.12.0.3",
    database="nibrs",
    user="completely-epic-seahorse",
    password="g%_9LeR%U|NlZo@7"
)
cursor = conn.cursor()

# Process each CSV file
for file in csv_files:
    # Download file locally
    blob = bucket.blob(file)
    local_path = f"/tmp/{os.path.basename(file)}"
    blob.download_to_filename(local_path)

    # Load data into Postgres
    with open(local_path, "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            # Process each row if needed
            cursor.execute(
                "INSERT INTO your_table (column1, column2, ...) VALUES (%s, %s, ...)",
                row
            )
            
        cursor.copy_expert(
            f"COPY your_table FROM STDIN WITH CSV HEADER", f
        )

    os.remove(local_path)

conn.commit()
cursor.close()
conn.close()

