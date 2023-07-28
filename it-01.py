import os
import pandas as pd
import psycopg2
from google.cloud import storage

def load_csv_to_postgres(bucket_name, postgres_host, postgres_db, postgres_user, postgres_password):
    # Initialize GCS client
    storage_client = storage.Client()

    # Get all CSV files from the GCS bucket
    bucket = storage_client.get_bucket(bucket_name)
    csv_files = [blob.name for blob in bucket.list_blobs() if blob.name.endswith('.csv')]

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_db,
        user=postgres_user,
        password=postgres_password
    )
    cur = conn.cursor()

    try:
        for csv_file in csv_files:
            # Read the CSV file using pandas
            df = pd.read_csv(f"gs://{bucket_name}/{csv_file}")

            # Extract table name from the CSV file name
            table_name = os.path.splitext(os.path.basename(csv_file))[0]

            # Create table dynamically in PostgreSQL
            columns = ", ".join([f'"{col}" VARCHAR' for col in df.columns])
            create_table_query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns});'
            cur.execute(create_table_query)

            # Load data into the PostgreSQL table
            for _, row in df.iterrows():
                insert_data_query = f'INSERT INTO "{table_name}" VALUES ({", ".join(["%s"] * len(row))});'
                cur.execute(insert_data_query, tuple(row))

            conn.commit()
            print(f"Data from '{csv_file}' has been loaded into '{table_name}' table.")

    except Exception as e:
        conn.rollback()
        print(f"Error occurred: {e}")

    finally:
        cur.close()
        conn.close()

# Replace with your GCS bucket name
bucket_name = "your-gcs-bucket"
# Replace with your PostgreSQL connection details
postgres_host = "your-postgres-host"
postgres_db = "your-postgres-database"
postgres_user = "your-postgres-user"
postgres_password = "your-postgres-password"

load_csv_to_postgres(bucket_name, postgres_host, postgres_db, postgres_user, postgres_password)
