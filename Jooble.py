import requests
import pandas as pd
import configparser
from pathlib import Path
import hashlib
from db_utils import get_db_connection  # Import from the utility file


def get_config(config_path: str) -> configparser.ConfigParser | None:
    """Reads and returns the configuration from a .ini file."""
    try:
        config = configparser.ConfigParser()
        if not config.read(config_path):
            print(f"❌ Config Error: Could not read the config file at: {config_path}")
            return None
        return config
    except configparser.Error as e:
        print(f"❌ Config Error: Failed to parse config file. {e}")
        return None


def fetch_jooble_data(api_key: str) -> pd.DataFrame:
    """Fetches and transforms job listings from the Jooble API."""
    print("🚀 Starting: Fetching data from Jooble...")
    api_url = "https://jooble.org/api/" + api_key
    headers = {"Content-type": "application/json"}
    body = {"keywords": "data engineer, python developer, software engineer, IT", "location": "United States, India"}

    try:
        response = requests.post(api_url, json=body, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        jobs = data.get("jobs", [])

        if not jobs:
            print("⚠️ Warning: Jooble API returned no jobs.")
            return pd.DataFrame()

        df = pd.DataFrame(jobs)
        df = df.rename(columns={
            "id": "api_id", "updated": "date_posted", "title": "position",
            "snippet": "tags", "salary": "salary_min", "link": "url"
        })
        df['salary_max'] = df['salary_min']  # Duplicate salary_min as salary_max

        # Use a raw string for the regex to fix the SyntaxWarning
        df['salary_min'] = df['salary_min'].astype(str).str.extract(r'(\d[\d,.]*)').replace(r'[^\d.]', '',
                                                                                            regex=True).astype(
            float).fillna(0).astype(int)
        df['salary_max'] = df['salary_max'].astype(str).str.extract(r'(\d[\d,.]*)').replace(r'[^\d.]', '',
                                                                                            regex=True).astype(
            float).fillna(0).astype(int)

        required_cols = ['api_id', 'date_posted', 'company', 'position', 'location', 'tags', 'salary_min', 'salary_max',
                         'url']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None

        print(f"✅ Success: Fetched and transformed {len(df)} records from Jooble.")
        return df[required_cols]

    except requests.exceptions.RequestException as e:
        print(f"❌ API Error (Jooble): {e}")
        return pd.DataFrame()


def load_jooble_to_sql(df: pd.DataFrame, conn):
    """Creates and bulk-merges Jooble data into the 'JoobleJobs' table."""
    print("🚀 Starting: Loading Jooble data into SQL Server...")
    if df.empty:
        print("⚠️ No Jooble data to load.")
        return

    cursor = None
    try:
        cursor = conn.cursor()
        create_table_query = """
        IF OBJECT_ID('JoobleJobs', 'U') IS NULL
        BEGIN
            CREATE TABLE JoobleJobs (
                unique_job_id NVARCHAR(64) PRIMARY KEY,
                api_id NVARCHAR(255),
                date_posted DATETIME2,
                company NVARCHAR(255),
                position NVARCHAR(255),
                location NVARCHAR(255),
                tags NVARCHAR(MAX),
                salary_min INT,
                salary_max INT,
                url NVARCHAR(MAX)
            )
        END
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Create a unique, deterministic ID to allow for reliable upserting
        df['unique_job_id'] = df.apply(
            lambda row: hashlib.sha256(f"{row['company']}-{row['position']}-{row['location']}".encode()).hexdigest(),
            axis=1)
        df = df[['unique_job_id', 'api_id', 'date_posted', 'company', 'position', 'location', 'tags', 'salary_min',
                 'salary_max', 'url']]

        merge_query = """
        MERGE JoobleJobs AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) 
               AS source (unique_job_id, api_id, date_posted, company, position, location, tags, salary_min, salary_max, url)
        ON target.unique_job_id = source.unique_job_id
        WHEN MATCHED THEN UPDATE SET
            target.api_id = source.api_id, target.date_posted = source.date_posted, target.company = source.company,
            target.position = source.position, target.location = source.location, target.tags = source.tags,
            target.salary_min = source.salary_min, target.salary_max = source.salary_max, target.url = source.url
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (unique_job_id, api_id, date_posted, company, position, location, tags, salary_min, salary_max, url)
            VALUES (source.unique_job_id, source.api_id, source.date_posted, source.company, source.position, 
                    source.location, source.tags, source.salary_min, source.salary_max, source.url);
        """
        data_to_load = df.where(pd.notnull(df), None).values.tolist()
        cursor.fast_executemany = True
        cursor.executemany(merge_query, data_to_load)
        conn.commit()
        print(f"✅ Success: {len(data_to_load)} rows merged into 'JoobleJobs'.")
    except Exception as e:
        print(f"❌ SQL Error (Jooble): {e}")
        if conn: conn.rollback()
    finally:
        if cursor: cursor.close()


def main():
    """Main ETL process for Jooble jobs."""
    print("--- Starting Jooble ETL Process ---")
    CONFIG_FILE = Path(__file__).parent / "config.ini"
    config = get_config(CONFIG_FILE)
    if not config:
        return

    conn = None
    try:
        db_config = config['database']
        conn = get_db_connection(db_config)
        if conn:
            jooble_df = fetch_jooble_data(config['api']['jooble_api_key'])
            jooble_df['date_posted'] = pd.to_datetime(jooble_df['date_posted'], errors='coerce')
            load_jooble_to_sql(jooble_df, conn)
    except KeyError as e:
        print(
            f"❌ DB Connection Error: Missing key in config file. Please ensure '[database]' section is defined with all required fields. {e}")
    finally:
        if conn:
            conn.close()
            print("🔒 SQL Server connection closed.")
            print("--- Jooble ETL Process Finished ---")


if __name__ == "__main__":
    main()