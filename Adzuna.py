import requests
import pandas as pd
import hashlib
from pathlib import Path
import configparser
from db_utils import get_db_connection


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


def fetch_adzuna_data(app_id: str, app_key: str) -> pd.DataFrame:
    """Fetches and transforms job listings from Adzuna API."""
    print("🚀 Starting: Fetching data from Adzuna...")

    url = "http://api.adzuna.com/v1/api/jobs/in/search/1"
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": 50,
        "what": "IT or Data or AI",  # keyword search
        "content-type": "application/json"
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        jobs = data.get("results", [])

        if not jobs:
            print("⚠️ Warning: Adzuna API returned no jobs.")
            return pd.DataFrame()

        df = pd.DataFrame(jobs)

        # Transform / standardize columns
        df = df.rename(columns={
            "id": "api_id",
            "created": "date_posted",
            "title": "position",
            "salary_min": "salary_min",
            "salary_max": "salary_max",
            "redirect_url": "redirect_url"
        })

        # ✅ Extract nested fields
        df["company"] = df["company"].apply(
            lambda x: x.get("display_name") if isinstance(x, dict) else x
        )
        df["location"] = df["location"].apply(
            lambda x: ", ".join(x.get("area", [])) if isinstance(x, dict) else x
        )
        df["category"] = df["category"].apply(
            lambda x: x.get("label") if isinstance(x, dict) else x
        )

        # ✅ Ensure required columns exist
        required_cols = [
            "api_id", "date_posted", "company", "position",
            "location", "category", "salary_min", "salary_max", "redirect_url"
        ]
        for col in required_cols:
            if col not in df.columns:
                df[col] = None

        df = df[required_cols]

        # ✅ Salary cleaning to prevent SQL out-of-range errors
        for col in ["salary_min", "salary_max"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")  # convert to number
            df[col] = df[col].fillna(0)  # replace NaN with 0
            df[col] = df[col].round(0).astype("Int64")  # round and keep as nullable int
            df[col] = df[col].clip(lower=0, upper=9223372036854775807)  # fit BIGINT range

        print(f"✅ Success: Fetched and transformed {len(df)} records from Adzuna.")
        return df

    except requests.exceptions.RequestException as e:
        print(f"❌ API Error (Adzuna): {e}")
        return pd.DataFrame()


def load_adzuna_to_sql(df: pd.DataFrame, conn):
    """Creates and bulk-merges Adzuna data into the 'AdzunaJobs' table."""
    print("🚀 Starting: Loading Adzuna data into SQL Server...")
    if df.empty:
        print("⚠️ No Adzuna data to load.")
        return

    cursor = None
    try:
        cursor = conn.cursor()
        create_table_query = """
        IF OBJECT_ID('AdzunaJobs', 'U') IS NULL
        BEGIN
            CREATE TABLE AdzunaJobs (
                unique_job_id NVARCHAR(64) PRIMARY KEY,
                api_id NVARCHAR(255),
                date_posted DATETIME2,
                company NVARCHAR(255),
                position NVARCHAR(255),
                location NVARCHAR(500),
                category NVARCHAR(255),
                salary_min BIGINT,
                salary_max BIGINT,
                redirect_url NVARCHAR(MAX)
            )
        END
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Create unique deterministic ID
        df['unique_job_id'] = df.apply(
            lambda row: hashlib.sha256(f"{row['company']}-{row['position']}-{row['location']}".encode()).hexdigest(),
            axis=1)

        df = df[['unique_job_id', 'api_id', 'date_posted', 'company', 'position',
                 'location', 'category', 'salary_min', 'salary_max', 'redirect_url']]

        merge_query = """
        MERGE AdzunaJobs AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?))
               AS source (unique_job_id, api_id, date_posted, company, position, location, category, salary_min, salary_max, redirect_url)
        ON target.unique_job_id = source.unique_job_id
        WHEN MATCHED THEN UPDATE SET
            target.api_id = source.api_id, target.date_posted = source.date_posted, target.company = source.company,
            target.position = source.position, target.location = source.location, target.category = source.category,
            target.salary_min = source.salary_min, target.salary_max = source.salary_max, target.redirect_url = source.redirect_url
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (unique_job_id, api_id, date_posted, company, position, location, category, salary_min, salary_max, redirect_url)
            VALUES (source.unique_job_id, source.api_id, source.date_posted, source.company, source.position, 
                    source.location, source.category, source.salary_min, source.salary_max, source.redirect_url);
        """
        data_to_load = df.where(pd.notnull(df), None).values.tolist()
        cursor.fast_executemany = True
        cursor.executemany(merge_query, data_to_load)
        conn.commit()
        print(f"✅ Success: {len(data_to_load)} rows merged into 'AdzunaJobs'.")
    except Exception as e:
        print(f"❌ SQL Error (Adzuna): {e}")
        if conn: conn.rollback()
    finally:
        if cursor: cursor.close()


def main():
    """Main ETL process for Adzuna jobs."""
    print("--- Starting Adzuna ETL Process ---")
    CONFIG_FILE = Path(__file__).parent / "config.ini"
    config = get_config(CONFIG_FILE)
    if not config:
        return

    conn = None
    try:
        db_config = config['database']
        conn = get_db_connection(db_config)
        if conn:
            adzuna_df = fetch_adzuna_data(config['adzuna']['app_id'], config['adzuna']['app_key'])
            adzuna_df["date_posted"] = pd.to_datetime(adzuna_df["date_posted"], errors="coerce")
            load_adzuna_to_sql(adzuna_df, conn)
    finally:
        if conn:
            conn.close()
            print("🔒 SQL Server connection closed.")
            print("--- Adzuna ETL Process Finished ---")


if __name__ == "__main__":
    main()
