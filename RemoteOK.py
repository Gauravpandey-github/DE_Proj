import requests
import pandas as pd
import pyodbc
import configparser
from pathlib import Path

def fetch_remoteok_data(api_url: str) -> pd.DataFrame | None:
    """
    Fetches job listings from the RemoteOK API and returns them as a pandas DataFrame.

    Args:
        api_url: The URL of the RemoteOK API.

    Returns:
        A pandas DataFrame containing job data, or None if the API call fails.
    """
    print("🚀 Starting: Fetching data from RemoteOK API...")
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)

        data = response.json()
        if not data or not isinstance(data, list) or len(data) < 2:
            print("❌ Error: API returned unexpected data format.")
            return None

        job_listings = data[1:]  # First item is metadata; skip it

        jobs_data = []
        for job in job_listings:
            job_info = {
                'id': job.get('id'),
                'date_posted': job.get('date'),
                'company': job.get('company'),
                'position': job.get('position'),
                'location': job.get('location'),
                'tags': ', '.join(job.get('tags', [])),
                'salary_min': job.get('salary_min') if job.get('salary_min') else None,
                'salary_max': job.get('salary_max') if job.get('salary_max') else None,
                'url': job.get('url')
            }
            jobs_data.append(job_info)

        # Convert to DataFrame and clean data types
        df = pd.DataFrame(jobs_data)
        # Convert to datetime (force UTC if present, coerce invalid)
        df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce')

        # Now remove timezone (tz-aware → tz-naive)
        df['date_posted'] = df['date_posted'].dt.tz_localize(None)

        df['id'] = df['id'].astype(str)
        df['salary_min'] = pd.to_numeric(df['salary_min'], errors='coerce').fillna(0).astype(int)
        df['salary_max'] = pd.to_numeric(df['salary_max'], errors='coerce').fillna(0).astype(int)

        df = df.where(pd.notnull(df), None) # Convert NaN to None for SQL NULL

        print("✅ Success: Fetched and prepared job data.")
        return df


    except requests.exceptions.RequestException as e:
        print(f"❌ API Error: {e}")
        return None

def get_db_connection(config_path: str) -> pyodbc.Connection | None:
    """
    Reads database credentials from a config file and establishes a connection.

    Args:
        config_path: Path to the configuration file.

    Returns:
        A pyodbc connection object, or None if connection fails.
    """
    print("🚀 Starting: Connecting to SQL Server...")
    try:
        config = configparser.ConfigParser()
        config.read(config_path)
        db_config = config['database']

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};"
            f"Trusted_Connection=yes;"
        )
        conn = pyodbc.connect(conn_str)
        print("✅ Success: Connected to SQL Server.")
        return conn

    except (pyodbc.Error, configparser.Error, FileNotFoundError) as e:
        print(f"❌ DB Connection Error: {e}")
        return None

def load_data_to_sql(df: pd.DataFrame, conn: pyodbc.Connection):
    """
    Creates a table (if needed) and bulk-inserts data from a DataFrame into SQL Server.

    Args:
        df: The pandas DataFrame containing the data to load.
        conn: The active pyodbc connection object.
    """
    print("🚀 Starting: Loading data into SQL Server...")
    cursor = None
    try:
        cursor = conn.cursor()

        # Step 1: Create Table (if not exists) with improved data types
        create_table_query = """
        IF OBJECT_ID('RemoteOKJobs', 'U') IS NULL
        BEGIN
            CREATE TABLE RemoteOKJobs (
                id NVARCHAR(50) PRIMARY KEY,
                date_posted DATETIME2,
                company NVARCHAR(255),
                position NVARCHAR(255),
                location NVARCHAR(255),
                tags NVARCHAR(MAX),
                salary_min int,
                salary_max int,
                url NVARCHAR(MAX)
            )
        END
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("✅ Table checked/created successfully.")

        # Step 2: Use MERGE for an efficient "upsert" operation
        # This will update existing jobs and insert new ones.
        merge_query = """
        MERGE RemoteOKJobs AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (id, date_posted, company, position, location, tags, salary_min, salary_max, url)
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                target.date_posted = source.date_posted,
                target.company = source.company,
                target.position = source.position,
                target.location = source.location,
                target.tags = source.tags,
                target.salary_min = source.salary_min,
                target.salary_max = source.salary_max,
                target.url = source.url
        WHEN NOT MATCHED THEN
            INSERT (id, date_posted, company, position, location, tags, salary_min, salary_max, url)
            VALUES (source.id, source.date_posted, source.company, source.position, source.location, source.tags, source.salary_min, source.salary_max, source.url);
        """

        # Prepare data for executemany
        data_to_insert = [tuple(row) for row in df.itertuples(index=False)]

        # Set fast_executemany for significant performance boost
        cursor.fast_executemany = True
        cursor.executemany(merge_query, data_to_insert)
        conn.commit()

        print(f"✅ Success: {len(data_to_insert)} rows merged into the database.")

    except pyodbc.Error as e:
        print(f"❌ SQL Error during data load: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()

def main():
    """Main ETL process function."""
    API_URL = "https://remoteok.com/api"
    CONFIG_FILE = Path(__file__).parent / "config.ini"

    # EXTRACT
    jobs_df = fetch_remoteok_data(API_URL)
    if jobs_df is None or jobs_df.empty:
        print(" halting process.")
        return

    # LOAD
    conn = None
    try:
        conn = get_db_connection(CONFIG_FILE)
        if conn:
            load_data_to_sql(jobs_df, conn)
    finally:
        if conn:
            conn.close()
            print("🔒 SQL Server connection closed.")

if __name__ == "__main__":
    main()
