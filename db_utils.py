# db_utils.py
import pyodbc
import configparser

def get_db_connection(db_config: dict) -> pyodbc.Connection | None:
    """
    Connect to SQL Server using details from a dictionary.
    """
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};"
            # Remove 'Trusted_Connection=yes;' if you use SQL Server authentication
            # If using SQL Server Authentication, include:
            # f"UID={db_config['username']};"
            # f"PWD={db_config['password']};"
            f"Trusted_Connection=yes;"  # Keep this if using Windows Authentication
        )

        conn = pyodbc.connect(conn_str)
        print("✅ Success: Connected to SQL Server.")
        return conn

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        if sqlstate == 'HY000': # Generic error, often related to driver or connection string
            print(f"❌ DB Connection Error: Could not connect. Check your connection string and driver. Details: {ex}")
        elif sqlstate == '28000': # Invalid authorization specification
            print(f"❌ DB Connection Error: Authentication failed. Check username and password. Details: {ex}")
        else:
            print(f"❌ DB Connection Error: An error occurred while connecting to the database. Details: {ex}")
        return None
    except KeyError as e:
        print(f"❌ DB Connection Error: Missing key in database configuration. Please ensure 'server', 'database', and 'username'/'password' (if applicable) are present. Missing key: {e}")
        return None
    except Exception as e:
        print(f"❌ DB Connection Error: An unexpected error occurred. Details: {e}")
        return None