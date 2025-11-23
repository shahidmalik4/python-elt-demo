import os
import logging
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text


# Load environment variables from .env file
load_dotenv() 

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

# Construct the database URL
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_db_engine():
    """Initializes and returns a SQLAlchemy engine, handling connection errors."""
    logging.info("Attempting to connect to PostgreSQL...")
    try:
        engine = create_engine(DATABASE_URL)
        # Verify connection by running a simple query
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logging.info("Database connection verified.")
        return engine
    except Exception as e:
        logging.critical(f"Failed to connect to PostgreSQL. Check your .env file and Docker status. Error: {e}")
        # Raise an error to halt the entire pipeline if extraction cannot proceed
        raise ConnectionError("Database extraction failed.")


def extract_raw_data(engine):
    """
    Extracts data from the raw_customers and raw_orders tables 
    and returns them as a tuple of Pandas DataFrames.
    """
    logging.info("Starting raw data extraction from database tables.")
    
    # SQL Queries to pull the entire contents of the raw tables
    # We pull both tables because we need to MERGE them in the next step (Transformation)
    query_orders = "SELECT * FROM raw_orders;"
    query_customers = "SELECT * FROM raw_customers;"

    try:
        # Use pd.read_sql() to efficiently execute SQL and return a DataFrame
        df_orders = pd.read_sql(query_orders, engine)
        df_customers = pd.read_sql(query_customers, engine)
        
        logging.info(f"Extracted {len(df_orders)} records from 'raw_orders'.")
        logging.info(f"Extracted {len(df_customers)} records from 'raw_customers'.")
        
        return df_orders, df_customers
        
    except Exception as e:
        logging.critical(f"Extraction failed during SQL query execution. Error: {e}")
        raise RuntimeError("Extraction query failed.")


if __name__ == "__main__":
    # This block allows you to test the script independently
    try:
        db_engine = get_db_engine()
        orders_df, customers_df = extract_raw_data(db_engine)
        
        logging.info("Extraction successful. DataFrames are loaded into memory.")
        # Print the head of the dataframes to quickly verify the data quality issues
        print("\n--- Orders DataFrame Head (Check for errors like negative price/nulls) ---")
        print(orders_df.head(10))
        print("\n--- Customers DataFrame Head (Check for null regions) ---")
        print(customers_df.head())
        
    except (ConnectionError, RuntimeError):
        # The exception handlers inside the functions already logged the error
        print("\nExtraction script halted due to critical error. See logs above.")