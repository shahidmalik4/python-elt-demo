import os
import logging
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
import sys

load_dotenv() 

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")


DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Create a custom logger named 'pipeline_logger'
logger = logging.getLogger('pipeline_logger')
logger.setLevel(logging.INFO)

# Define the format for all log messages
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 1. Console Handler (for real-time terminal output)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# 2. File Handler (for persistent log storage)
# Writes all logs (INFO level and above) to a file named 'pipeline.log'
file_handler = logging.FileHandler('pipeline.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Add both handlers to the custom logger
if not logger.handlers:
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def get_db_engine():
    logger.info("Attempting to connect to PostgreSQL...")
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("Database connection verified.")
        return engine
    except Exception as e:
        logger.critical(f"Failed to connect to PostgreSQL. Check your .env file and Docker status. Error: {e}")
        raise ConnectionError("Database extraction failed.")


def extract_raw_data(engine):

    logger.info("Starting raw data extraction from database tables.")
    
    query_orders = "SELECT * FROM raw_orders;"
    query_customers = "SELECT * FROM raw_customers;"

    try:
        df_orders = pd.read_sql(query_orders, engine)
        df_customers = pd.read_sql(query_customers, engine)
        
        logger.info(f"Extracted {len(df_orders)} records from 'raw_orders'.")
        logger.info(f"Extracted {len(df_customers)} records from 'raw_customers'.")
        
        return df_orders, df_customers
        
    except Exception as e:
        logger.critical(f"Extraction failed during SQL query execution. Error: {e}")
        raise RuntimeError("Extraction query failed.")


if __name__ == "__main__":
    try:
        db_engine = get_db_engine()
        orders_df, customers_df = extract_raw_data(db_engine)
        
        logger.info("Extraction successful. DataFrames are loaded into memory.")
        
        print("\n--- Orders DataFrame Head (Check for errors like negative price/nulls) ---")
        print(orders_df.head(10))
        print("\n--- Customers DataFrame Head (Check for null regions) ---")
        print(customers_df.head())
        
    except (ConnectionError, RuntimeError):
        logger.info("\nExtraction script halted due to critical error.")
