import os
import logging
from faker import Faker
from sqlalchemy import create_engine, text
import pandas as pd
import random
from datetime import datetime, timedelta

# --- Configuration (Load from environment variables/defaults) ---
# NOTE: Update these defaults if your PostgreSQL setup uses different credentials.
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "test_db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5434")
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

NUM_CUSTOMERS = 100
NUM_ORDERS = 1000

# --- Setup ---
# Configure logging to show timestamps and severity level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
fake = Faker()

def create_database_engine():
    """Initializes and returns a SQLAlchemy engine."""
    try:
        engine = create_engine(DATABASE_URL)
        # Attempt to connect to verify the credentials
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logging.info("Successfully connected to PostgreSQL.")
        return engine
    except Exception as e:
        logging.error(f"Failed to connect to PostgreSQL at {POSTGRES_HOST}. Error: {e}")
        # Crucial to raise an exception to halt the script if the DB connection fails
        raise ConnectionError("Database connection failed. Check credentials, host, and port.")

def create_raw_tables(engine):
    """Drops existing tables and creates the raw tables with explicit DDL."""
    logging.info("Defining and creating raw tables in PostgreSQL.")
    
    # DDL for Raw Customers Table
    customers_ddl = """
    DROP TABLE IF EXISTS raw_customers;
    CREATE TABLE raw_customers (
        customer_key VARCHAR(10) PRIMARY KEY,
        customer_name VARCHAR(100) NOT NULL,
        region TEXT, 
        email_address VARCHAR(100)
    );
    """
    
    # DDL for Raw Orders Table
    orders_ddl = """
    DROP TABLE IF EXISTS raw_orders;
    CREATE TABLE raw_orders (
        order_id INTEGER, 
        customer_key VARCHAR(10), 
        raw_price NUMERIC, 
        quantity INTEGER,
        creation_timestamp TIMESTAMP WITHOUT TIME ZONE
    );
    """
    
    with engine.connect() as connection:
        # Execute DDL statements
        connection.execute(text(customers_ddl))
        connection.execute(text(orders_ddl))
        connection.commit()
    logging.info("Tables 'raw_customers' and 'raw_orders' created successfully.")

def generate_customers(engine):
    """Generates mock customer data and loads it into the 'raw_customers' table."""
    logging.info(f"Generating {NUM_CUSTOMERS} customer records.")
    
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        customer_key = f"CUST_{i:03d}"
        
        # Introduce a null region for quality check practice
        region = fake.country() if random.random() > 0.1 else None 
        
        customers.append({
            'customer_key': customer_key,
            'customer_name': fake.name(),
            'region': region,
            'email_address': fake.email()
        })
        
    df_customers = pd.DataFrame(customers)
    
    # Load into PostgreSQL using 'append' since the DDL created the table structure
    df_customers.to_sql('raw_customers', engine, if_exists='append', index=False)
    logging.info(f"Successfully loaded {len(df_customers)} records into 'raw_customers'.")
    
    # Return the list of keys for use in generating orders (to ensure most are valid FKs)
    return [c['customer_key'] for c in customers]

def generate_orders(engine, customer_keys):
    """Generates mock order data including intentional errors, and loads it into 'raw_orders' table."""
    logging.info(f"Generating {NUM_ORDERS} order records, including 'dirty' data.")

    orders = []
    
    for i in range(1, NUM_ORDERS + 1):
        # Intentional Data Quality Issue 1: Duplicate order_id (Pytest/Pandas check)
        order_id = 1000 + i if i != 50 else 1049 
        
        # Intentional Data Quality Issue 2: Bad foreign key (Data Integrity check)
        if i == 100:
             customer_key = "CUST_999" 
        else:
             customer_key = random.choice(customer_keys) 
        
        # Intentional Data Quality Issue 3: Negative price (Pydantic check)
        raw_price = round(random.uniform(10, 500) * (1 if i != 200 else -1), 2)
        
        # Intentional Data Quality Issue 4: Null quantity (Pandas cleaning/fill check)
        quantity = random.randint(1, 5) if random.random() > 0.05 else None
        
        orders.append({
            'order_id': order_id,
            'customer_key': customer_key,
            'raw_price': raw_price,
            'quantity': quantity,
            'creation_timestamp': fake.date_time_between(start_date="-30d", end_date="now")
        })
        
    df_orders = pd.DataFrame(orders)
    
    # Load into PostgreSQL using 'append'
    df_orders.to_sql('raw_orders', engine, if_exists='append', index=False)
    logging.info(f"Successfully loaded {len(df_orders)} records into 'raw_orders'.")


def main():
    """Main execution function to run the data generation pipeline."""
    logging.info("--- Data Source Generation Pipeline Start ---")
    try:
        # Step 1: Initialize DB connection
        engine = create_database_engine()
        
        # Step 2: Create the table structure first (DDL)
        create_raw_tables(engine)

        # Step 3: Generate and load customer data
        customer_keys = generate_customers(engine)
        
        # Step 4: Generate and load order data
        generate_orders(engine, customer_keys)
        
        logging.info("Data generation and loading complete. Raw tables are ready for ELT.")

    except ConnectionError as e:
        # Catches the database connection error raised in create_database_engine
        logging.error(f"Application terminated due to database connection failure: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during execution: {e}")
    
    logging.info("--- Data Source Generation Pipeline End ---")

if __name__ == "__main__":
    main()