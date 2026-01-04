import os
import logging
from faker import Faker
from sqlalchemy import create_engine, text
import pandas as pd
import random
from datetime import datetime, timedelta

POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "warehouse")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

NUM_CUSTOMERS = 100
NUM_ORDERS = 1000

# Configure logging to show timestamps and severity level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
fake = Faker()

def create_database_engine():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logging.info("Successfully connected to PostgreSQL.")
        return engine
    except Exception as e:
        logging.error(f"Failed to connect to PostgreSQL at {POSTGRES_HOST}. Error: {e}")
        raise ConnectionError("Database connection failed. Check credentials, host, and port.")

def create_raw_tables(engine):
    logging.info("Defining and creating raw tables in PostgreSQL.")
    
    customers_ddl = """
    DROP TABLE IF EXISTS raw_customers;
    CREATE TABLE raw_customers (
        customer_key VARCHAR(10) PRIMARY KEY,
        customer_name VARCHAR(100) NOT NULL,
        region TEXT, 
        email_address VARCHAR(100)
    );
    """

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
        connection.execute(text(customers_ddl))
        connection.execute(text(orders_ddl))
        connection.commit()
    logging.info("Tables 'raw_customers' and 'raw_orders' created successfully.")

def generate_customers(engine):
    logging.info(f"Generating {NUM_CUSTOMERS} customer records.")
    
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        customer_key = f"CUST_{i:03d}"
        
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
    
    return [c['customer_key'] for c in customers]

def generate_orders(engine, customer_keys):
    logging.info(f"Generating {NUM_ORDERS} order records, including 'dirty' data.")

    orders = []
    
    for i in range(1, NUM_ORDERS + 1):
        order_id = 1000 + i if i != 50 else 1049 

        if i == 100:
             customer_key = "CUST_999" 
        else:
             customer_key = random.choice(customer_keys) 

        raw_price = round(random.uniform(10, 500) * (1 if i != 200 else -1), 2)

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
        logging.error(f"Application terminated due to database connection failure: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during execution: {e}")
    
    logging.info("--- Data Source Generation Pipeline End ---")

if __name__ == "__main__":
    main()
