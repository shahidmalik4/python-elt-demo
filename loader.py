import logging
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv

# Import the core logic from previous days
from extractor import get_db_engine, extract_raw_data
from cleaner import clean_and_transform

load_dotenv()

logger = logging.getLogger('pipeline_logger')


def load_data(df_clean, engine):
    """
    Loads the final, clean DataFrame into the target PostgreSQL table.
    
    The table is set to 'analytics_sales', representing the final reporting layer.
    """
    TARGET_TABLE = "analytics_sales"
    logger.info(f"Starting Load phase: Preparing to load {len(df_clean)} records into '{TARGET_TABLE}'.")

    # 1. Ensure the target table is clean (overwrite strategy)
    try:
        with engine.connect() as connection:
            # TRUNCATE is used for speed; it removes all rows but keeps the table structure.
            truncate_sql = text(f"TRUNCATE TABLE {TARGET_TABLE};")
            connection.execute(truncate_sql)
            connection.commit()
            logger.info(f"Successfully truncated old data from {TARGET_TABLE}.")
    except Exception as e:
        # If the table doesn't exist yet, TRUNCATE will fail. We ignore this and let to_sql create it.
        logger.warning(f"Could not TRUNCATE {TARGET_TABLE}. It will be created on load if it doesn't exist. Error: {e}")
        
    # 2. Use Pandas to_sql for efficient bulk loading
    try:
        df_clean.to_sql(
            name=TARGET_TABLE,
            con=engine,
            if_exists='append', # Appends the new data to the (now empty or newly created) table
            index=False,
            method='multi' # Recommended for faster insertion
        )
        logger.info(f"Load SUCCESS: {len(df_clean)} records loaded into '{TARGET_TABLE}'.")
        
    except Exception as e:
        logger.critical(f"Load FAILED: Could not insert data into {TARGET_TABLE}. Error: {e}")
        raise RuntimeError("Load phase failed.")


def run_elt_pipeline():
    """Executes the full Extract, Transform, Load pipeline."""
    logger.info("--- Day 4: Full ELT Pipeline Start (E -> T -> L) ---")
    
    try:
        # --- E: Extract ---
        db_engine = get_db_engine()
        df_orders_raw, df_customers_raw = extract_raw_data(db_engine)
        
        # --- T: Transform ---
        df_final_clean = clean_and_transform(df_orders_raw, df_customers_raw)
        
        # --- L: Load ---
        load_data(df_final_clean, db_engine)
        
        logger.info("--- Full ELT Pipeline Run COMPLETE ---")
        
    except Exception as e:
        logger.critical(f"Pipeline halted due to a critical error: {e}")
        

if __name__ == "__main__":
    run_elt_pipeline()