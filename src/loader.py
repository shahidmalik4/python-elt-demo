import logging
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv

from extractor import get_db_engine, extract_raw_data
from cleaner import clean_and_transform

load_dotenv()

logger = logging.getLogger('pipeline_logger')


def load_data(df_clean, engine):

    TARGET_TABLE = "analytics_sales"
    logger.info(f"Starting Load phase: Preparing to load {len(df_clean)} records into '{TARGET_TABLE}'.")

    # 1. Ensure the target table is clean (overwrite strategy)
    try:
        with engine.connect() as connection:
            # TRUNCATE is used for speed;
            truncate_sql = text(f"TRUNCATE TABLE {TARGET_TABLE};")
            connection.execute(truncate_sql)
            connection.commit()
            logger.info(f"Successfully truncated old data from {TARGET_TABLE}.")
    except Exception as e:
        logger.warning(f"Could not TRUNCATE {TARGET_TABLE}. It will be created on load if it doesn't exist. Error: {e}")
        
    # 2. Use Pandas to_sql for efficient bulk loading
    try:
        df_clean.to_sql(
            name=TARGET_TABLE,
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        logger.info(f"Load SUCCESS: {len(df_clean)} records loaded into '{TARGET_TABLE}'.")
        
    except Exception as e:
        logger.critical(f"Load FAILED: Could not insert data into {TARGET_TABLE}. Error: {e}")
        raise RuntimeError("Load phase failed.")


def run_elt_pipeline():
    logger.info("--- Full ELT Pipeline Start (E -> T -> L) ---")
    
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
