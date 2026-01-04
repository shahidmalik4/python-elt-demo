import pandas as pd
import numpy as np
from datetime import datetime
from src.schemas import CleanedOrder
import logging


logger = logging.getLogger(__name__)

if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

def clean_and_transform(df_orders: pd.DataFrame, df_customers: pd.DataFrame) -> pd.DataFrame:

    logger.info("Starting transformation: Merging and cleaning raw data.")

    df_merged = pd.merge(
        df_orders, 
        df_customers, 
        left_on='customer_key', 
        right_on='customer_key', 
        how='left'
    )
    logger.info(f"Merged DataFrames. Total rows: {len(df_merged)}")
    
    initial_rows = len(df_merged)
    df_merged.drop_duplicates(subset=['order_id'], keep='first', inplace=True)
    logger.info(f"Removed {initial_rows - len(df_merged)} duplicate rows based on 'order_id'.")
    
    df_merged['quantity'].fillna(1, inplace=True)
    logger.info("Imputed missing quantities with 1.")

    df_merged['region'].fillna('Unknown', inplace=True)
    logger.info("Imputed missing regions with 'Unknown'.")

    df_merged['customer_name'].fillna('Invalid Customer', inplace=True)
    logger.info("Marked orders with bad foreign keys as 'Invalid Customer'.")

    df_merged['price_usd'] = df_merged['raw_price'].abs()

    df_merged['total_sale'] = df_merged['price_usd'] * df_merged['quantity']
    
    df_merged['creation_timestamp'] = datetime.now()
    logger.info("Added 'creation_timestamp' metadata column.")
    
    df_clean = df_merged[[
        'order_id', 'customer_key', 'customer_name', 'region', 
        'price_usd', 'quantity', 'total_sale', 'creation_timestamp'
    ]].copy()
    
    validated_records = []
    rejected_count = 0
    
    for index, row in df_clean.iterrows():
        try:
            # Validate against the CleanedOrder schema
            CleanedOrder(**row.to_dict())
            validated_records.append(row)
        except Exception as e:
            # Log rejected record and skip
            logger.warning(f"Rejected record (Order ID: {row['order_id']}) due to Pydantic violation: {e}")
            rejected_count += 1
            
    if rejected_count > 0:
        logger.warning(f"Rejected {rejected_count} record(s) during Pydantic validation.")
        
    df_validated = pd.DataFrame(validated_records)
    logger.info(f"Transformation complete. Cleaned and validated {len(df_validated)} records.")

    return df_validated
