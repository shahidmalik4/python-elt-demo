import logging
import pandas as pd
from pydantic import ValidationError
from extractor import extract_raw_data, get_db_engine
from schemas import CleanedOrder 

# Get the logger created in extractor.py
logger = logging.getLogger('pipeline_logger')


def clean_and_transform(df_orders, df_customers):
    """
    Performs merging, cleaning, transformation, and validation on raw data.
    """
    logger.info("Starting transformation: Merging and cleaning raw data.")

    df_merged = df_orders.merge(
        df_customers,
        on='customer_key',
        how='left',
        suffixes=('_order', '_customer')
    )
    logger.info(f"Merged DataFrames. Total rows: {len(df_merged)}")

    # Fix 1: Handle Duplicate Order IDs
    pre_dedupe_count = len(df_merged)
    df_merged = df_merged.drop_duplicates(subset=['order_id'], keep='first')
    deduped_count = pre_dedupe_count - len(df_merged)
    logger.info(f"Removed {deduped_count} duplicate rows based on 'order_id'.")
    
    # Fix 2: Handle Null Quantities
    df_merged['quantity'] = df_merged['quantity'].fillna(1).astype(int)
    logger.info("Imputed missing quantities with 1.")

    # Fix 3: Handle Null Regions (for Dimension data)
    df_merged['region'] = df_merged['region'].fillna('Unknown')
    logger.info("Imputed missing regions with 'Unknown'.")
    
    # Fix 4: Handle Bad Foreign Keys (FKs)
    df_merged['customer_name'] = df_merged['customer_name'].fillna('Invalid Customer')
    logger.info("Marked orders with bad foreign keys as 'Invalid Customer'.")

    # Clean the price and apply the validation fix (e.g., turning -10 into 10 or dropping it)
    df_merged['price_usd'] = df_merged['raw_price'].abs() 
    
    # Derive the key analytical metric: total sale amount
    df_merged['total_sale'] = df_merged['price_usd'] * df_merged['quantity']
    
    # Final column selection and renaming to match the Pydantic schema exactly
    df_clean = df_merged[[
        'order_id', 'customer_key', 'customer_name', 'region', 
        'price_usd', 'quantity', 'total_sale', 'creation_timestamp'
    ]].copy()
    
    logger.info("Completed cleaning and transformation. Proceeding to Pydantic validation.")
    

    # Convert DataFrame records to a list of dictionaries for validation
    records_to_validate = df_clean.to_dict('records')
    
    validated_records = []
    failed_validations = 0

    for record in records_to_validate:
        try:
            # Attempt to instantiate the Pydantic model
            CleanedOrder(**record)
            validated_records.append(record)
        except ValidationError as e:
            # If validation fails, log the error and skip the record
            failed_validations += 1
            # Log the specific record that failed validation
            logger.warning(f"Record failed Pydantic validation (ID: {record.get('order_id')}): {e.errors()[:1]}")

    if failed_validations > 0:
        logger.warning(f"Dropped {failed_validations} rows due to Pydantic validation errors.")
    
    # Convert validated records back into a clean DataFrame
    df_validated = pd.DataFrame(validated_records)
    
    logger.info(f"Transformation complete. Final clean rows: {len(df_validated)}")
    
    return df_validated


if __name__ == "__main__":
    logger.info("--- Day 3: Transformation Pipeline Start ---")
    
    try:
        # Step 1: Extract the raw data using the Day 2 function
        db_engine = get_db_engine()
        df_orders_raw, df_customers_raw = extract_raw_data(db_engine)
        
        # Step 2: Clean and Transform
        df_final_clean = clean_and_transform(df_orders_raw, df_customers_raw)
        
        logger.info("Transformation successful. Final DataFrame is ready.")
        
        # Display the output for inspection
        print("\n--- Final Clean & Validated DataFrame Head ---")
        print(df_final_clean.head())
        print(f"\nTotal Final Records: {len(df_final_clean)}")
        
    except Exception as e:
        logger.critical(f"Pipeline failed during Transformation stage: {e}")