import pytest
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
TARGET_TABLE = "analytics_sales"


@pytest.fixture(scope="module")
def db_engine():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    return engine

@pytest.fixture(scope="module")
def analytics_data(db_engine):

    query = f"SELECT * FROM {TARGET_TABLE};"
    df = pd.read_sql(query, db_engine)
    return df

# --- Data Quality Tests ---

def test_01_final_table_exists(db_engine):
    """Test that the target table was successfully created/loaded."""
    table_check = text(f"SELECT to_regclass('{TARGET_TABLE}');")
    with db_engine.connect() as connection:
        result = connection.execute(table_check).scalar()
        assert result is not None, f"The table {TARGET_TABLE} does not exist in the database."

def test_02_no_negative_prices(analytics_data):
    """Test that the transformation successfully removed/fixed all negative prices."""
    negative_prices = analytics_data[analytics_data['price_usd'] < 0]
    assert len(negative_prices) == 0, \
        f"Found {len(negative_prices)} rows with negative prices, transformation failed."

def test_03_no_duplicate_order_ids(analytics_data):
    """Test that the deduplication step worked correctly on the primary key."""
    assert not analytics_data['order_id'].duplicated().any(), \
        "Duplicate order IDs were found in the final analytics table."

def test_04_data_integrity_bad_fk_handled(analytics_data):
    """Test that the foreign key issue was handled by marking the customer."""
    invalid_customer_count = analytics_data[
        analytics_data['customer_name'] == 'Invalid Customer'
    ]
    assert len(invalid_customer_count) == 1, \
        f"Expected 1 'Invalid Customer' record, found {len(invalid_customer_count)}."

def test_05_derived_column_accuracy(analytics_data):
    """Test that the derived total_sale metric is correctly calculated."""
    calculated_sale = analytics_data['price_usd'] * analytics_data['quantity']
    
    # Calculate the difference between calculated and loaded values
    comparison = (calculated_sale - analytics_data['total_sale']).abs()
    
    # Assert that the maximum difference is negligible (due to float precision)
    assert comparison.max() < 0.001, \
        "Derived column 'total_sale' calculation is incorrect."