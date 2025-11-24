from pydantic import BaseModel, field_validator
from typing import Optional
from datetime import datetime


class CleanedOrder(BaseModel):
    """
    Defines the contract for the final, clean combined order record.
    This schema will validate the structure, types, and business logic.
    """
    order_id: int
    customer_key: str
    customer_name: str
    region: Optional[str]
    
    # price_usd is the cleaned, non-negative version of raw_price
    price_usd: float 
    quantity: int
    
    # Derived column: The total sale amount
    total_sale: float 
    
    creation_timestamp: datetime

    # Validator 1: Ensure Price is Non-Negative
    @field_validator('price_usd')
    @classmethod
    def check_price_non_negative(cls, v):
        if v < 0:
            raise ValueError("Price USD must be non-negative.")
        return v
    
    # Validator 2: Ensure ID is Positive
    @field_validator('order_id')
    @classmethod
    def check_order_id_positive(cls, v):
        if v <= 1000: # Assuming order IDs start above a certain number
            raise ValueError("Order ID must be positive and greater than 1000.")
        return v