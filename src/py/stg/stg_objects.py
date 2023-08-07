from datetime import datetime 
from pydantic import BaseModel 
 
 
class transactions(BaseModel): 
    operation_id: str 
    account_number_from: int 
    account_number_to: int 
    currency_code: int
    country: str
    status: str
    transaction_type: str
    amount: int
    transaction_dt: datetime
 
 
class currencies(BaseModel): 
    date_update: datetime
    currency_code: int 
    currency_code_with: int
    currency_with_div: float