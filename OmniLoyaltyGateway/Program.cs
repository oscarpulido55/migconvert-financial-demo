from fastapi import FastAPI
from omnibus_loyalty_gateway.bigquery_data_access import BigQueryLoyaltyRepository
import uvicorn

app = FastAPI()

def get_bigquery_loyalty_repository():
    return BigQueryLoyaltyRepository()

uvicorn.run(app, host="0.0.0.0", port=8000)
