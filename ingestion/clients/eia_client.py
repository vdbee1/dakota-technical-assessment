import requests
import time
from utils import logger, retry_on_failure

class EIAClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.eia.gov/v2/electricity/retail-sales/data/"

    @retry_on_failure(retries=3)
    def get_batch(self, state: str, offset: int = 0, length: int = 10):
        # We request all 4 available metrics: sales, price, revenue, customers
        query_params = (
            f"api_key={self.api_key}"
            f"&frequency=monthly"
            f"&data[0]=sales"
            f"&data[1]=price"
            f"&data[2]=revenue"
            f"&data[3]=customers"
            f"&facets[stateid][]={state.upper()}"
            f"&facets[sectorid][]=RES"  # Still filtering to RES for data consistency
            f"&sort[0][column]=period"
            f"&sort[0][direction]=desc"
            f"&offset={offset}"
            f"&length={length}"
        )
        
        full_url = f"{self.base_url}?{query_params}"
        logger.info(f" Fetching ALL EIA fields for state: {state}")
        
        response = requests.get(full_url, timeout=30)
        response.raise_for_status()
        return response.json()['response']['data']