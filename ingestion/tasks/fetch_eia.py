import requests
import os
from utils import logger

def ingest_eia_for_state(api_key: str, state: str, length: int = 120):
    """
    Fetches historical retail electricity price and sales data for a specific state.
    Uses EIA V2 API for 50-state compatibility.
    """
    # The V2 Retail Sales endpoint is the most consistent for all US states
    url = "https://api.eia.gov/v2/electricity/retail-sales/data/"
    
    params = {
        "api_key": api_key,
        "frequency": "monthly",
        "data[0]": "price",
        "data[1]": "sales",
        "facets[stateid][]": state,
        "facets[sectorid][]": "RES", # Residential sector is best for benchmark
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": length
    }

    try:
        response = requests.get(url, params=params, timeout=20)
        
        if response.status_code == 403:
            logger.error("EIA API Key Invalid or Forbidden.")
            return []
            
        response.raise_for_status()
        json_data = response.json()
        
        records = json_data.get('response', {}).get('data', [])
        
        if not records:
            logger.warning(f"EIA API returned 0 records for state: {state}")
            return []

        # Standardize: Ensure every record has a 'source' and 'stateid'
        for r in records:
            r['source'] = 'EIA'
            # API V2 already returns 'stateid', but we ensure it's present
            if 'stateid' not in r:
                r['stateid'] = state

        logger.info(f"Successfully fetched {len(records)} EIA records for {state}")
        return records

    except Exception as e:
        logger.error(f"Failed to fetch EIA data for {state}: {e}")
        return []