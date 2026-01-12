import requests
from utils import logger

class SynthClient:
    def __init__(self, api_url: str):
        self.api_url = api_url

    def get_data(self, state: str, month_count: int):
        query = """
        query GetEnrichedData($state: String!, $monthInt: Int!) {
            consumption(state: $state, months: $monthInt) {
                state
                period
                peak_share
                weather_index
                grid_stability_index
                renewable_share
            }
            pricing(state: $state, months: $monthInt) {
                state
                period
                pricing_type
                congestion_premium
                fuel_cost_index
                volatility_score
                carbon_intensity
            }
        }
        """
        variables = {
            "state": state,
            "monthInt": month_count
        }

        try:
            response = requests.post(
                self.api_url, 
                json={'query': query, 'variables': variables},
                timeout=15
            )
            
            if response.status_code == 429:
                raise Exception("Status 429: Rate limit exceeded")
                
            response.raise_for_status()
            result = response.json()
            
            if "errors" in result:
                raise Exception(f"GraphQL Error: {result['errors'][0]['message']}")
            
            data = result.get('data', {})
            cons_list = data.get('consumption', [])
            price_list = data.get('pricing', [])
            
            # Merge and Standardize keys
            merged_records = []
            for c, p in zip(cons_list, price_list):
                # Standardize 'state' key to 'stateid' to match EIA V2 schema
                record = {**c, **p}
                if 'state' in record:
                    record['stateid'] = record.pop('state')
                merged_records.append(record)
            
            return merged_records
            
        except Exception as e:
            logger.error(f"Synth API error for {state}: {e}")
            raise e