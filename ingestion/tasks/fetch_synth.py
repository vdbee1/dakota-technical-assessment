from clients.synth_client import SynthClient
from utils import logger

# Ensure this URL matches your local docker setup
client = SynthClient(api_url="http://localhost:8000/graphql")

def ingest_synth_for_state(state: str, months: int):
    """
    Calls the synthetic API once to get a batch of historical records.
    """
    try:
        # Request the full range of months in one batch
        records = client.get_data(state=state, month_count=months)
        
        if records:
            logger.info(f"Successfully fetched {len(records)} synth records for {state}")
            return records
        
        return []
                
    except Exception as e:
        logger.error(f"Error fetching synthetic data for {state}: {e}")
        return []