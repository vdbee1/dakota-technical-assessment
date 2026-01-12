import strawberry
import logging
from typing import List
from datetime import datetime

from app.graphql.gpl_types import Consumption, Pricing, Health
from app.services.synthetic import (
    generate_consumption,
    generate_pricing
)

# Setup logging to capture errors on the server side
logger = logging.getLogger(__name__)

@strawberry.type
class Query:
    @strawberry.field(description="Fetches synthetic consumption data mapped to EIA historical periods.")
    def consumption(self, state: str, months: int = 1) -> List[Consumption]:
        # Validation for Orchestration
        if months < 1: raise ValueError("Months must be at least 1")
        if len(state) != 2: raise ValueError("Use 2-letter state code")

        try:
            # i=0 is 3 months ago, i=1 is 4 months ago, etc.
            return [
                Consumption(**generate_consumption(state, months_back=i))
                for i in range(months)
            ]
        except Exception as e:
            logger.error(f"GraphQL Resolver Error: {e}")
            raise Exception("Internal Synthetic Engine Error")

    @strawberry.field(description="Fetches synthetic pricing data mapped to EIA historical periods.")
    def pricing(self, state: str, months: int = 1) -> List[Pricing]:
        try:
            return [
                Pricing(**generate_pricing(state, months_back=i))
                for i in range(months)
            ]
        except Exception as e:
            logger.error(f"GraphQL Resolver Error: {e}")
            raise Exception("Internal Synthetic Engine Error")