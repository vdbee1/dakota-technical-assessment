from pydantic import BaseModel
from typing import Optional

class EIARecord(BaseModel):
    stateid: str
    stateDescription: str
    period: str
    sectorid: str
    sales: float
    price: float
    revenue: float
    customers: float
    unit: str
    source: str = "EIA"

class SyntheticRecord(BaseModel):
    stateid: str
    period: str
    # Unique Enrichment Fields
    peak_share: Optional[float]           # % usage during peak hours
    weather_index: Optional[float]        # Proxy for heating/cooling demand
    grid_stability_index: Optional[float] # Reliability metric
    renewable_share: Optional[float]      # % of load from renewables
    pricing_type: Optional[str]           # Regulated vs Deregulated
    congestion_premium: Optional[float]   # Price adder for bottlenecks
    fuel_cost_index: Optional[float]      # Fuel price proxy
    volatility_score: Optional[float]     # Market stability index
    carbon_intensity: Optional[float]     # gCO2 per kWh
    source: str = "synthetic"