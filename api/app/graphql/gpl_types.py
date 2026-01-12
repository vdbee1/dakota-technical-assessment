import strawberry

@strawberry.type
class Consumption:
    # --- Join Keys ---
    state: str            # Two-letter US State abbreviation (e.g., 'TX')
    period: str           # Historical month in YYYY-MM format (EIA reporting period)
    
    # --- Enrichment logic fields (Unique) ---
    peak_share: float     # Percentage of total energy consumed during peak demand hours (0.0 - 1.0)
    weather_index: float  # A proxy value (0.8 - 1.3) where >1.0 indicates extreme heating/cooling demand
    grid_stability_index: float  # Metric (0.0 - 1.0) representing grid reliability; lower values suggest higher outage risk
    renewable_share: float       # Percentage of the total load met by wind, solar, and hydro sources (0.0 - 1.0)

@strawberry.type
class Pricing:
    # --- Join Keys ---
    state: str            # Two-letter US State abbreviation
    period: str           # Historical month in YYYY-MM format
    
    # --- Enrichment logic fields (Unique) ---
    pricing_type: str      # Market structure classification: 'regulated' or 'deregulated'
    congestion_premium: float   # Additional cost per MWh caused by transmission bottlenecks ($/MWh)
    fuel_cost_index: float      # Relative price proxy (0.85 - 1.25) for raw generation fuel (Gas/Coal)
    volatility_score: float     # 0-100 index measuring price fluctuations; higher in deregulated markets
    carbon_intensity: float     # Estimated gCO2 emitted per kWh generated based on current fuel mix

@strawberry.type
class Health:
    status: str           # Basic service health status (e.g., 'ok')