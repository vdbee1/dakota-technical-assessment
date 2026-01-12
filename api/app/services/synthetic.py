import random
from datetime import datetime
from dateutil.relativedelta import relativedelta

PRICING_TYPES = ["regulated", "deregulated"]

def get_historical_period(months_back: int):
    target_date = datetime.utcnow() - relativedelta(months=3 + months_back)
    return target_date.strftime("%Y-%m")

def generate_consumption(state: str, months_back: int = 0):
    weather_index = round(random.uniform(0.8, 1.3), 2)
    peak_share = round(random.uniform(0.25, 0.45), 2)
    
    # Logic: Renewable share simulation
    base_renewables = 0.4 if state.upper() in ["CA", "OR", "WA", "TX"] else 0.15
    renewable_share = round(base_renewables + random.uniform(0, 0.2), 2)
    grid_stability = round(random.uniform(0.85, 0.99), 3)

    return {
        "state": state.upper(),
        "period": get_historical_period(months_back),
        "peak_share": peak_share,
        "weather_index": weather_index,
        "renewable_share": renewable_share,
        "grid_stability_index": grid_stability
    }

def generate_pricing(state: str, months_back: int = 0):
    pricing_type = random.choice(PRICING_TYPES)
    fuel_cost_index = round(random.uniform(0.85, 1.25), 2)
    
    # Volatility logic based on market type
    volatility = random.uniform(40, 90) if pricing_type == "deregulated" else random.uniform(5, 25)
    carbon_intensity = round(fuel_cost_index * 400 + random.uniform(-50, 50), 2)

    congestion_premium = (
        round(random.uniform(5, 20), 2) if pricing_type == "deregulated" 
        else round(random.uniform(0, 5), 2)
    )

    return {
        "state": state.upper(),
        "period": get_historical_period(months_back),
        "pricing_type": pricing_type,
        "congestion_premium": congestion_premium,
        "fuel_cost_index": fuel_cost_index,
        "volatility_score": round(volatility, 2),
        "carbon_intensity": carbon_intensity
    }