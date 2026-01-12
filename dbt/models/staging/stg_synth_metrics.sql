with source as (
    select * from {{ source('raw_api', 'synth_metrics') }}
)

select
    (period || '-01')::DATE as period,
    stateid as state_code,
    carbon_intensity,
    weather_index,
    grid_stability_index,
    renewable_share,
    volatility_score,
    ingested_at
from source