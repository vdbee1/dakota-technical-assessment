{{ config(
    materialized='incremental',
    unique_key=['period', 'state_code']
) }}

with eia as (
    select * from {{ ref('stg_eia_energy') }}
),
synth as (
    select * from {{ ref('stg_synth_metrics') }}
)

select
    eia.period,
    eia.state_code,
    eia.price_cents_kwh,
    eia.sales_mwh,
    synth.carbon_intensity,
    synth.weather_index,
    synth.renewable_share,
    synth.grid_stability_index,
    synth.volatility_score,
    -- Audit column for incremental logic
    greatest(eia.ingested_at, synth.ingested_at) as last_updated_at
from eia
left join synth 
    on eia.period = synth.period 
    and eia.state_code = synth.state_code

{% if is_incremental() %}
  where last_updated_at > (select max(last_updated_at) from {{ this }})
{% endif %}