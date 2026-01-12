with enriched_data as (
    select * from {{ ref('int_energy_enriched') }}
),
state_metadata as (
    select * from {{ source('raw_api', 'dim_states') }}
)

select
    d.period,
    d.state_code,
    s.state_name,
    s.census_region,
    s.market_type,
    d.price_cents_kwh,
    d.sales_mwh,
    d.carbon_intensity,
    d.weather_index,
    d.renewable_share,
    case 
        when d.carbon_intensity > 0 then (d.price_cents_kwh / d.carbon_intensity) 
        else null 
    end as price_to_carbon_ratio
from enriched_data d
left join state_metadata s on d.state_code = s.stateid