with source as (
    select * from {{ source('raw_api', 'eia_energy') }}
)

select
    (period || '-01')::DATE as period,
    stateid as state_code,
    price as price_cents_kwh,
    sales as sales_mwh,
    sectorid,
    ingested_at
from source