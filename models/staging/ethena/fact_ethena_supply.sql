with max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_ethena_supply_data") }}
)

, date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date >='2024-04-01'
)

, supply_data_monthly as (
    select
        date_trunc('month', value:date::date) as month,
        value:period::number as period,
        value:foundation_allocation::float as foundation_allocation,
        value:ecosystem_development_allocation::float as ecosystem_development_allocation,
        value:investors_allocation::float as investors_allocation,
        value:circulating_supply::float as circulating_supply_native,
        value:circulating_ratio::float as circulating_ratio,
    from
    {{ source("PROD_LANDING", "raw_ethena_supply_data") }},
        lateral flatten(input => parse_json(source_json))
    where extraction_date = (select max_date from max_extraction)
)

select
    ds.date as date,
    coalesce(supply_data_monthly.foundation_allocation, 2250000000) as foundation_allocation,
    coalesce(supply_data_monthly.ecosystem_development_allocation, 4500000000) as ecosystem_development_allocation,
    coalesce(supply_data_monthly.investors_allocation, 3750000000) as investors_allocation,
    coalesce(supply_data_monthly.circulating_supply_native, 15000000000) as circulating_supply_native,
    coalesce(supply_data_monthly.circulating_ratio, 1) as circulating_ratio
from supply_data_monthly
full outer join date_spine as ds 
    on date_trunc('month', ds.date) = month