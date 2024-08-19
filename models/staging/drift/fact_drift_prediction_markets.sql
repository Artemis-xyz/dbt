{{ config(materialized="table") }}
with
    trump_data as (
        select
            DATE_TRUNC('DAY', extraction_date)::date as date,
            source_json as trump_price
        from {{ source("PROD_LANDING", "raw_drift_fill_price_trump") }}
    ), kamala_data as (
        select
            DATE_TRUNC('DAY', extraction_date)::date as date,
            source_json as kamala_price
        from {{ source("PROD_LANDING", "raw_drift_fill_price_kamala") }}
    )

select
    trump_data.date,
    'drift' as app,
    'DeFi' as category,
    'solana' as chain,
    trump_price,
    kamala_price
from trump_data
left join kamala_data
    on trump_data.date = kamala_data.date
    
