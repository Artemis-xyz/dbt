with
    data as (
        select
            partition_date,
            max_by(source_json, extraction_date) as source_json,
            max_by(source_url, extraction_date) as source_url
        from {{ source("PROD_LANDING", "raw_dydx_unique_traders") }}
        group by partition_date
    )

select
    source_json::number as unique_traders,
    'dydx' as app,
    null as chain,
    'DeFi' as category,
    date(partition_date) as date
from data
