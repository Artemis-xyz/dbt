with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_stellar_rwa_tvl") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_stellar_rwa_tvl") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            f.value:date::date as date,
            f.value:totalLiquidityUSD::float AS rwa_tvl
        from latest_data, lateral flatten(input => data) as f
    )
select date, rwa_tvl, 'stellar' as chain
from flattened_data
where date < to_date(sysdate())
order by date desc
