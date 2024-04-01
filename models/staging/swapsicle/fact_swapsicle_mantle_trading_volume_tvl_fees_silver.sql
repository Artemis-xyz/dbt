with
    max_extraction as (
        select max(extraction_date) as max_date
        from
            {{ source("PROD_LANDING", "raw_swapsicle_mantle_trading_volume_tvl_fees") }}
    ),
    raw_data as (
        select
            to_date(value:"date"::timestamp) as date,
            value:"tvlUSD"::float as tvl,
            value:"feesUSD"::float as fees,
            value:"volumeUSD"::float as trading_volume
        from
            {{ source("PROD_LANDING", "raw_swapsicle_mantle_trading_volume_tvl_fees") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select
    date,
    'swapsicle' as app,
    'mantle' as chain,
    'DeFi' as category,
    trading_volume,
    tvl,
    fees
from raw_data
