{{ config(materialized="table", snowflake_warehouse="IMMUTABLE_X") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_immutable_x_nft_trading_volume") }}
    )
select
    to_date(value:"date"::string) as date,
    value:"totalPriceUSD"::float as nft_trading_volume,
    'immutable_x' as chain
from
    {{ source("PROD_LANDING", "raw_immutable_x_nft_trading_volume") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
