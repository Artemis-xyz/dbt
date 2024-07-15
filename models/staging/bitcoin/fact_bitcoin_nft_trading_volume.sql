{{ config(materialized="table") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_bitcoin_nft_trading_volume") }}
    )
select
    to_date(value:"date"::string) as date,
    value:"nft_trading_volume"::float as nft_trading_volume,
    'bitcoin' as chain
from
    {{ source("PROD_LANDING", "raw_bitcoin_nft_trading_volume") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
