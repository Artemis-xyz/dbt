{{ config(materialized="table") }}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_cardano_nft_trading_volume") }}
    )
select
    to_date(value:"date"::string) as date,
    value:"totalPriceUSD"::float as nft_trading_volume,
    'cardano' as chain
from
    {{ source("PROD_LANDING", "raw_cardano_nft_trading_volume") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
