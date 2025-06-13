{{ config(materialized="table") }}
select
    date,
    coingecko_id,
    shifted_token_price_usd,
    shifted_token_market_cap,
    shifted_token_h24_volume_usd,
    shifted_token_circulating_supply
from {{ ref("fact_coingecko_token_date_adjusted") }}
