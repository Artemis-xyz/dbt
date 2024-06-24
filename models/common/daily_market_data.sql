{{
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
    )
}}

select 
    date,
    coingecko_id,
    shifted_token_price_usd as price,
    shifted_token_market_cap as market_cap,
    shifted_token_h24_volume_usd as h24_volume
from {{ source("PC_DBT_DB_UPSTREAM", "fact_coingecko_token_date_adjusted_gold") }} as fact_coingecko_token_date_adjusted_gold