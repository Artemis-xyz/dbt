{{ config(materialized="table") }}
with
    tagged_coingecko_id as (
        select distinct coingecko_id
        from {{ ref("dim_chain") }}
        union
        select distinct coingecko_id
        from {{ ref("dim_all_apps_gold") }}
    )
select
    date,
    coingecko_id,
    shifted_token_price_usd,
    shifted_token_market_cap,
    shifted_token_h24_volume_usd
from {{ ref("fact_coingecko_token_date_adjusted_gold") }}
where coingecko_id != all (select coingecko_id from tagged_coingecko_id)
