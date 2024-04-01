{{ config(materialized="table") }}

select
    date,
    coingecko_nft_id,
    nft_floor_price_usd,
    nft_floor_price_native,
    nft_h24_volume_usd,
    nft_h24_volume_native,
    nft_market_cap_usd,
    nft_market_cap_native
from {{ ref("fact_coingecko_nft_price_mc_vol") }}
