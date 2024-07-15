{{ config(materialized="table") }}

select
    chain,
    app,
    'DeFi' as category,
    pool_type,
    registration_address,
    pool_address,
    token,
    amplification_coefficient,
    name,
    symbol,
    swap_fee,
    admin_fee,
    mid_fee,
    out_fee,
    coin_0,
    coin_1,
    coin_2,
    coin_3,
    underlying_coin_0,
    underlying_coin_1,
    underlying_coin_2,
    underlying_coin_3
from {{ ref("dim_curve_pools") }}
