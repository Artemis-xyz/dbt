{{ config(materialized="table") }}

with
    matic_prices as ({{ get_coingecko_price_with_latest("matic-network") }}),
    pol_prices as ({{ get_coingecko_price_with_latest("polygon-ecosystem-token") }}),
    chain_rev as (
        select
            date_trunc('day', block_timestamp) date,
            sum(block_burn) / 1e18 as native_token_burn
        from {{ ref("fact_polygon_blocks") }}
        group by date
    )
select
    'polygon' as chain,
    chain_rev.date as date,
    coalesce(native_token_burn, 0) as native_token_burn,
    CASE
        WHEN chain_rev.date < '2024-09-04' THEN coalesce(native_token_burn * matic_prices.price, 0) 
        WHEN chain_rev.date >= '2024-09-04' THEN coalesce(native_token_burn * pol_prices.price, 0)
    END as revenue
from chain_rev
left join matic_prices on chain_rev.date = matic_prices.date
left join pol_prices on chain_rev.date = pol_prices.date
order by date desc
