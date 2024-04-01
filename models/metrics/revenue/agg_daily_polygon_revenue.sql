{{ config(materialized="table") }}

with
    prices as ({{ get_coingecko_price_with_latest("matic-network") }}),
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
    coalesce(native_token_burn * price, 0) as revenue
from chain_rev
left join prices on chain_rev.date = prices.date
order by date desc
