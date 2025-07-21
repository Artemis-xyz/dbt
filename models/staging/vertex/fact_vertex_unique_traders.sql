{{ config(materialized="table", enabled=false) }}
with
    unique_traders_data as (
        select
            date_trunc('day', block_timestamp) as date,
            count(distinct(trader)) as unique_traders
        from arbitrum_flipside.vertex.ez_perp_trades
        group by 1
        order by 1
    ),
    results as (
        select
            'arbitrum' as chain,
            date,
            unique_traders,
            'vertex' as app,
            'DeFi' as category
        from unique_traders_data
    )

select chain, app, category, date, unique_traders
from results
