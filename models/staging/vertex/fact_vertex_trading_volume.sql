{{ config(materialized="table", enabled=false) }}
with
    trading_volume_data as (
        select
            date_trunc('day', block_timestamp) as date,
            sum(amount_usd) as trading_volume
        from arbitrum_flipside.vertex.ez_perp_trades
        where is_taker = 'TRUE'
        group by 1
        order by 1
    ),
    results as (
        select
            'arbitrum' as chain,
            date,
            trading_volume,
            'vertex' as app,
            'DeFi' as category
        from trading_volume_data
    )

select chain, app, category, date, trading_volume
from results
