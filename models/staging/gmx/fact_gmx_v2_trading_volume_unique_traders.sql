{{ config(materialized="table") }}
with
    gmx_v2_by_chain as (
        select
            block_timestamp::date as date,
            sum(decoded_log:"eventData"[1][0][12][1]::number / 1e30) trading_volume,
            count(distinct(decoded_log:"eventData"[0][0][0][1])) unique_traders,
            'arbitrum' as chain,
            'gmx_v2' as app,
            'DeFi' as category
        from arbitrum_flipside.core.ez_decoded_event_logs
        where
            lower(contract_address)
            = lower('0xC8ee91A54287DB53897056e12D9819156D3822Fb')
            and (
                decoded_log:"eventName" = 'PositionDecrease'
                or decoded_log:"eventName" = 'PositionIncrease'
            )
        group by 1
        union
        select
            block_timestamp::date as date,
            sum(decoded_log:"eventData"[1][0][12][1]::number / 1e30) trading_volume,
            count(distinct(decoded_log:"eventData"[0][0][0][1])) unique_traders,
            'avalanche' as chain,
            'gmx_v2' as app,
            'DeFi' as category
        from avalanche_flipside.core.ez_decoded_event_logs
        where
            lower(contract_address)
            = lower('0xDb17B211c34240B014ab6d61d4A31FA0C0e20c26')
            and (
                decoded_log:"eventName" = 'PositionDecrease'
                or decoded_log:"eventName" = 'PositionIncrease'
            )
        group by 1
    )
select date, trading_volume, unique_traders, chain, app, category
from gmx_v2_by_chain
union
select
    date,
    sum(trading_volume) as trading_volume,
    sum(unique_traders) as unique_traders,
    null as chain,
    app,
    category
from gmx_v2_by_chain
group by date, app, category
