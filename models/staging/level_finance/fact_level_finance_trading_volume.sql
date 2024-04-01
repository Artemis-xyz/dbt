{{ config(materialized="view") }}

with
    level_finance_by_chain as (
        select chain, date, app, trading_volume, category
        from {{ ref("fact_level_finance_arbitrum_trading_volume") }}
        union
        select chain, date, app, trading_volume, category
        from {{ ref("fact_level_finance_bsc_trading_volume") }}
    )
select chain, date, app, trading_volume, category
from level_finance_by_chain
union
select
    null as chain, date, app, sum(trading_volume) as trading_volume, 'DeFi' as category
from level_finance_by_chain
group by date, app
