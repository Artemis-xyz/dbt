{{ config(materialized="view") }}

with
    level_finance_by_chain as (
        select chain, date, app, unique_traders, category
        from {{ ref("fact_level_finance_arbitrum_unique_traders") }}
        union
        select chain, date, app, unique_traders, category
        from {{ ref("fact_level_finance_bsc_unique_traders") }}
    )
select chain, date, app, unique_traders, category
from level_finance_by_chain
union
select
    null as chain, date, app, sum(unique_traders) as unique_traders, 'DeFi' as category
from level_finance_by_chain
group by date, app
