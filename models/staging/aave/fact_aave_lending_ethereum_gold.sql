{{ config(materialized="table") }}
with
    aave_chain_data as (
        select date, chain, 'aave' as app, category, daily_borrows_usd, daily_supply_usd
        from {{ ref("fact_aave_v2_lending_ethereum_gold") }}
        union all
        select date, chain, 'aave' as app, category, daily_borrows_usd, daily_supply_usd
        from {{ ref("fact_aave_v3_lending_ethereum_gold") }}
    ),
    combined_aave_data as (
        select
            date,
            chain,
            app,
            category,
            sum(daily_borrows_usd) as daily_borrows_usd,
            sum(daily_supply_usd) as daily_supply_usd

        from aave_chain_data
        group by date, chain, app, category
    )
select date, chain, app, category, daily_borrows_usd, daily_supply_usd
from combined_aave_data
