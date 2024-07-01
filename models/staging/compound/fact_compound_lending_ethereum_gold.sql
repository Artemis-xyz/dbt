{{ config(materialized="table") }}
with
    compound_ethereum_data as (
        select
            date,
            chain,
            'compound' as app,
            category,
            daily_borrows_usd,
            daily_supply_usd
        from {{ ref("fact_compound_v2_lending_ethereum_gold") }}
        union all
        select
            date,
            chain,
            'compound' as app,
            category,
            daily_borrows_usd,
            daily_supply_usd
        from {{ ref("fact_compound_v3_lending_ethereum_gold") }}
    ),
    combined_compound_data as (
        select
            date,
            chain,
            app,
            category,
            sum(daily_borrows_usd) as daily_borrows_usd,
            sum(daily_supply_usd) as daily_supply_usd
        from compound_ethereum_data
        group by date, chain, app, category
    )
select date, chain, app, category, daily_borrows_usd, daily_supply_usd
from combined_compound_data
