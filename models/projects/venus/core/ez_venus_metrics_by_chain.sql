{{
    config(
        materialized="table",
        snowflake_warehouse="venus",
        database="venus",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    venus_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_venus_v4_lending_bsc_gold"),
                ],
            )
        }}
    )
select
    venus_by_chain.date,
    'venus' as app,
    'DeFi' as category,
    venus_by_chain.chain,
    venus_by_chain.daily_borrows_usd,
    venus_by_chain.daily_supply_usd
from venus_by_chain
where venus_by_chain.date < to_date(sysdate())