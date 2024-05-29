{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    aave_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_aave_lending_ethereum_gold"),
                    ref("fact_aave_lending_optimism_gold"),
                    ref("fact_aave_lending_arbitrum_gold"),
                    ref("fact_aave_lending_polygon_gold"),
                    ref("fact_aave_lending_fantom_gold"),
                    ref("fact_aave_lending_base_gold"),
                    ref("fact_aave_lending_avalanche_gold"),
                ],
            )
        }}
    )
select
    aave_by_chain.date,
    'aave' as app,
    'DeFi' as category,
    aave_by_chain.chain,
    aave_by_chain.daily_borrows_usd,
    aave_by_chain.daily_supply_usd
from aave_by_chain
where aave_by_chain.date < to_date(sysdate())