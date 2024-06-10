{{
    config(
        materialized="table",
        snowflake_warehouse="RADIANT",
        database="radiant",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    radiant_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_radiant_arbitrum_borrows_deposits_gold"),
                    ref("fact_radiant_bsc_borrows_deposits_gold"),
                    ref("fact_radiant_ethereum_borrows_deposits_gold"),
                ],
            )
        }}
    )
select
    radiant_by_chain.date,
    'radiant' as app,
    'DeFi' as category,
    radiant_by_chain.chain,
    radiant_by_chain.daily_borrows_usd,
    radiant_by_chain.daily_supply_usd
from radiant_by_chain
where radiant_by_chain.date < to_date(sysdate())