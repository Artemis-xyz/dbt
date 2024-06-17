{{
    config(
        materialized="table",
        snowflake_warehouse="benqi",
        database="benqi",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    benqi_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_benqi_avalanche_borrows_deposits_gold"),
                ],
            )
        }}
    )
select
    benqi_by_chain.date,
    'benqi' as app,
    'DeFi' as category,
    benqi_by_chain.chain,
    benqi_by_chain.daily_borrows_usd,
    benqi_by_chain.daily_supply_usd
from benqi_by_chain
where benqi_by_chain.date < to_date(sysdate())