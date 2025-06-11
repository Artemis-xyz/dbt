{{
    config(
        materialized="table",
        snowflake_warehouse="SEAMLESSPROTOCOL",
        database="seamlessprotocol",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    seamless_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_seamless_protocol_base_borrows_deposits_gold"),
                ],
            )
        }}
    )
select
    seamless_by_chain.date
    , 'seamless' as app
    , 'DeFi' as category
    , seamless_by_chain.chain
    , seamless_by_chain.daily_borrows_usd
    , seamless_by_chain.daily_supply_usd
    -- Standardized metrics
    , seamless_by_chain.daily_borrows_usd as lending_loans
    , seamless_by_chain.daily_supply_usd as lending_deposits
from seamless_by_chain
where seamless_by_chain.date < to_date(sysdate())