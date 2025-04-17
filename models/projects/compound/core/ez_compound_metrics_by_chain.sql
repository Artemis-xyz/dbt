{{
    config(
        materialized="table",
        snowflake_warehouse="COMPOUND",
        database="compound",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    compound_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_compound_lending_arbitrum_gold"),
                    ref("fact_compound_lending_base_gold"),
                    ref("fact_compound_lending_ethereum_gold"),
                    ref("fact_compound_lending_polygon_gold"),
                ],
            )
        }}
    )
select
    compound_by_chain.date
    , 'compound' as app
    , 'DeFi' as category
    , compound_by_chain.chain
    , compound_by_chain.daily_borrows_usd
    , compound_by_chain.daily_supply_usd
    -- Standardized metrics
    , compound_by_chain.daily_borrows_usd as lending_loans
    , compound_by_chain.daily_supply_usd as lending_deposits
from compound_by_chain
where compound_by_chain.date < to_date(sysdate())