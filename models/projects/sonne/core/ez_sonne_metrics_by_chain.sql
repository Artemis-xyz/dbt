{{
    config(
        materialized="table",
        snowflake_warehouse="SONNE_FINANCE",
        database="sonne_finance",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    sonne_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_sonne_base_borrows_deposits_gold"),
                    ref("fact_sonne_optimism_borrows_deposits_gold"),
                ],
            )
        }}
    )
select
    sonne_by_chain.date
    , 'sonne' as app
    , 'DeFi' as category
    , sonne_by_chain.chain
    , sonne_by_chain.daily_borrows_usd
    , sonne_by_chain.daily_supply_usd
    -- Standardized metrics
    , sonne_by_chain.daily_borrows_usd as lending_loans
    , sonne_by_chain.daily_supply_usd as lending_deposits
from sonne_by_chain
where sonne_by_chain.date < to_date(sysdate())