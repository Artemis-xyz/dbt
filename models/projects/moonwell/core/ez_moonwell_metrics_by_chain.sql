{{
    config(
        materialized="table",
        snowflake_warehouse="MOONWELL",
        database="moonwell",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    moonwell_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_moonwell_base_borrows_deposits_gold"),
                ],
            )
        }}
    )
select
    moonwell_by_chain.date
    , 'moonwell' as artemis_id
    , moonwell_by_chain.chain

    -- Standardized Metrics

    -- Usage Metrics
    , moonwell_by_chain.daily_borrows_usd as lending_loans
    , moonwell_by_chain.daily_supply_usd as lending_deposits
from moonwell_by_chain
where moonwell_by_chain.date < to_date(sysdate())