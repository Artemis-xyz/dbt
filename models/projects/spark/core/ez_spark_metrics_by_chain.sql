{{
    config(
        materialized="table",
        snowflake_warehouse="SPARK",
        database="spark",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    spark_by_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_spark_ethereum_borrows_deposits_gold"),
                    ref("fact_spark_gnosis_borrows_deposits_gold"),
                ],
            )
        }}
    )
select
    spark_by_chain.date
    , 'spark' as app
    , 'DeFi' as category
    , spark_by_chain.chain
    , spark_by_chain.daily_borrows_usd
    , spark_by_chain.daily_supply_usd
    -- Standardized metrics
    , spark_by_chain.daily_borrows_usd as lending_loans
    , spark_by_chain.daily_supply_usd as lending_deposits
from spark_by_chain
where spark_by_chain.date < to_date(sysdate())