{{
    config(
        materialized="table",
        snowflake_warehouse="SPARK",
        database="spark",
        schema="core",
        alias="ez_metrics",
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
    , spark_metrics as (
        select
            date
            , sum(daily_borrows_usd) as daily_borrows_usd
            , sum(daily_supply_usd) as daily_supply_usd
        from spark_by_chain
        group by 1
    )

select
    spark_metrics.date
    , 'spark' as app
    , 'DeFi' as category
    , spark_metrics.daily_borrows_usd
    , spark_metrics.daily_supply_usd
    -- Standardized metrics
    , spark_metrics.daily_borrows_usd as lending_loans
    , spark_metrics.daily_supply_usd as lending_deposits
from spark_metrics
where spark_metrics.date < to_date(sysdate())