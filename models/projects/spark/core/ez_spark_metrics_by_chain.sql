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
                    "fact_spark_ethereum_borrows_deposits_gold",
                    "fact_spark_gnosis_borrows_deposits_gold",
                ],
            )
        }}
    )
select
    spark_by_chain.date,
    'spark' as app,
    'DeFi' as category,
    spark_by_chain.chain,
    spark_by_chain.daily_borrows_usd,
    spark_by_chain.daily_supply_usd
from spark_by_chain
where spark_by_chain.date < to_date(sysdate())