{{
    config(
        materialized="table",
        snowflake_warehouse="BLACKROCK",
        database="blackrock",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

SELECT * FROM {{ref("agg_rwa_by_product_and_chain")}}
