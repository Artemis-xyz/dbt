{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER",
        database="jupiter",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

select * from {{ref("ez_jupiter_metrics")}}