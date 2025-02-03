{{
    config(
        materialized="table",
        snowflake_warehouse="MAGICEDEN",
        database="magiceden",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

select 
    * 
from
    {{ ref('fact_magiceden_metrics_by_chain') }}