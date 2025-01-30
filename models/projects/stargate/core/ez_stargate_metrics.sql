{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE",
        database="stargate",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    stargate_metrics as (
        select * from {{ ref('fact_stargate_metrics') }}
    )
select * from stargate_metrics