{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}


WITH 

    , token_incentives as (
        SELECT * FROM {{ ref('fact_uniswap_token_incentives') }}
    )