{{
    config(
        materialized="view",
        snowflake_warehouse="ETHEREUM",
        database="ethereum",
        schema="core",
        alias="ez_etf_metrics",
    )
}}

select * from {{ ref('fact_ethereum_etf_flows') }}