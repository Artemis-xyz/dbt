{{
    config(
        materialized="view",
        snowflake_warehouse="BITCOIN",
        database="bitcoin",
        schema="core",
        alias="ez_etf_metrics",
    )
}}

select * from {{ ref('fact_bitcoin_etf_flows') }}