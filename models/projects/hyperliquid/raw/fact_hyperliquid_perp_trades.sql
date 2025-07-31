{{
    config(
        materialized="table",
        snowflake_warehouse="HYPERLIQUID",
    )
}}

select * from {{ ref('fact_hyperliquid_trades') }}
where direction in ('Open Long', 'Open Short')
