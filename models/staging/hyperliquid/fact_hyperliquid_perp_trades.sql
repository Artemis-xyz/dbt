{{
    config(
        materialized="table",
        snowflake_warehouse="HYPERLIQUID",
    )
}}

select * from {{ ref('fact_hyperliquid_trades') }}
where direction not in ('Buy', 'Sell')
