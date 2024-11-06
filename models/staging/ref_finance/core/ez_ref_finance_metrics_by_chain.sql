{{
    config(
        materialized = 'view',
        snowflake_warehouse = 'REF_FINANCE',
        unique_key = ['date'],
        alias = 'ez_metrics_by_chain',
        schema = 'core',
        database = 'REF_FINANCE'
    )
}}

select
    date,
    'near' as chain,
    trading_fees,
    revenue,
    tvl,
    daily_swaps,
    unique_traders,
    trading_volume,
    price,
    market_cap,
    fdmc,
    token_turnover_circulating,
    token_turnover_fdv,
    token_volume
from {{ref('ez_ref_finance_metrics')}}
where date < to_date(sysdate())
