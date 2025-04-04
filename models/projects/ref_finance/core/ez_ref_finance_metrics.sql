{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'REF_FINANCE',
        unique_key = ['date'],
        alias = 'ez_metrics',
        schema = 'core',
        database = 'REF_FINANCE'
    )
}}

with dau_txns_volume as(
    select
        date,
        daily_swaps,
        unique_traders,
        volume
    from {{ ref('fact_ref_finance_dau_txns_volume') }}
), price as (
    {{ get_coingecko_metrics('ref-finance') }}
)

select
    d.date,
    d.daily_swaps,
    d.unique_traders,
    d.volume as trading_volume,

    -- Standardized Metrics
    d.unique_traders as spot_dau,
    d.daily_swaps as spot_txns,
    d.volume as spot_volume,

    p.price,
    p.market_cap,
    p.fdmc,
    p.token_turnover_circulating,
    p.token_turnover_fdv,
    p.token_volume
from dau_txns_volume d
left join price p on d.date = p.date        
where d.date < to_date(sysdate())
