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

with fees_revs_tvl as (
    select
        date,
        fees,
        revenue,
        tvl
    from {{ ref('fact_ref_finance_fees_revs_tvl') }}
)
, dau_txns_volume as(
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
    coalesce(d.date, f.date) as date,
    f.fees as trading_fees,
    f.revenue,
    f.tvl,
    d.daily_swaps,
    d.unique_traders,
    d.volume as trading_volume,
    p.price,
    p.market_cap,
    p.fdmc,
    p.token_turnover_circulating,
    p.token_turnover_fdv,
    p.token_volume
from dau_txns_volume d
left join price p on d.date = p.date
left join fees_revs_tvl f on d.date = f.date        
where d.date < to_date(sysdate())
