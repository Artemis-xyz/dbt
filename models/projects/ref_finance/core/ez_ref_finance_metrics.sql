{{
    config(
        materialized = 'incremental',
        snowflake_warehouse = 'REF_FINANCE',
        unique_key = ['date'],
        alias = 'ez_metrics',
        schema = 'core',
        database = 'REF_FINANCE',
        incremental_strategy = 'merge',
        on_schema_change = 'append_new_columns',
        merge_update_columns = var('backfill_columns', []),
        merge_exclude_columns=['created_on'] if not var('backfill_columns', []) else none,
        full_refresh = false,
        tags = ['ez_metrics']
    )
}}

{% set backfill_date = var("backfill_date", None) %}

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
    p.token_volume,

    -- timestamp columns
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from dau_txns_volume d
left join price p on d.date = p.date   
where true     
{{ ez_metrics_incremental('d.date', backfill_date) }}
and d.date < to_date(sysdate())
