{{
    config(
        materialized="incremental",
        snowflake_warehouse="DODO",
        database="dodo",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with swap_metrics as (
    select
        block_timestamp::date as date,
        count(distinct tx_hash) as txns,
        count(distinct swapper) as dau,
        sum(amount_usd) as volume_usd,
    from {{ref('fact_dodo_swap_metrics')}}
    group by date
)
, token_incentives as (
    select
        block_timestamp::date as date,
        sum(amount_usd) as token_incentives
    from {{ref('fact_dodo_token_incentives')}}
    group by date
)
, date_spine as (
    select * 
    from {{ ref('dim_date_spine') }}
    where date between (select min(date) from swap_metrics) and to_date(sysdate())
)
, market_metrics as (
    {{ get_coingecko_metrics("dodo") }}
)

select
    date_spine.date
    , 'dodo' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , swap_metrics.txns as spot_txns
    , swap_metrics.dau as spot_dau

    -- Fee Data
    , swap_metrics.fees_native
    , swap_metrics.fees as chain_fees
    , swap_metrics.fees

    -- Financial Statements
    , coalesce(token_incentives.token_incentives, 0) as token_incentives

    -- Turnover Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine
left join market_metrics using (date)
left join swap_metrics using (date)
left join token_incentives using (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date <= to_date(sysdate())
    