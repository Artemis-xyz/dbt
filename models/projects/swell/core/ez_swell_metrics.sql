{{
    config(
        materialized="incremental",
        snowflake_warehouse="SWELL",
        database="swell",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with restaked_eth_metrics as (
    select
        date,
        chain,
        num_restaked_eth,
        amount_restaked_usd,
        num_restaked_eth_net_change,
        amount_restaked_usd_net_change
    from {{ ref('fact_rsweth_restaked_eth_count_with_usd_and_change') }}
),
staked_eth_metrics as (
    select
        date,
        chain,
        num_staked_eth,
        amount_staked_usd,
        num_staked_eth_net_change,
        amount_staked_usd_net_change
    from {{ ref('fact_sweth_staked_eth_count_with_usd_and_change') }}
),
market_metrics as (
    {{get_coingecko_metrics('swell-network')}}
),
date_spine as (
    select
        ds.date
    from {{ ref('dim_date_spine') }} ds
    where ds.date between (select min(date) from restaked_eth_metrics) and to_date(sysdate())
)
select
    date_spine.date,
    'swell' as artemis_id,

    --Standardized Metrics

    --Market Metrics 
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc

    -- LRT Usage Metrics
    , restaked_eth_metrics.num_restaked_eth as lrt_tvl_native
    , restaked_eth_metrics.amount_restaked_usd as lrt_tvl
    
    -- LST Usage Metrics
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    
    -- TVL Metrics
    , coalesce(lst_tvl_native, 0) + coalesce(lrt_tvl_native, 0) as tvl_native
    , coalesce(lst_tvl, 0) + coalesce(lrt_tvl, 0) as tvl

    -- Market Metrics
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , market_metrics.token_turnover_fdv as token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine
left join restaked_eth_metrics on date_spine.date = restaked_eth_metrics.date
left join staked_eth_metrics on date_spine.date = staked_eth_metrics.date and restaked_eth_metrics.chain = staked_eth_metrics.chain
left join market_metrics on date_spine.date = market_metrics.date
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())
