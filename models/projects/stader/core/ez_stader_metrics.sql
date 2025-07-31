{{
    config(
        materialized="incremental",
        snowflake_warehouse="STADER",
        database="stader",
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

with
    staked_eth_metrics as (
        select
            date,
            chain,
            num_staked_eth,
            amount_staked_usd,
            num_staked_eth_net_change,
            amount_staked_usd_net_change
        from {{ ref('fact_ethx_staked_eth_count_with_usd_and_change') }}
    )
    , market_metrics as (
        {{ get_coingecko_metrics('stader') }}
    )
    , date_spine as (
        select
            date
        from {{ ref('dim_date_spine') }}
        where date between (
                SELECT min(date) FROM (
                    SELECT date FROM staked_eth_metrics
                    UNION ALL
                    SELECT date FROM market_metrics
                )
            ) and to_date(sysdate())
    )
select
    date_spine.date,
    'stader' as artemis_id,

    --Market Metrics
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc

    --Standardized Metrics
    , staked_eth_metrics.num_staked_eth as tvl_native
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as tvl
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.num_staked_eth_net_change as lst_tvl_native_net_change
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change

    --Other Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine
left join staked_eth_metrics using (date)
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())
