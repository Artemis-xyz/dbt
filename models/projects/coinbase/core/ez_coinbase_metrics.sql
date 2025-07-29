{{
    config(
        materialized="incremental",
        snowflake_warehouse="COINBASE",
        database="coinbase",
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

with
    staked_eth_metrics as (
        select
            date,
            sum(num_staked_eth) as num_staked_eth,
            sum(amount_staked_usd) as amount_staked_usd,
            sum(num_staked_eth_net_change) as num_staked_eth_net_change,
            sum(amount_staked_usd_net_change) as amount_staked_usd_net_change
        from {{ ref('fact_coinbase_staked_eth_count_with_usd_and_change') }}
        GROUP BY 1
    )
select
    staked_eth_metrics.date,
    'coinbase' as app,
    'DeFi' as category,
    staked_eth_metrics.num_staked_eth,
    staked_eth_metrics.amount_staked_usd,
    staked_eth_metrics.num_staked_eth_net_change,
    staked_eth_metrics.amount_staked_usd_net_change
    -- Standardized Metrics
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.num_staked_eth_net_change as lst_tvl_native_net_change
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from staked_eth_metrics
where true 
{{ ez_metrics_incremental('staked_eth_metrics.date', backfill_date) }}
and staked_eth_metrics.date < to_date(sysdate())
