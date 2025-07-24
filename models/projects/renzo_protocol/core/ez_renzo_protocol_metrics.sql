{{
    config(
        materialized="incremental",
        snowflake_warehouse="RENZO_PROTOCOL",
        database="renzo_protocol",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    restaked_eth_metrics_by_chain as (
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_ethereum_restaked_eth_count_with_usd_and_change') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        group by 1
        union all
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_arbitrum_restaked_eth_count_with_usd_and_change') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        group by 1
        union all
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_base_restaked_eth_count_with_usd_and_change') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        group by 1
        union all
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_blast_restaked_eth_count_with_usd_and_change') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        group by 1
        union all
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_bsc_restaked_eth_count_with_usd_and_change') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        group by 1
        union all
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_linea_restaked_eth_count_with_usd_and_change') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        group by 1
        union all
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from {{ ref('fact_renzo_protocol_mode_restaked_eth_count_with_usd_and_change') }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        group by 1
    ),
    restaked_eth_metrics as (
        select
            date,
            sum(num_restaked_eth) as num_restaked_eth,
            sum(amount_restaked_usd) as amount_restaked_usd,
            sum(num_restaked_eth_net_change) as num_restaked_eth_net_change,
            sum(amount_restaked_usd_net_change) as amount_restaked_usd_net_change
        from restaked_eth_metrics_by_chain
        {{ ez_metrics_incremental('date', backfill_date) }}
        group by 1
    ),
    market_metrics as (
        {{get_coingecko_metrics('renzo')}}
    ),
    date_spine as (
        select
            ds.date
        from {{ ref('dim_date_spine') }} ds
        where ds.date between (select min(date) from restaked_eth_metrics) and to_date(sysdate())
    )
select
    date_spine.date,
    'renzo_protocol' as app,
    'DeFi' as category

    --Old metrics needed for compatibility
    , restaked_eth_metrics.num_restaked_eth as num_restaked_eth
    , restaked_eth_metrics.amount_restaked_usd as amount_restaked_usd
    , restaked_eth_metrics.num_restaked_eth_net_change as num_restaked_eth_net_change
    , restaked_eth_metrics.amount_restaked_usd_net_change as amount_restaked_usd_net_change

    --Standardized Metrics

    --Market Metrics
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc

    --Usage Metrics
    , restaked_eth_metrics.num_restaked_eth as tvl_native
    , restaked_eth_metrics.num_restaked_eth as lrt_tvl_native
    , restaked_eth_metrics.amount_restaked_usd as tvl
    , restaked_eth_metrics.amount_restaked_usd as lrt_tvl
    , restaked_eth_metrics.num_restaked_eth_net_change as lrt_tvl_native_net_change
    , restaked_eth_metrics.amount_restaked_usd_net_change as lrt_tvl_net_change

    --Other Metrics
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , market_metrics.token_turnover_fdv as token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine
--left join restaked_eth_metrics_by_chain using(date)
left join restaked_eth_metrics using(date)
left join market_metrics using(date)
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())
