{{
    config(
        materialized="incremental",
        snowflake_warehouse="SUSHISWAP_SM",
        database="sushiswap",
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

with trading_volume_by_pool as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_sushiswap_v2_arbitrum_trading_vol_fees_traders_by_pool"),
                ref("fact_sushiswap_v2_avalanche_trading_vol_fees_traders_by_pool"),
                ref("fact_sushiswap_v2_bsc_trading_vol_fees_traders_by_pool"),
                ref("fact_sushiswap_v2_ethereum_trading_vol_fees_traders_by_pool"),
                ref("fact_sushiswap_v2_gnosis_trading_vol_fees_traders_by_pool"),
            ],
        )
    }}
),
trading_volume as (
    select
        trading_volume_by_pool.date,
        sum(trading_volume_by_pool.trading_volume) as trading_volume,
        sum(trading_volume_by_pool.trading_fees) as trading_fees,
        sum(trading_volume_by_pool.unique_traders) as unique_traders,
        sum(trading_volume_by_pool.gas_cost_native) as gas_cost_native,
        sum(trading_volume_by_pool.gas_cost_usd) as gas_cost_usd
    from trading_volume_by_pool
    group by trading_volume_by_pool.date
),
tvl_by_pool as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_sushiswap_v2_gnosis_tvl_by_pool"),
                ref("fact_sushiswap_v2_ethereum_tvl_by_pool"),
                ref("fact_sushiswap_v2_bsc_tvl_by_pool"),
                ref("fact_sushiswap_v2_avalanche_tvl_by_pool"),
                ref("fact_sushiswap_v2_arbitrum_tvl_by_pool"),

            ],
        )
    }}
),
tvl_data as (
    select
        tvl_by_pool.date,
        sum(tvl_by_pool.tvl) as tvl
    from tvl_by_pool
    group by tvl_by_pool.date
)
, cashflow_metrics as (
    select
        date,
        sum(trading_fees) as ecosystem_revenue,
        case
            when date between '2023-01-23' and '2024-01-23' THEN
                sum(trading_fees * 0.0030)
            else
                sum(trading_fees * 0.0025 / 0.0030)
        end as service_fee_allocation
        , case
            when date between '2023-01-23' and '2024-01-23' THEN
                sum(0)
            else
                sum(trading_fees * 0.0005 / 0.0030)
        end as staking_fee_allocation
    from trading_volume
    group by date
)
, token_incentives as (
    select
        date,
        sum(incentives_usd) as token_incentives
    from {{ ref('fact_sushiswap_token_incentives') }}
    group by date
)
, date_spine AS (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    where date between '2020-08-27' and to_date(sysdate())
)
, market_metrics AS (
    {{ get_coingecko_metrics('sushi') }}
)
    
select
    date_spine.date
    , 'sushiswap' as app
    , 'DeFi' as category

    -- Old metrics needed for compatibility
    , trading_volume.gas_cost_native
    , trading_volume.gas_cost_usd
    , trading_volume.trading_volume
    , trading_volume.trading_fees
    , trading_volume.unique_traders

    -- Standardized Metrics

    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Metrics
    , trading_volume.unique_traders as spot_dau
    , trading_volume.trading_volume as spot_volume
    , tvl.tvl

    -- Cashflow Metrics
    , trading_volume.trading_fees as spot_fees
    , cashflow_metrics.service_fee_allocation as service_fee_allocation
    , cashflow_metrics.staking_fee_allocation as staking_fee_allocation

    -- Financial Statement Metrics
    , cashflow_metrics.ecosystem_revenue as fees
    , 0 as revenue
    , cashflow_metrics.staking_fee_allocation as staking_revenue
    , token_incentives.token_incentives as token_incentives
    , revenue - token_incentives as earnings

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine
left join tvl_data tvl using(date)
left join cashflow_metrics using(date)
left join trading_volume using(date)
left join token_incentives using(date)
left join market_metrics using(date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())