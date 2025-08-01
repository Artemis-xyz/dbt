{{
    config(
        materialized="incremental",
        snowflake_warehouse="CURVE",
        database="curve",
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

with trading_volume_by_pool as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_curve_arbitrum_daily_trading_metrics"),
                ref("fact_curve_avalanche_daily_trading_metrics"),
                ref("fact_curve_ethereum_daily_trading_metrics"),
                ref("fact_curve_optimism_daily_trading_metrics"),
                ref("fact_curve_polygon_daily_trading_metrics"),
            ],
        )
    }}
)
, trading_volume as (
    select
        trading_volume_by_pool.date,
        sum(trading_volume_by_pool.trading_volume) as trading_volume,
        sum(trading_volume_by_pool.trading_fees) as trading_fees,
        sum(trading_volume_by_pool.trading_revenue) as trading_revenue,
        sum(trading_volume_by_pool.gas_cost_native) as gas_cost_native,
        sum(trading_volume_by_pool.gas_cost_usd) as gas_cost_usd
    from trading_volume_by_pool
    group by trading_volume_by_pool.date
)
, ez_dex_swaps as (
    SELECT
        block_timestamp::date as date,
        count(distinct sender) as unique_traders,
        count(*) as spot_txns
    FROM
        {{ ref('ez_curve_dex_swaps') }}
    group by 1
)
, tvl_by_pool as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref("fact_curve_arbitrum_tvl_by_pool"),
                ref("fact_curve_avalanche_tvl_by_pool"),
                ref("fact_curve_ethereum_tvl_by_pool"),
                ref("fact_curve_optimism_tvl_by_pool"),
                ref("fact_curve_polygon_tvl_by_pool"),
            ],
        )
    }}
)
, tvl as (
    select
        tvl_by_pool.date,
        sum(tvl_by_pool.tvl) as tvl
    from tvl_by_pool
    group by tvl_by_pool.date
)
, token_incentives as (
    select
        date,
        sum(minted_amount) as token_incentives_native,
        sum(minted_usd) as token_incentives
    from {{ ref('fact_curve_token_incentives') }}
    group by 1
)
, date_spine as (
    select
        date
    from {{ ref('dim_date_spine') }}
    where date between (select min(date) from tvl) and to_date(sysdate())
)

, issued_supply_metrics as (
    select 
        date,
        max_supply as max_supply_native,
        total_supply as total_supply_native,
        issued_supply as issued_supply_native,
        circulating_supply as circulating_supply_native
    from {{ ref('fact_curve_issued_supply_and_float') }}
)

, market_metrics as (
    {{ get_coingecko_metrics('curve-dao-token')}}
)

select
    date_spine.date
    , 'curve' as app
    , 'DeFi' as category
    -- Standardized Metrics
    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume
    -- Usage Metrics
    , ez_dex_swaps.unique_traders as spot_dau
    , ez_dex_swaps.spot_txns
    , trading_volume.trading_volume as spot_volume
    , tvl.tvl
    -- Cashflow Metrics
    , trading_volume.trading_fees as spot_fees
    , trading_volume.trading_fees as ecosystem_revenue
    , trading_volume.trading_fees * 0.5 as staking_fee_allocation
    , trading_volume.trading_fees * 0.5 as service_fee_allocation
    , token_incentives.token_incentives_native
    , trading_volume.gas_cost_native
    , trading_volume.gas_cost_usd as gas_cost
    -- Issued Supply Metrics
    , issued_supply_metrics.max_supply_native
    , issued_supply_metrics.total_supply_native
    , issued_supply_metrics.issued_supply_native
    , issued_supply_metrics.circulating_supply_native
    -- Financial Statement Metrics
    , trading_volume.trading_fees as fees
    , trading_volume.trading_fees * 0.5 as revenue
    , token_incentives.token_incentives as token_incentives
    , revenue - token_incentives as earnings
    -- Other Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from date_spine
left join market_metrics using(date)
left join ez_dex_swaps using(date)
left join trading_volume using(date)
left join tvl using(date)
left join token_incentives using(date)
left join issued_supply_metrics using(date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())