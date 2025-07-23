{{
    config(
        materialized="incremental",
        snowflake_warehouse="FRAX",
        database="frax",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with dex_data as (
    SELECT
        block_timestamp::date as date,
        count(distinct sender) as spot_dau,
        count(*) as spot_txns,
        sum(trading_volume) as spot_volume,
        sum(gas_cost_native) as gas_cost_native,
        sum(trading_fees) as spot_fees
    FROM
        {{ ref("ez_frax_dex_swaps") }}
    {{ ez_metrics_incremental('block_timestamp::date', backfill_date) }}
    GROUP BY
        1
)
, staked_eth_metrics as (
    select
        date,
        num_staked_eth,
        amount_staked_usd,
        num_staked_eth_net_change,
        amount_staked_usd_net_change
    from {{ ref('fact_frax_staked_eth_count_with_USD_and_change') }}
    {{ ez_metrics_incremental('date', backfill_date) }}
)
, tvl_data as (
    SELECT
        date,
        sum(tvl) as tvl
    FROM
        {{ ref("fact_fraxswap_ethereum_tvl_by_pool") }}
    {{ ez_metrics_incremental('date', backfill_date) }}
    GROUP BY
        date
)
, fxs_daily_supply_data as (
    SELECT
        date,
        emissions_native,
        total_premine_unlocks,
        burns_native,
        net_supply_change_native,
        total_circulating_supply
    FROM
        {{ ref("fact_fxs_daily_supply_data") }}
    {{ ez_metrics_incremental('date', backfill_date) }}
)
, frax_daily_supply_data as (
    SELECT
        date,
        supply as frax_circulating_supply
    FROM
        {{ ref("fact_frax_circulating_supply") }}
    {{ ez_metrics_incremental('date', backfill_date) }}
)
, veFXS_daily_supply_data as (
    SELECT
        date,
        circulating_supply
    FROM
        {{ ref("fact_veFXS_daily_supply") }}
    {{ ez_metrics_incremental('date', backfill_date) }}
)
, fractal_l2_txns as (
    SELECT
        date,
        l2_txns
    FROM
        {{ ref("fact_frax_L2_transactions") }}
    {{ ez_metrics_incremental('date', backfill_date) }}
)
, date_spine as (
    select
        ds.date
    from {{ ref('dim_date_spine') }} ds
    where ds.date between (select min(date) from tvl_data) and to_date(sysdate())
)
, market_metrics as (
    {{ get_coingecko_metrics('frax-share')}}
)

SELECT
    date_spine.date
    -- Standardized Metrics
    -- Price Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume
    -- Usage Metrics
    , dex_data.spot_txns as spot_txns
    , dex_data.spot_dau as spot_dau
    , dex_data.spot_volume as spot_volume
    , fractal_l2_txns.l2_txns as chain_txns
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.num_staked_eth_net_change as lst_tvl_native_net_change
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change
    , tvl_data.tvl as spot_tvl
    , frax_daily_supply_data.frax_circulating_supply as stablecoin_total_supply
    , veFXS_daily_supply_data.circulating_supply as veFXS_total_supply
    --Cashflow Metrics
    , dex_data.spot_fees as spot_fees
    , spot_fees as ecosystem_revenue
    -- Other Metrics
    , dex_data.gas_cost_native
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv
    --FXS Token Supply Data
    , fxs_daily_supply_data.emissions_native as emissions_native
    , fxs_daily_supply_data.total_premine_unlocks as premine_unlocks_native
    , fxs_daily_supply_data.burns_native as burns_native
    , fxs_daily_supply_data.net_supply_change_native as net_supply_change_native
    , fxs_daily_supply_data.total_circulating_supply as circulating_supply_native   
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from date_spine
left join market_metrics using (date)
left join dex_data using (date)
left join fractal_l2_txns using (date)
left join staked_eth_metrics using (date)
left join tvl_data using (date)
left join frax_daily_supply_data using (date)
left join veFXS_daily_supply_data using (date)
left join fxs_daily_supply_data using (date)
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
    and date_spine.date < to_date(sysdate())
