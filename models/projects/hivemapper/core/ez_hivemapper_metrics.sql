{{
    config(
        materialized="incremental",
        snowflake_warehouse="HIVEMAPPER",
        database="hivemapper",
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

with bme_reward_types_cte as (
    SELECT
        distinct reward_type as bme_reward_types
    FROM 
        {{ ref('fact_hivemapper_stats') }}
    WHERE reward_type like 'Honey Burst%' or reward_type like 'Map Consumption%'
)
, stats as (
    select
        block_timestamp::date as date,
        SUM(
            CASE
                WHEN reward_type in (SELECT bme_reward_types FROM bme_reward_types_cte) AND action = 'mint' THEN amount_usd
            END
        ) as supply_side_fees,
        SUM(
            CASE
                WHEN action = 'burn' and block_timestamp::date >= '2024-04-17' THEN amount_usd * 0.75 -- MIP 15 was introduced on 2024-04-17 and changed the burn fee structure such that 75% of burns are permanent (prev 0%)
            END
        ) as revenue,
        SUM(
            CASE
                WHEN action = 'burn' THEN amount_usd -- MIP 15 was introduced on 2024-04-17 and changed the burn fee structure such that 75% of burns are permanent (prev 0%)
            END
        ) as fees,
        SUM(
            CASE WHEN action = 'mint'
                THEN amount_native
            END
        ) as mints_native,
        SUM(
            CASE WHEN action = 'burn'
                THEN amount_native
            END
        ) as burn_native,
        COUNT( distinct
            CASE WHEN action = 'mint'
                THEN tx_to_account
            WHEN action = 'transfer' AND reward_type in ('Bounty')
                THEN tx_to_account
            END
        ) AS contributors
    from
        {{ ref('fact_hivemapper_stats') }}
    GROUP BY
        1
)
, daily_supply_data as (
    select
        date,
        premine_unlocks_native,
    from {{ref('fact_hivemapper_daily_supply_data')}}
)
, km_data as (
    select
        date,
        total_km,
        total_unique_km
    from {{ref('fact_hivemapper_KM_data')}}
)

, date_spine as ( 
        select * from {{ ref('dim_date_spine') }}
        where date between '2022-12-20' and to_date(sysdate())
    )
, market_metrics as ({{ get_coingecko_metrics("hivemapper") }})

SELECT
    date_spine.date
    --Old metrics needed for backwards compatibility
    , coalesce(stats.fees, 0) as fees
    , coalesce(stats.supply_side_fees, 0) as primary_supply_side_revenue
    , coalesce(stats.revenue, 0) as revenue
    , coalesce(stats.contributors, 0) as dau
    , coalesce(stats.mints_native, 0) as gross_emissions_native
    , coalesce(stats.mints_native, 0) * coalesce(market_metrics.price, 0) as gross_emissions
    -- Standardized Metrics
    -- Market Metrics
    , coalesce(market_metrics.price, 0) as price
    , coalesce(market_metrics.market_cap, 0) as market_cap
    , coalesce(market_metrics.fdmc, 0) as fdmc
    , coalesce(market_metrics.token_volume, 0) as token_volume
    -- Usage Metrics
    , coalesce(stats.contributors, 0) as chain_dau
    , coalesce(km_data.total_unique_km, 0) as unique_km_mapped
    , coalesce(km_data.total_km, 0) as total_km
    -- Cash Flow Metrics
    , coalesce(stats.fees, 0) as ecosystem_revenue
    , coalesce(stats.supply_side_fees, 0) as service_fee_allocation
    , coalesce(stats.fees, 0) as burned_fee_allocation
    , coalesce(stats.burn_native, 0) as burned_fee_allocation_native
    -- Turnover Metrics
    , coalesce(market_metrics.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_metrics.token_turnover_fdv, 0) as token_turnover_fdv
    --HONEY Token Supply Data
    , coalesce(stats.mints_native, 0) as emissions_native
    , coalesce(daily_supply_data.premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(stats.burn_native, 0) as burns_native
    , coalesce(stats.mints_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(stats.burn_native, 0) as net_supply_change_native
    , sum(coalesce(stats.mints_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(stats.burn_native, 0)) over (order by daily_supply_data.date) as circulating_supply_native
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine
left join market_metrics on date_spine.date = market_metrics.date
left join stats on date_spine.date = stats.date
left join km_data on date_spine.date = km_data.date
left join daily_supply_data on date_spine.date = daily_supply_data.date
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())