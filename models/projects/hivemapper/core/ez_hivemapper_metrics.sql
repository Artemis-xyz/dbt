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
        full_refresh=var("full_refresh", false),
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
    , 'hivemapper' as artemis_id

    -- Standardized Metrics

    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Metrics
    , stats.contributors as dau
    , km_data.total_unique_km as unique_km_mapped
    , km_data.total_km as total_km

    -- Fees Metrics
    , stats.fees as fees
    , stats.supply_side_fees as service_fee_allocation
    , stats.revenue as burned_fee_allocation
    , stats.burn_native as burned_fee_allocation_native

    -- Financial Metrics
    , stats.revenue as revenue

    -- Turnover Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- Supply Metrics
    , coalesce(stats.mints_native, 0) as gross_emissions_native
    , daily_supply_data.premine_unlocks_native as premine_unlocks_native
    , stats.burn_native as burns_native
    , stats.mints_native + daily_supply_data.premine_unlocks_native - stats.burn_native as net_supply_change_native
    , sum(stats.mints_native + daily_supply_data.premine_unlocks_native - stats.burn_native) over (order by daily_supply_data.date) as circulating_supply_native

    -- Timestamp Columns
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