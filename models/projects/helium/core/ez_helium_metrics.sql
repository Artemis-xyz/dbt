{{
    config(
        materialized="incremental",
        snowflake_warehouse="HELIUM",
        database="helium",
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
    date_spine as (
        select date
        from {{ ref("dim_date_spine") }}
        where date between '2020-04-17' and to_date(sysdate())
    ),
    revenue_data as (
        select date, hnt_burned, revenue
        from {{ ref("fact_helium_revenue_silver") }}
    ),
    fees_data as(
        select date, fees
        from {{ ref("fact_helium_fees_silver") }}
    ),
    new_mobile_subscribers_data as (
        select date, new_subscribers
        from {{ ref("fact_helium_new_mobile_subscribers") }}
    ),
    new_hotspot_onboards_data as (
        select date, device_onboards
        from {{ ref("fact_helium_new_hotspot_onboards") }}
    ),
    mints_data as (
        select date, mints_native
        from {{ ref("fact_helium_mints") }}
    ),
    daily_supply_data as (
        select * from {{ ref("fact_helium_daily_supply_data") }}
    ),
    price_data as ({{ get_coingecko_metrics("helium") }})
select
    date_spine.date
    -- Standardized Metrics)
    -- Market Metrics
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume
    
    -- Usage Metrics
    , new_mobile_subscribers_data.new_subscribers
    , new_hotspot_onboards_data.device_onboards

    -- Fee Metrics
    , fees_data.fees
    , revenue_data.revenue as service_fee_allocation
    , coalesce(revenue_data.hnt_burned, 0) * coalesce(price_data.price, 0) as burned_fee_allocation
    , revenue_data.hnt_burned as burned_fee_allocation_native

    -- Financial Metrics
    , revenue_data.revenue


    -- Supply Metrics
    , daily_supply_data.emissions_native
    , daily_supply_data.premine_unlocks_native
    , daily_supply_data.burns_native
    , coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_supply_data.burns_native, 0) as net_supply_change_native
    , sum(coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_supply_data.burns_native, 0)) over (order by daily_supply_data.date) as circulating_supply_native

    -- Other Metrics
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv

    -- Timestamp Columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine
left join revenue_data using (date)
left join price_data using (date)
left join fees_data using (date)
left join new_mobile_subscribers_data using (date)
left join new_hotspot_onboards_data using (date)
left join mints_data using (date)
left join daily_supply_data using (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())
order by 1 desc