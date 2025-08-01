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
    --Old metrics needed for backwards compatibility
    , coalesce(revenue_data.revenue, 0) as revenue
    , coalesce(fees_data.fees, 0) as fees
    -- Standardized Metrics)
    -- Token Metrics
    , coalesce(price_data.price, 0) as price
    , coalesce(price_data.market_cap, 0) as market_cap
    , coalesce(price_data.fdmc, 0) as fdmc
    , coalesce(price_data.token_volume, 0) as token_volume
    -- Cash Flow Metrics
    , coalesce(fees_data.fees, 0) as ecosystem_revenue
    , coalesce(revenue_data.revenue, 0) as service_fee_allocation
    , coalesce(revenue_data.hnt_burned, 0) * coalesce(price_data.price, 0) as burned_fee_allocation
    , coalesce(revenue_data.hnt_burned, 0) as burned_fee_allocation_native
    -- Turnover Metrics
    , coalesce(price_data.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(price_data.token_turnover_fdv, 0) as token_turnover_fdv
    -- Other Metrics
    , coalesce(new_mobile_subscribers_data.new_subscribers, 0) as new_subscribers
    , coalesce(new_hotspot_onboards_data.device_onboards, 0) as device_onboards
    --HNT Token Supply Data
    , coalesce(daily_supply_data.emissions_native, 0) as gross_emissions_native
    , coalesce(daily_supply_data.premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(daily_supply_data.burns_native, 0) as burns_native
    , coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_supply_data.burns_native, 0) as net_supply_change_native
    , sum(coalesce(daily_supply_data.emissions_native, 0) + coalesce(daily_supply_data.premine_unlocks_native, 0) - coalesce(daily_supply_data.burns_native, 0)) over (order by daily_supply_data.date) as circulating_supply_native
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
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