{{
    config(
        materialized="incremental",
        snowflake_warehouse="RENDER",
        database="render",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with 
    burn_data as (
        select 
            block_timestamp::date as date,
            sum(amount_native) as total_burns,
            sum(amount) as revenue
        from {{ ref("fact_render_burns") }}
        group by 1
    )
    , supply_data as (
        select
            date,
            total_supply_native,
            issued_supply_native,
            circulating_supply_native
        from {{ ref("fact_render_supply") }}
    )
    , price_data as (
        {{ get_coingecko_metrics("render") }}
    )
    , date_spine as (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date between '2019-02-12' and to_date(sysdate())
    )

select
    date_spine.date,
    price_data.price,


    -- Financial Data
    burn_data.revenue,

    -- Supply Data
    supply_data.total_supply_native,
    supply_data.issued_supply_native,
    supply_data.circulating_supply_native,

    -- Timestamp Columns
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from date_spine
left join burn_data using(date)
left join price_data using(date)
left join supply_data using(date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())
