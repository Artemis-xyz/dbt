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
        full_refresh=false,
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
    , price_data as (
        {{ get_coingecko_metrics("render") }}
    )

select
    burn_data.date,
    price_data.price,
    burn_data.total_burns as burns_native,
    burn_data.revenue,
    -- timestamp columns
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from burn_data
left join price_data using(date)
where true
{{ ez_metrics_incremental('burn_data.date', backfill_date) }}
and burn_data.date < to_date(sysdate())