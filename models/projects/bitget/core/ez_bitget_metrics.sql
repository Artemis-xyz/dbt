{{
    config(
        materialized='incremental',
        snowflake_warehouse='BITGET',
        database='BITGET',
        schema='core',
        alias='ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_exclude_columns=['created_on'],
        full_refresh=false
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = None %}

with price as (
    {{ get_coingecko_metrics("bitget-token") }}
), revenue_data as (
    select 
        date,
        burns_native,
        revenue
    from {{ref("fact_bitget_burns")}}
    {{ ez_metrics_incremental('date', backfill_date) }}
),
supply_data as (
    select
        date,
        max_supply,
        total_supply,
        issued_supply,
        float_supply,
        cumulative_burn
    from {{ref("fact_bitget_supply")}}
    {{ ez_metrics_incremental('date', backfill_date) }}
)
select
    price.date
    -- price data
    , price.price
    , price.market_cap
    , price.fdmc
    , price.token_volume
    --revenue data
    ,coalesce(burns_native, 0) as burns_native
    -- supply data
    ,supply_data.max_supply as max_supply_native
    , supply_data.total_supply as total_supply_native
    , supply_data.cumulative_burn as total_burned_native
    , supply_data.issued_supply as issued_supply_native
    , supply_data.float_supply as circulating_supply_native
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from price
left join revenue_data using(date)
left join supply_data using(date)
{{ ez_metrics_incremental('price.date', backfill_date) }}
    and price.date > TO_DATE('2024-06-30','YYYY-MM-DD')
    and price.date < TO_DATE(SYSDATE())