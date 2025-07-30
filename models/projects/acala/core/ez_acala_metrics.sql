--depends_on: {{ ref("fact_acala_rolling_active_addresses") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="ACALA",
        database="acala",
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
    fundamental_data as (
        select
            date, chain, daa, txns, fees_native, fees_usd as fees, fees_native * .2 as revenue_native, fees_usd * .2 as revenue
        from {{ ref("fact_acala_fundamental_metrics") }}
    ),
    rolling_metrics as ({{ get_rolling_active_address_metrics("acala") }}),
    price_data as ({{ get_coingecko_metrics("acala") }})
select
    fundamental_data.date
    , 'acala' as artemis_id

    --Usage Data
    , daa as dau
    , txns

    -- Market Data
    , price
    , market_cap
    , fdmc

    -- Chain Data
    , dau AS dau
    , wau AS wau
    , mau AS mau
    , txns AS txns

    --Fee Data
    , fees_native
    , fees

    --Fee Allocation
    , fees_native AS burned_fee_allocation

    --Financial Statements
    , fees_native AS revenue_native 
    , fees AS revenue

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join price_data on fundamental_data.date = price_data.date
where true
{{ ez_metrics_incremental("fundamental_data.date", backfill_date) }}
and fundamental_data.date < to_date(sysdate())

