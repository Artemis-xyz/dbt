{{
    config(
        materialized="incremental",
        snowflake_warehouse="HEDERA",
        database="hedera",
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

WITH issued_supply_metrics AS (
    SELECT
        date,
        max_supply as max_supply_native,
        uncreated_tokens as uncreated_tokens_native,
        total_supply as total_supply_native,
        cumulative_burned_hbar as cumulative_burned_hbar_native,
        foundation_balances as foundation_balances_native,
        issued_supply as issued_supply_native,
        unvested_balances as unvested_balances_native,
        circulating_supply_native
    FROM {{ ref('fact_hedera_issued_supply_and_float') }}
)
, date_spine AS (
    select * 
    from {{ ref('dim_date_spine') }} 
    where date between '2020-01-01' and to_date(sysdate())
)
, market_metrics AS ({{ get_coingecko_metrics("hedera-hashgraph") }}) 

SELECT
    date_spine.date
    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume
    -- Cash Flow Metrics
    , 0 as revenue
    -- Issued Supply Metrics
    , issued_supply_metrics.max_supply_native
    , issued_supply_metrics.total_supply_native
    , issued_supply_metrics.issued_supply_native
    , issued_supply_metrics.circulating_supply_native
    -- Token Turnover Metrics
    , coalesce(market_metrics.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_metrics.token_turnover_fdv, 0) as token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM date_spine
left join issued_supply_metrics using (date)
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
and date_spine.date < to_date(sysdate())