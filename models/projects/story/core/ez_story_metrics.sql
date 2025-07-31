{{
    config(
        materialized="incremental",
        snowflake_warehouse="STORY",
        database="story",
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


WITH issued_supply_metrics AS (
    SELECT 
        date,
        total_supply + cumulative_ip_burned AS max_supply_native,
        total_supply AS total_supply_native,
        total_ip_burned AS native_burns,
        revenue,
        issued_supply AS issued_supply_native,
        circulating_supply AS circulating_supply_native
    FROM {{ ref('fact_story_issued_supply_and_float') }}
)
, story_market_data as (
    {{ get_coingecko_metrics('story-2') }}
)

SELECT
    f.date,
    'story' as artemis_id,

    --Market Data
    smd.price, 
    smd.market_cap as mc, 
    smd.fdmc,
    smd.token_volume,

    --Usage Data
    f.txns,
    f.daa AS dau,

    --Fee Data
    f.fees_native,
    f.fees,
    
    --Financial Statments
    i.revenue,

    --Supply Data
    i.max_supply_native,
    i.total_supply_native,
    i.native_burns,
    i.issued_supply_native,
    i.circulating_supply_native,

    
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM {{ ref("fact_story_fundamental_metrics") }} f
LEFT JOIN issued_supply_metrics i
    ON DATE(f.date) = DATE(i.date)
LEFT JOIN story_market_data smd 
    ON DATE(smd.date) = DATE(i.date)

WHERE TRUE
{{ ez_metrics_incremental('f.date', backfill_date) }}
AND f.date < TO_DATE(SYSDATE())
