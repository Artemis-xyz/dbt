{{
    config(
        materialized="table",
        snowflake_warehouse="HYPERLIQUID",
    )
}}



SELECT
    a.value:time::date as date
    , a.value:coin::string as token
    , a.value:open_interest::number as open_interest
FROM {{ source("PROD_LANDING", "raw_hyperliquid_open_interest") }},
lateral flatten(input => source_json) a
qualify row_number() over (partition by a.value:time::date, a.value:coin::string order by extraction_date DESC) = 1