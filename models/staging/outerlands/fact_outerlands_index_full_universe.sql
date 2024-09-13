{{
    config(
        materialized="table",
        snowflake_warehouse="outerlands"
    )
}}

SELECT 
    DATE(CONVERT_TIMEZONE('UTC', DATE)) as date, -- Convert to UTC and cast to date due to weird timezone handling in Sigma/Snowflake
    price
FROM {{ source("SIGMA", "outerlands_full_universe") }}