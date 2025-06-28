{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="fact_optimism_mints_burns",
    )
}}

WITH date_spine AS (
    SELECT date
    FROM {{ ref("dim_date_spine") }}
    WHERE date >= '2022-04-26'
    AND date < sysdate()
)

, mints_burns AS (
    SELECT
        date(block_timestamp) as date,
        SUM(CASE WHEN LOWER(from_address) = '0x0000000000000000000000000000000000000000' THEN amount ELSE 0 END) as mints_native,
        SUM(CASE WHEN LOWER(to_address) = '0x0000000000000000000000000000000000000000' THEN amount ELSE 0 END) as burns_native
    FROM {{ source("OPTIMISM_FLIPSIDE", "ez_token_transfers") }}
    WHERE LOWER(contract_address) = LOWER('0x4200000000000000000000000000000000000042')
    GROUP BY 1
)

SELECT
    date,
    COALESCE(mints_native, 0) as mints_native,
    COALESCE(burns_native, 0) as burns_native,
    SUM(COALESCE(mints_native, 0)) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_mints_native,
    SUM(COALESCE(burns_native, 0)) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_burns_native
FROM date_spine
LEFT JOIN mints_burns USING (date)
ORDER BY 1