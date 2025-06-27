{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="fact_optimism_mints_burns",
    )
}}

SELECT
    date(block_timestamp) as date,
    SUM(CASE WHEN LOWER(from_address) = '0x0000000000000000000000000000000000000000' THEN amount ELSE 0 END) as mints_native,
    SUM(CASE WHEN LOWER(to_address) = '0x0000000000000000000000000000000000000000' THEN amount ELSE 0 END) as burns_native,
    SUM(mints_native) OVER (ORDER BY date(block_timestamp) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_mints_native,
    SUM(burns_native) OVER (ORDER BY date(block_timestamp) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_burns_native,
FROM {{ source("OPTIMISM_FLIPSIDE", "ez_token_transfers") }}
WHERE LOWER(contract_address) = LOWER('0x4200000000000000000000000000000000000042')
GROUP BY 1
ORDER BY 1