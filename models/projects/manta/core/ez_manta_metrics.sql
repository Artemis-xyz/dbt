{{
    config(
        materialized="table",
        snowflake_warehouse="MANTA"
    )
}}

SELECT
    date,
    daily_txns as txns,
    dau,
    fees
FROM {{ ref('fact_manta_txns_daa') }}