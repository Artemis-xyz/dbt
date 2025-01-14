{{
    config(
        materialized="table",
        snowflake_warehouse="MANTA",
        database="manta",
        schema="core",
        alias="ez_metrics"
    )
}}

SELECT
    date,
    daily_txns as txns,
    dau,
    fees
FROM {{ ref('fact_manta_txns_daa') }}