{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_mkr_vest_events"
    )
}}

SELECT
    tx_hash AS hash,
    1 AS vested
FROM {{ ref('fact_dssvesttransferrable_vest') }}
-- Note: In the future, add a condition for call_success when available