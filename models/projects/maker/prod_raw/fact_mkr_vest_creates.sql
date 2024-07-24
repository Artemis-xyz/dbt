{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_mkr_vest_creates"
    )
}}

SELECT 
    block_timestamp AS ts,
    tx_hash AS hash,
    output_id,
    _bgn,
    _tau,
    _tot / 1e18 AS total_mkr
FROM {{ ref('fact_dssvesttransferrable_create') }}
-- Note: In the future, add a condition for call_success when available