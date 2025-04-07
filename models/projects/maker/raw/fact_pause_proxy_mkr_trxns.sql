{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_pause_proxy_mkr_trxns"
    )
}}

WITH pause_proxy_mkr_trxns_preunion AS (
    SELECT
        raw.ts,
        raw.hash,
        CASE 
            WHEN vest.vested IS NOT NULL THEN 32120 -- reserved surplus depletion for vested transactions
            ELSE 32110 -- direct protocol surplus impact for non-vested transactions
        END AS code,
        -raw.expense / 1e18 AS value
    FROM {{ ref('fact_pause_proxy_mkr_trxns_raw') }} raw
    LEFT JOIN {{ ref('fact_mkr_vest_tx') }} vest
        ON raw.hash = vest.hash
)

SELECT
    ts,
    hash,
    code,
    value
FROM pause_proxy_mkr_trxns_preunion

UNION ALL

SELECT
    ts,
    hash,
    32210 AS code, -- MKR contra equity
    -value
FROM pause_proxy_mkr_trxns_preunion