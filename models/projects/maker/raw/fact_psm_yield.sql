{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_psm_yield"
    )
}}

WITH psm_yield_preunioned AS (
    SELECT 
        vat.block_timestamp AS ts,
        vat.tx_hash AS hash,
        tx.ilk,
        SUM(vat.rad) AS value
    FROM ethereum_flipside.maker.fact_vat_move vat
    INNER JOIN {{ ref('fact_psm_yield_tx') }} tx
        ON vat.tx_hash = tx.tx_hash
    WHERE vat.dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
    GROUP BY vat.block_timestamp, vat.tx_hash, tx.ilk
)

SELECT 
    ts,
    hash,
    31180 AS code,
    value, --increased equity
    ilk
FROM psm_yield_preunioned

UNION ALL

SELECT 
    ts,
    hash,
    21120 AS code,
    -value AS value, --decreased liability
    ilk
FROM psm_yield_preunioned