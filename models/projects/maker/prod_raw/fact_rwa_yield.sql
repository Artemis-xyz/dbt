{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_rwa_yield"
    )
}}

WITH rwa_yield_preunioned AS (
    SELECT 
        vat.block_timestamp AS ts,
        vat.tx_hash AS hash,
        tx.ilk,
        SUM(vat.rad) AS value
    FROM ethereum_flipside.maker.fact_vat_move vat
    JOIN {{ ref('fact_rwa_yield_tx') }} tx
        ON vat.tx_hash = tx.tx_hash
    WHERE vat.dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
    GROUP BY vat.block_timestamp, vat.tx_hash, tx.ilk
)

SELECT 
    ts,
    hash,
    COALESCE(ilm.equity_code, 31170) AS code, --default to off-chain private credit
    value, --increased equity
    rwy.ilk
FROM rwa_yield_preunioned rwy
LEFT JOIN {{ ref('dim_ilk_list_manual_input') }} ilm
    USING (ilk)

UNION ALL

SELECT 
    ts,
    hash,
    21120 AS code,
    -value AS value, --decreased liability
    ilk
FROM rwa_yield_preunioned