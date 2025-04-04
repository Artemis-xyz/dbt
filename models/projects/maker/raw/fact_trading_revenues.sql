{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_trading_revenues"
    )
}}

WITH trading_revenues_preunion AS (
    SELECT 
        vat.block_timestamp AS ts,
        vat.tx_hash AS hash,
        psms.ilk,
        SUM(CAST(vat.rad AS DOUBLE)) AS value
    FROM ethereum_flipside.maker.fact_vat_move vat
    INNER JOIN {{ ref('dim_psms') }} psms
        ON vat.src_address = psms.psm_address
    WHERE vat.dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
    GROUP BY vat.block_timestamp, vat.tx_hash, psms.ilk
)

SELECT
    ts,
    hash,
    31310 AS code,
    value AS value,
    ilk
FROM trading_revenues_preunion

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -value AS value,
    ilk
FROM trading_revenues_preunion