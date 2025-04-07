{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_mkr_mints"
    )
}}

WITH mkr_mints_preunioned AS (
    SELECT 
        vat.block_timestamp AS ts,
        vat.tx_hash AS hash,
        SUM(vat.rad) AS value
    FROM ethereum_flipside.maker.fact_vat_move vat
    JOIN {{ ref('fact_liquidation_excluded_tx') }} tx
        ON vat.tx_hash = tx.tx_hash
    WHERE vat.dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
      AND vat.src_address NOT IN (SELECT contract_address FROM {{ ref('dim_maker_contracts') }} WHERE contract_type = 'PSM')
    GROUP BY vat.block_timestamp, vat.tx_hash
)

SELECT
    ts,
    hash,
    31410 AS code,
    value --increased equity
FROM mkr_mints_preunioned

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -value AS value --decreased liability
FROM mkr_mints_preunioned