{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_mkr_burns"
    )
}}

WITH mkr_burns_preunioned AS (
    SELECT
        block_timestamp AS ts,
        tx_hash AS hash,
        SUM(CAST(rad AS DOUBLE)) AS value
    FROM ethereum_flipside.maker.fact_vat_move
    WHERE src_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- vow
    GROUP BY block_timestamp, tx_hash
)

SELECT
    ts,
    hash,
    31420 AS code,
    -value AS value --decreased equity
FROM mkr_burns_preunioned

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    value --increased liability
FROM mkr_burns_preunioned