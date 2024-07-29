{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_other_sin_outflows"
    )
}}

WITH other_sin_outflows_raw AS (
    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        CAST(rad AS DOUBLE) AS value
    FROM ethereum_flipside.maker.fact_vat_suck
    WHERE u_address = '0xa950524441892a31ebddf91d3ceefa04bf454466' -- Vow
      AND v_address NOT IN (
        '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7', -- Pot (DSR)
        '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb', -- Pause Proxy
        '0x2cc583c0aacdac9e23cb601fda8f1a0c56cdcb71', -- Old Pause Proxy
        '0xa4c22f0e25c6630b2017979acf1f865e94695c4b'  -- Old Pause Proxy
      )
)

SELECT
    ts,
    hash,
    31520 AS code,
    -value AS value --reduced equity
FROM other_sin_outflows_raw

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    value AS value --increased liability
FROM other_sin_outflows_raw