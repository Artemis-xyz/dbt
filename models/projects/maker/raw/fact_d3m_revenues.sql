{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_d3m_revenues"
    )
}}

WITH d3m_revenues_preunion AS (
    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        CASE
            WHEN src_address = '0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a' THEN 'DIRECT-AAVEV2-DAI'
            WHEN src_address = '0x621fe4fde2617ea8ffade08d0ff5a862ad287ec2' THEN 'DIRECT-COMPV2-DAI'
        END AS ilk,
        SUM(CAST(rad AS DOUBLE)) AS value
    FROM ethereum_flipside.maker.fact_vat_move
    WHERE src_address IN (
        '0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a',  -- aave d3m
        '0x621fe4fde2617ea8ffade08d0ff5a862ad287ec2'   -- compound v2 d3m
    )
    AND dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'  -- vow
    GROUP BY 1, 2, 3

    UNION ALL

    SELECT 
        block_timestamp AS ts,
        tx_hash AS hash,
        ilk,
        SUM(dart) / 1e18 AS value
    FROM {{ ref('fact_vat_grab')}}
    WHERE dart > 0
    GROUP BY 1, 2, 3
)

SELECT
    ts,
    hash,
    31160 AS code,
    value AS value,
    ilk
FROM d3m_revenues_preunion

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    -value AS value,
    ilk
FROM d3m_revenues_preunion