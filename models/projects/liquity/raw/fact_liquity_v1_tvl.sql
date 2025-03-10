{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='raw',
        alias='fact_liquity_v1_tvl'
    )
}}

WITH traces AS (
    SELECT
        block_timestamp::date AS date,
        SUM(
            CASE
                WHEN to_address = lower('0xdf9eb223bafbe5c5271415c75aecd68c21fe3d7f') THEN value
                ELSE -(value)
            END
        ) AS val
    FROM
        ethereum_flipside.core.fact_traces
    WHERE 1=1
        AND tx_succeeded = true
        AND (
            to_address = lower('0xdf9eb223bafbe5c5271415c75aecd68c21fe3d7f')
            OR from_address = lower('0xdf9eb223bafbe5c5271415c75aecd68c21fe3d7f')
        )
    GROUP BY
        block_timestamp::date
    ORDER BY
        1
)
SELECT
    t.date,
    'ethereum' as chain,
    'v1' as version,
    'Liquity' as app,
    'ETH' as token,
    SUM(val) OVER (
        ORDER BY
            t.date
    ) AS tvl_native,
    SUM(val) OVER (
        ORDER BY
            t.date
    ) * p.price AS tvl_usd
FROM
    traces t
    LEFT JOIN ethereum_flipside.price.ez_prices_hourly p ON p.hour = t.date
WHERE
    1 = 1
    AND is_native