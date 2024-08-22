{{ config(materialized="table") }}

WITH prices AS (
    SELECT
        hour,
        price
    FROM
        ethereum_flipside.price.ez_prices_hourly
    WHERE
        is_native = True
),
cte AS (
    SELECT
        date(block_timestamp) AS date,
        NULL AS el_fees_smoothingpool_realized_users_eth,
        NULL AS el_fees_smoothingpool_realized_users_usd,
        NULL AS el_fees_smoothingpool_realized_stakers_eth,
        NULL AS el_fees_smoothingpool_realized_stakers_usd,
        sum(value) AS el_fees_smoothingpool_accrued_eth,
        sum(value * p.price) AS el_fees_smoothingpool_accrued_usd
    FROM
        ethereum_flipside.core.fact_transactions
        LEFT JOIN prices p ON p.hour = date_trunc('hour', block_timestamp)
    WHERE
        to_address = lower('0xd4E96eF8eee8678dBFf4d535E033Ed1a4F7605b7')
    GROUP BY
        1
    UNION ALL
    SELECT
        date(block_timestamp) AS date,
        SUM(
            CASE
                WHEN to_address = lower('0xae78736Cd615f374D3085123A210448E74Fc6393') THEN value
                ELSE 0
            END
        ) AS el_fees_smoothingpool_realized_users_eth,
        SUM(
            CASE
                WHEN to_address = lower('0xae78736Cd615f374D3085123A210448E74Fc6393') THEN value * p.price
                ELSE 0
            END
        ) AS el_fees_smoothingpool_realized_users_usd,
        SUM(
            CASE
                WHEN to_address <> lower('0xae78736Cd615f374D3085123A210448E74Fc6393') THEN value
                ELSE 0
            END
        ) AS el_fees_smoothingpool_realized_stakers_eth,
        SUM(
            CASE
                WHEN to_address <> lower('0xae78736Cd615f374D3085123A210448E74Fc6393') THEN value * p.price
                ELSE 0
            END
        ) AS el_fees_smoothingpool_realized_stakers_usd,
        NULL AS el_fees_smoothingpool_accrued_eth,
        NULL AS el_fees_smoothingpool_accrued_usd
    FROM
        ethereum_flipside.core.fact_traces
        LEFT JOIN prices p ON p.hour = date_trunc('hour', block_timestamp)
    WHERE
        from_address = lower('0xd4E96eF8eee8678dBFf4d535E033Ed1a4F7605b7')
    GROUP BY
        1
)
SELECT
    date,
    SUM(el_fees_smoothingpool_realized_users_eth) AS el_fees_smoothingpool_realized_users_eth,
    SUM(el_fees_smoothingpool_realized_users_usd) AS el_fees_smoothingpool_realized_users_usd,
    SUM(el_fees_smoothingpool_realized_stakers_eth) AS el_fees_smoothingpool_realized_stakers_eth,
    SUM(el_fees_smoothingpool_realized_stakers_usd) AS el_fees_smoothingpool_realized_stakers_usd,
    SUM(el_fees_smoothingpool_accrued_eth) AS el_fees_smoothingpool_accrued_eth,
    SUM(el_fees_smoothingpool_accrued_usd) AS el_fees_smoothingpool_accrued_usd
FROM
    cte
GROUP BY
    date