{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="raw",
        alias="fact_uniswap_treasury_by_token",
    )
}}

WITH dates AS (
    SELECT
        DISTINCT DATE(hour) AS date
    FROM
        ethereum_flipside.price.ez_prices_hourly
    WHERE
        symbol = 'UNI'
),
treasury_addresses AS (
    SELECT
        addresses
    FROM
        (
            VALUES
                (
                    LOWER('0x1a9C8182C09F50C8318d769245beA52c32BE35BC')
                ),
                (
                    LOWER('0x3D30B1aB88D487B0F3061F40De76845Bec3F1e94')
                ),
                (
                    LOWER('0x4750c43867EF5F89869132ecCF19B9b6C4286E1a')
                ),
                (
                    LOWER('0x4b4e140D1f131fdaD6fb59C13AF796fD194e4135')
                ),
                (
                    LOWER('0xe3953D9d317B834592aB58AB2c7A6aD22b54075D')
                )
        ) AS treasury_addresses(addresses)
),
tokens AS (
    SELECT
        DISTINCT LOWER(contract_address) AS token_address
    FROM
        ethereum_flipside.core.ez_token_transfers
    WHERE
        LOWER(to_address) IN (
            SELECT
                addresses
            FROM
                treasury_addresses
        )
),
sparse_balances AS (
    SELECT
        DATE(block_timestamp) AS date,
        user_address,
        contract_address,
        MAX_BY(balance, block_timestamp) / 1e18 AS balance_daily
    FROM
        ethereum_flipside.core.fact_token_balances
    WHERE
        LOWER(contract_address) IN (
            SELECT
                token_address
            FROM
                tokens
        )
        AND LOWER(user_address) IN (
            SELECT
                addresses
            FROM
                treasury_addresses
        )
    GROUP BY
        1,
        2,
        3
),
full_balances AS (
    SELECT
        d.date,
        ta.addresses AS user_address,
        t.token_address AS contract_address,
        COALESCE(
            LAST_VALUE(sb.balance_daily) IGNORE NULLS OVER (
                PARTITION BY ta.addresses,
                t.token_address
                ORDER BY
                    d.date ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ),
            0
        ) AS balance_daily
    FROM
        dates d
        CROSS JOIN treasury_addresses ta
        CROSS JOIN tokens t
        LEFT JOIN sparse_balances sb ON d.date = sb.date
        AND ta.addresses = sb.user_address
        AND t.token_address = sb.contract_address
),
daily_prices AS (
    SELECT
        DATE(hour) AS date,
        token_address,
        symbol,
        AVG(price) AS avg_daily_price,
        MAX(decimals) as decimals
    FROM
        ethereum_flipside.price.ez_prices_hourly
    WHERE
        token_address IN (
            SELECT
                token_address
            FROM
                tokens
        )
    GROUP BY
        1,
        2,
        3
),
full_table as (
    SELECT
        fb.date,
        fb.user_address,
        fb.contract_address,
        dp.symbol,
        fb.balance_daily as balance_daily,
        COALESCE(dp.avg_daily_price, 0) AS avg_daily_price,
        fb.balance_daily * COALESCE(dp.avg_daily_price, 0) AS usd_balance
    FROM
        full_balances fb
        LEFT JOIN daily_prices dp ON fb.date = dp.date
        AND fb.contract_address = dp.token_address -- AND dp.decimals is not null
        -- and dp.decimals > 0
    WHERE
        symbol is not null
)
SELECT
    date,
    symbol,
    SUM(balance_daily) as balance_daily,
    SUM(usd_balance) as usd_balance
FROM
    full_table
WHERE
    USD_BALANCE > 1
GROUP BY
    1
    , 2
ORDER BY
    1 DESC