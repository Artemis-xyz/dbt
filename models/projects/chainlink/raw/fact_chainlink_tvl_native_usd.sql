{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_tvl_native_usd",

    )
}}

WITH filtered_balances AS (
    SELECT
        DATE(block_timestamp) AS date,
        address,
        MAX_BY(balance_token / 1e18, block_timestamp) AS balance_token
    FROM pc_dbt_db.prod.fact_ethereum_address_balances_by_token
    WHERE contract_address = '0x514910771af9ca656af840dff83e8264ecf986ca' -- set token contract address
    and lower(address) in (lower('0xBc10f2E862ED4502144c7d632a3459F49DFCDB5e'), lower('0xA1d76A7cA72128541E9FCAcafBdA3a92EF94fDc5'),
    lower('0x3feB1e09b4bb0E7f0387CeE092a52e85797ab889'))
    GROUP BY 1, 2
),
unique_dates AS (
    SELECT DISTINCT DATE(block_timestamp) AS date
    FROM pc_dbt_db.prod.fact_ethereum_address_balances_by_token
    where block_timestamp > '2022-12-06' -- set token contract creation date
),
addresses AS (
    SELECT DISTINCT address
    FROM filtered_balances
),
all_combinations AS (
    SELECT
        ud.date,
        a.address
    FROM unique_dates ud
    CROSS JOIN addresses a
)
, joined_balances AS (
    SELECT
        ac.date,
        ac.address,
        fb.balance_token
    FROM all_combinations ac
    LEFT JOIN filtered_balances fb
        ON ac.date = fb.date
        AND ac.address = fb.address
),
prices as(
    SELECT 
        date(hour) as date,
        AVG(price) as price
    FROM ethereum_flipside.price.ez_prices_hourly 
    where lower(token_address) = lower('0x514910771AF9Ca656af840dff83E8264EcF986CA')
    group by 1
),
filled_balances as(
    SELECT
        j.date,
        address,
        COALESCE(
            balance_token,
            LAST_VALUE(balance_token IGNORE NULLS) OVER (
                PARTITION BY address ORDER BY j.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS balance_token,
        COALESCE(
            balance_token,
            LAST_VALUE(balance_token IGNORE NULLS) OVER (
                PARTITION BY address ORDER BY j.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) * p.price AS balance_usd
    FROM joined_balances j
    LEFT JOIN prices p on p.date = j.date
    order by j.date desc
)
SELECT
    date,
    SUM(balance_usd) as balance_usd,
    SUM(balance_token) as balance_link
FROM filled_balances
GROUP BY date
ORDER BY date desc