{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_treasury_mkr"
    )
}}

with prices as (
    SELECT
        date(hour) as date,
        AVG(price) as price
    FROM ethereum_flipside.price.ez_prices_hourly
    WHERE symbol = 'MKR'
    GROUP BY 1
),
mkr_balance_cte as (
    SELECT
        date(block_timestamp) as date,
        MAX_BY(balance,date(block_timestamp))/1e18 as mkr_balance,
        user_address
    FROM ethereum_flipside.core.fact_token_balances
    where user_address in (lower('0xBE8E3e3618f7474F8cB1d074A26afFef007E98FB'), lower('0x8EE7D9235e01e6B42345120b5d270bdB763624C7'))
    and contract_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2'
    GROUP BY 1, 3
),
date_sequence AS (
    SELECT DISTINCT date
    FROM prices
),
user_addresses AS (
    SELECT DISTINCT user_address
    FROM mkr_balance_cte
),
all_dates_users AS (
    SELECT
        d.date,
        u.user_address
    FROM date_sequence d
    CROSS JOIN user_addresses u
),
joined_balances AS (
    SELECT
        a.date,
        a.user_address,
        p.price,
        m.mkr_balance AS balance_token
    FROM all_dates_users a
    LEFT JOIN prices p ON p.date = a.date
    LEFT JOIN mkr_balance_cte m ON m.date = a.date AND m.user_address = a.user_address
)
,filled_balances AS (
    SELECT
        date,
        user_address,
        price,
        COALESCE(
            balance_token,
            LAST_VALUE(balance_token IGNORE NULLS) OVER (
                PARTITION BY user_address
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS balance_token,
        COALESCE(
            user_address,
            LAST_VALUE(user_address IGNORE NULLS) OVER (
                PARTITION BY user_address
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS filled_user_address
    FROM joined_balances
)

SELECT
    date,
    SUM(balance_token) amount_mkr,
    SUM(balance_token *price) as amount_usd,
    'MKR' as token
FROM filled_balances
GROUP BY date