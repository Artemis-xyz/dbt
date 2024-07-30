{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_uni_lp_value"
    )
}}

with token_balances as (
    SELECT
        DATE(block_timestamp) as date,
        case
            when contract_address = lower('0x6B175474E89094C44Da98b954EedeAC495271d0F') then 'DAI'
            when contract_address = lower('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2') then 'MKR'
        END AS token,
        MAX_BY(balance, block_timestamp)::number / 1e18 as balance
    FROM
        ethereum_flipside.core.fact_token_balances
    WHERE
        user_address = lower('0x517F9dD285e75b599234F7221227339478d0FcC8')
        AND contract_address in (
            lower('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2'),
            lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')
        )
    GROUP BY
        1,
        2
),
prices as (
    SELECT
        date(hour) as date,
        MAX_BY(price, hour) as price,
        symbol
    FROM
        ethereum_flipside.price.ez_prices_hourly
    WHERE
        token_address in (
            lower('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2'),
            lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')
        )
    GROUP BY
        1,
        3
),
daily_prices_by_token as (
    SELECT
        b.date,
        p.price * b.balance as balance_usd,
        b.token
    FROM
        token_balances b
        LEFT JOIN prices p on p.date = b.date
        AND p.symbol = b.token
)
SELECT
    date,
    sum(balance_usd) as amount_usd
FROM 
    daily_prices_by_token
GROUP BY 1
ORDER BY 1 DESC