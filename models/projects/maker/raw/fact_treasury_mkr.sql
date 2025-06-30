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
        token_address,
        symbol,
        AVG(price) as price
    FROM ethereum_flipside.price.ez_prices_hourly
    WHERE token_address in (
        lower('0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2') -- MKR
        , lower('0x56072C95FAA701256059aa122697B133aDEd9279')
        ) 
    GROUP BY 1, 2, 3
),
mkr_balance_cte as (
    SELECT
        date(block_timestamp) as date,
        contract_address,
        user_address,
        MAX_BY(balance,block_timestamp)/1e18 as balance
    FROM ethereum_flipside.core.fact_token_balances
    where user_address in (lower('0xBE8E3e3618f7474F8cB1d074A26afFef007E98FB'), lower('0x8EE7D9235e01e6B42345120b5d270bdB763624C7'), lower('0x7Bb0b08587b8a6B8945e09F1Baca426558B0f06a'))
    and contract_address in (
        lower('0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2') -- MKR
        , lower('0x56072C95FAA701256059aa122697B133aDEd9279')
        )
    GROUP BY 1, 2, 3
),
date_sequence AS (
    SELECT DISTINCT date
    FROM pc_dbt_db.prod.dim_date_spine
    WHERE date between (SELECT MIN(date) FROM mkr_balance_cte) and to_date(sysdate())
),
user_addresses_contracts AS (
    SELECT DISTINCT user_address, contract_address
    FROM mkr_balance_cte
),
all_dates_users AS (
    SELECT
        d.date,
        contract_address,
        u.user_address
    FROM date_sequence d
    CROSS JOIN user_addresses_contracts u
)
, joined_balances AS (
    SELECT
        a.date,
        a.contract_address,
        a.user_address,
        p.price,
        p.symbol,
        m.balance AS balance_token
    FROM all_dates_users a
    LEFT JOIN prices p ON p.date = a.date and lower(p.token_address) = lower(a.contract_address)
    LEFT JOIN mkr_balance_cte m ON m.date = a.date AND m.user_address = a.user_address AND m.contract_address = a.contract_address
)
,filled_balances AS (
    SELECT
        date,
        user_address,
        contract_address,
        price,
        symbol,
        COALESCE(
            balance_token,
            LAST_VALUE(balance_token IGNORE NULLS) OVER (
                PARTITION BY user_address, contract_address
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS balance_token,
        COALESCE(
            user_address,
            LAST_VALUE(user_address IGNORE NULLS) OVER (
                PARTITION BY user_address, contract_address
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS filled_user_address
    FROM joined_balances
)

SELECT
    date,
    price,
    user_address,
    IFF(symbol='SKY',symbol, 'MKR') as token,
    SUM(balance_token) amount_native,
    SUM(balance_token *price) as amount_usd
FROM filled_balances
GROUP BY date, token, price, user_address
