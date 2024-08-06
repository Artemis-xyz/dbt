{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_treasury_lp_balances"
    )
}}


with
dates as (
    SELECT
        DISTINCT date(hour) as date
    FROM ethereum_flipside.price.ez_prices_hourly
    WHERE symbol = 'MKR'
)
, treasury_balance as (
    select
        date(block_timestamp) as date,
        MAX(balance)::number / 1e18 as treasury_lp_balance
    from
        ethereum_flipside.core.fact_token_balances
    where
        contract_address = LOWER('0x517F9dD285e75b599234F7221227339478d0FcC8')
        and user_address = lower('0xBE8E3e3618f7474F8cB1d074A26afFef007E98FB')
    GROUP BY
        1
),
value_per_token_cte as (
    SELECT
        s.date,
        v.amount_usd / s.circulating_supply as value_per_token,
        s.circulating_supply
    FROM
        {{ ref('fact_uni_lp_supply') }} s
        LEFT JOIN {{ ref('fact_uni_lp_value') }} v ON v.date = s.date
    where
        value_per_token is not null
)
, filled_data as (
    SELECT
        d.date,
        LAST_VALUE(t.treasury_lp_balance IGNORE NULLS) OVER (
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as amount_native,
        LAST_VALUE(v.value_per_token IGNORE NULLS) OVER (
            ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as value_per_token
    FROM
        dates d
        LEFT JOIN treasury_balance t ON d.date = t.date
        LEFT JOIN value_per_token_cte v ON d.date = v.date
)
SELECT
    date,
    amount_native,
    value_per_token,
    amount_native * value_per_token as amount_usd,
    'UNI V2: DAI-MKR' as token
FROM
    treasury_balance t
