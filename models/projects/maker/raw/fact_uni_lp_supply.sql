{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_uni_lp_supply"
    )
}}

WITH token_transfers AS (
    SELECT
        date(block_timestamp) AS date,
        from_address,
        to_address,
        raw_amount_precise::number / 1e18 AS amount
    FROM ethereum_flipside.core.ez_token_transfers
    WHERE contract_address = LOWER('0x517F9dD285e75b599234F7221227339478d0FcC8')
),
daily_mints AS (
    SELECT
        date,
        SUM(amount) AS daily_minted
    FROM token_transfers
    WHERE from_address = LOWER('0x0000000000000000000000000000000000000000')
    GROUP BY date
),
daily_burns AS (
    SELECT
        date,
        SUM(amount) AS daily_burned
    FROM token_transfers
    WHERE to_address = LOWER('0x0000000000000000000000000000000000000000')
    GROUP BY date
),
dim_dates AS (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    WHERE date < to_date(sysdate())
),
daily_net_supply AS (
    SELECT
        d.date,
        COALESCE(m.daily_minted, 0) AS daily_minted,
        COALESCE(b.daily_burned, 0) AS daily_burned,
        COALESCE(m.daily_minted, 0) - COALESCE(b.daily_burned, 0) AS daily_net
    FROM dim_dates d
    LEFT JOIN daily_mints m ON d.date = m.date
    LEFT JOIN daily_burns b ON d.date = b.date
    WHERE d.date < to_date(sysdate())
),
cumulative_supply AS (
    SELECT
        date,
        daily_minted,
        daily_burned,
        daily_net,
        SUM(daily_net) OVER (
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS circulating_supply
    FROM daily_net_supply
)
SELECT 
    date,
    circulating_supply
FROM cumulative_supply
ORDER BY date