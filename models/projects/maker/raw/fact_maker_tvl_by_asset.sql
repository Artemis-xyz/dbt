{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_maker_tvl_by_asset"
    )
}}


WITH weth AS (
    SELECT DISTINCT
        m.symbol,
        g.gem_address,
        g.join_address,
        m.decimals
    FROM
        {{ ref('dim_gem_join_addresses') }} g
        LEFT JOIN ethereum_flipside.price.ez_asset_metadata m ON g.gem_address = m.token_address
    WHERE
        symbol IS NOT NULL
),
daily_balances AS (
    SELECT
        DATE(t.block_timestamp) AS date,
        t.user_address,
        t.contract_address,
        w.symbol,
        w.decimals,
        AVG(t.balance / POWER(10, w.decimals)) AS amount_native
    FROM
        ethereum_flipside.core.fact_token_balances t
        JOIN weth w ON t.user_address = w.join_address AND t.contract_address = w.gem_address
    GROUP BY 1, 2, 3, 4, 5
),
date_series AS (
    SELECT date_day as date 
    FROM ethereum_flipside.core.dim_dates
    WHERE date_day < to_date(sysdate())
),
all_combinations AS (
    SELECT DISTINCT
        d.date,
        db.user_address,
        db.contract_address
    FROM
        date_series d
        CROSS JOIN (SELECT DISTINCT user_address, contract_address FROM daily_balances) db
),
forward_filled_balances AS (
    SELECT
        ac.date,
        ac.user_address,
        ac.contract_address,
        LAST_VALUE(db.symbol IGNORE NULLS) OVER (
            PARTITION BY ac.user_address, ac.contract_address
            ORDER BY ac.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS symbol,
        LAST_VALUE(db.decimals IGNORE NULLS) OVER (
            PARTITION BY ac.user_address, ac.contract_address
            ORDER BY ac.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS decimals,
        LAST_VALUE(db.amount_native IGNORE NULLS) OVER (
            PARTITION BY ac.user_address, ac.contract_address
            ORDER BY ac.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS amount_native
    FROM
        all_combinations ac
        LEFT JOIN daily_balances db 
            ON ac.date = db.date 
            AND ac.user_address = db.user_address 
            AND ac.contract_address = db.contract_address 
),
usd_values AS (
    SELECT
        ffb.date,
        ffb.user_address,
        ffb.contract_address,
        ffb.symbol,
        ffb.amount_native,
        ffb.amount_native * COALESCE(
            p.price,
            FIRST_VALUE(p.price) OVER (
                PARTITION BY ffb.contract_address
                ORDER BY CASE WHEN p.price IS NOT NULL THEN ffb.date END DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS amount_usd
    FROM
        forward_filled_balances ffb
        LEFT JOIN ethereum_flipside.price.ez_prices_hourly p 
            ON p.hour = DATE_TRUNC('day', ffb.date) 
            AND p.token_address = ffb.contract_address
    WHERE
        ffb.amount_native IS NOT NULL
)
SELECT
    date,
    SUM(amount_native) AS total_amount_native,
    symbol,
    SUM(amount_usd) AS total_amount_usd
FROM
    usd_values
GROUP BY 1, 3
ORDER BY 1