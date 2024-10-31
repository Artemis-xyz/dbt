{{ config(
    materialized= "table",
    snowflake_warehouse="METAPLEX"
) }}

WITH daily_balances AS (
    SELECT
        DATE_TRUNC('day', block_timestamp) AS date,
        account_address,
        mint,
        MAX(balance) AS ending_balance
    FROM
        {{ source('SOLANA_FLIPSIDE', 'fact_token_balances') }}
    WHERE
        owner = 'E7Hzc1cQwx5BgJa8hJGVuDF2G2f2penLrhiKU6nU53gK'
        AND mint = 'METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m'
        AND succeeded = TRUE
        {% if is_incremental() %}
            AND block_timestamp > (SELECT MAX(date) FROM {{ this }})
        {% endif %}
    GROUP BY
        DATE_TRUNC('day', block_timestamp),
        account_address,
        mint
)
, prices AS (
    SELECT
        date(hour) AS date,
        symbol,
        avg(price) as price
    FROM
        {{source('SOLANA_FLIPSIDE_PRICE', 'ez_prices_hourly')}}
    WHERE token_address = 'METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m'
    GROUP BY 1, 2
)

SELECT
    b.date,
    p.symbol,
    b.ending_balance,
    b.ending_balance - LAG(b.ending_balance) OVER (ORDER BY b.date DESC) AS buyback_native,
    buyback_native * p.price AS buyback_usd
FROM
    daily_balances b
LEFT JOIN
    prices p
    ON b.date = p.date
ORDER BY
    b.date DESC
