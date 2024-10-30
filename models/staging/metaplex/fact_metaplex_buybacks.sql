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

SELECT
    date,
    ending_balance,
    ending_balance - LAG(ending_balance) OVER (ORDER BY date DESC) AS buyback_amount
FROM
    daily_balances
ORDER BY
    date DESC
