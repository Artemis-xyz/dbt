{{ config(
    materialized= "table",
    snowflake_warehouse="METAPLEX"
) }}

WITH daily_balances AS (
    SELECT
        DATE_TRUNC('day', block_timestamp) AS block_date,
        account_address,
        mint,
        MAX(balance) AS ending_balance
    FROM
        solana_flipside.core.fact_token_balances
    WHERE
        owner = 'E7Hzc1cQwx5BgJa8hJGVuDF2G2f2penLrhiKU6nU53gK'
        AND mint = 'METAewgxyPbgwsseH8T16a39CQ5VyVxZi9zXiDPY18m'
        AND succeeded = TRUE
    GROUP BY
        DATE_TRUNC('day', block_timestamp),
        account_address,
        mint
)

SELECT
    block_date,
    ending_balance
FROM
    daily_balances
ORDER BY
    block_date DESC
