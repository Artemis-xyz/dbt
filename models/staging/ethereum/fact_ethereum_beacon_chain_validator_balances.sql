{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}


with daily_balances as (
    SELECT
        b.slot_timestamp::date as date,
        index,
        max_by(balance, v.block_number) as balance
    FROM
        ethereum_flipside.beacon_chain.fact_validator_balances v
        LEFT JOIN ethereum_flipside.beacon_chain.fact_blocks b ON b.slot_number = v.slot_number
    GROUP BY
        1,
        2
    ORDER by
        date desc,
        2 desc
)
SELECT date, sum(balance) as balance_native FROM daily_balances GROUP BY 1