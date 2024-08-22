{{
    config(
        materialized="table"
    )
}}

with daily as (
    select
        date(block_timestamp) as date
        ,SUM(case when to_address = '0x0000000000000000000000000000000000000000'
                then -amount
            when from_address = '0x0000000000000000000000000000000000000000'
                then amount
        END) as daily_change
    from
    ethereum_flipside.core.ez_token_transfers
    WHERE contract_address = lower('0xae78736cd615f374d3085123a210448e74fc6393')
    GROUP BY 1
)
, dates as (
    SELECT distinct date(hour) as date
    FROM ethereum_flipside.price.ez_prices_hourly
    WHERE hour > (SELECT MIN(date) FROM daily)
)
, running_sum AS (
    SELECT
        dates.date,
        SUM(COALESCE(daily_change, 0)) OVER (ORDER BY dates.date) AS cumulative_sum
    FROM dates
    LEFT JOIN daily ON daily.date = dates.date
)
SELECT
    date,
    LAST_VALUE(cumulative_sum IGNORE NULLS) OVER (
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS reth_supply
FROM running_sum