{{ config(snowflake_warehouse="CELESTIA") }}
with fees as (
    SELECT
        date,
        fees_tia
    FROM  {{ ref("fact_celestia_txn_count_and_fees_silver") }}
),
prices as (
    {{ get_coingecko_price_with_latest('celestia')}}
)
SELECT
    p.date,
    f.fees_tia * p.price as fees
FROM fees f
LEFT JOIN prices p on p.date = f.date
order by 1 desc