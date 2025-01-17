{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

with raw as (
    select
        date(block_timestamp) as date,
        max_by(decoded_log:price::number /1e8, block_timestamp) as rate
    from ethereum_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x4c48bcb2160F8e0aDbf9D4F3B034f1e36d1f8b3e')
    and event_name = 'BalanceReported'
    group by 1
)
, dates as (
    SELECT date FROM
    pc_dbt_db.prod.dim_date_spine
    WHERE date between
        (SELECT MIN(date) FROM raw)
        AND to_date(sysdate())
)
, sparse as (
    SELECT
        d.date,
        rate
    FROM dates d
    LEFT JOIN raw s on d.date = s.date
)
SELECT
 date,
 COALESCE(rate, LAST_VALUE(rate IGNORE NULLS) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as rate
FROM sparse