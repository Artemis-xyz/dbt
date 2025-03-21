{{
    config(
        materialized="table",
        snowflake_warehouse="BAM_TRANSACTION_XLG"
    )
}}
-- This table is used for 30-day onchain explorer metric recalculation

select * from {{ ref("fact_solana_transactions_v2") }}
where raw_date < to_date(sysdate())
    and block_timestamp
    >= dateadd('day', -30, to_date(sysdate()))
