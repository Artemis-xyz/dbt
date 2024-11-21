{{
    config(
        materialized="table",
        snowflake_warehouse="RWA",
    )
}}

select
    date(block_timestamp) as date,
    max_by(decoded_log:price::number /1e8, block_timestamp) as rate
from ethereum_flipside.core.ez_decoded_event_logs
where contract_address = lower('0x4c48bcb2160F8e0aDbf9D4F3B034f1e36d1f8b3e')
and event_name = 'BalanceReported'
group by 1
