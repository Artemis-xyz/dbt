{{ config(materialized="table", snowflake_warehouse="KAMINO") }}

select
    date_trunc('day', block_timestamp) as date, 
    count(distinct tx_id) as tx_count, 
from solana_flipside.core.ez_events_decoded
where program_id = 'KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD'
group by date
order by date desc

