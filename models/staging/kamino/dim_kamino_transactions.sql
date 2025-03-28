{{ config(materialized="table", snowflake_warehouse="KAMINO") }}

with expanded_signers AS (
    select
        date_trunc('day', block_timestamp) AS date,
        tx_id,
        s.value::string AS signer
from solana_flipside.core.ez_events_decoded,
    lateral flatten(input => signers) as s
where program_id = 'KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD'
)

select
    date, 
    count(distinct tx_id) as tx_count,
    count(distinct signer) as dau
from expanded_signers
group by date
order by date desc

