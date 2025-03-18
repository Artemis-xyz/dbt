{{ config(materialized="table", snowflake_warehouse="KAMINO") }}

with expanded_signers AS (
    select
        date_trunc('day', block_timestamp) AS date,
        s.value::string AS signer
from solana_flipside.core.ez_events_decoded,
    lateral flatten(input => signers) as s
where program_id = 'KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD' and date > '2025-03-10'
)

select
    date,
    count(distinct signer) AS distinct_signers_count
from expanded_signers
group by date
order by date desc