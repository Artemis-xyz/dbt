{{ config(materialized="incremental", unique_key="date") }}

select
    count(distinct(signers)) daa,
    date_trunc('day', block_timestamp) as date,
    count(*) txns,
    'solana' as chain
from solana_flipside.core.fact_transactions

where
    succeeded = 'TRUE'
    {% if is_incremental() %}
        and block_timestamp >= (select max(date) from {{ this }})
    {% endif %}
    and block_timestamp is not null

group by 2
order by 2 asc
