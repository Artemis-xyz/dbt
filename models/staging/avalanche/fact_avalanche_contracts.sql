{{ config(materialized="incremental", unique_key="date") }}

select
    'avalanche' as chain,
    date_trunc(week, block_timestamp) as date,
    count(distinct from_address) as contract_deployers,
    count(*) as contracts_deployed
from avalanche_flipside.core.fact_transactions
where
    to_address is null
    {% if is_incremental() %}
        and block_timestamp >= (select max(date) from {{ this }})
    {% endif %}
group by date
order by date desc
