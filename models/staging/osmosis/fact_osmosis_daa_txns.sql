{{ config(materialized="incremental", unique_key="date") }}
select
    block_timestamp::date as date,
    count(distinct tx_from) as daa,
    count(*) as txns,
    'osmosis' as chain
from osmosis_flipside.core.fact_transactions
{% if is_incremental() %}
    where block_timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
{% endif %}
group by date
