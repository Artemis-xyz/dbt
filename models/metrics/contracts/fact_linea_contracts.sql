{{ config(materialized="incremental", unique_key="date") }}

select
    'linea' as chain,
    date_trunc('week', to_timestamp(block_timestamp)::date) as date,
    count(distinct(from_address)) as contract_deployers,
    count(*) as contracts_deployed
from {{ ref("fact_linea_transactions") }}
where
    to_address is null
    {% if is_incremental() %}
        and to_timestamp(block_timestamp)::date >= (select max(date) from {{ this }})
    {% endif %}
group by date
order by date desc
