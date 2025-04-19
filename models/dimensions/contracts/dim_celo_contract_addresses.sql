{{
    config(
        materialized='incremental',
        unique_key='contract_address',
        snowflake_warehouse="CELO"
    )
}}
select min(block_timestamp) as block_timestamp, to_address as contract_address, 'CREATE' as type -- Celo does not currently support CREATE2
from {{ref("fact_celo_transactions")}}
where to_address is null
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
group by contract_address