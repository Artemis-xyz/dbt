{{
    config(
        materialized='incremental',
        unique_key='contract_address',
        snowflake_warehouse="CELO_LG"
    )
}}
select min(block_timestamp) as block_timestamp, to_address::string as contract_address, max(upper(trace_type)) as type -- Celo does not currently support CREATE2
from {{ref("fact_celo_traces")}}
where trace_type in ('create', 'create2')
    and to_address is not null
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
group by contract_address