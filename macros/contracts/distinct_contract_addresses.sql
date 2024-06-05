{% macro distinct_contract_addresses(chain) %}
select min(block_timestamp) as block_timestamp, to_address as contract_address, min(type) as type --Contracts can be redeployed at the same addresses with CREATE2
from {{ chain }}_flipside.core.fact_traces
where type in ('CREATE', 'CREATE2')
    and contract_address is not null --if the deploy fails the to address will be null
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
group by contract_address
{% endmacro %}
