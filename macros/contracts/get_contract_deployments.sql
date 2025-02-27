{% macro get_contract_deployments(chain) %}
select
    '{{chain}}' as chain,
    date_trunc(week, block_timestamp) as date,
    count(distinct from_address) as contract_deployers,
    count(*) as contracts_deployed
{% if chain == 'sei' %}
from {{chain}}_flipside.core_evm.fact_traces
{% else %}
from {{chain}}_flipside.core.fact_traces
{% endif %}
where type in ('CREATE', 'CREATE2')
and TRACE_STATUS = 'SUCCESS'
{% if is_incremental() %}
    and block_timestamp >= (select max(date) from {{ this }})
{% endif %}
group by date
order by date desc
{% endmacro %}
