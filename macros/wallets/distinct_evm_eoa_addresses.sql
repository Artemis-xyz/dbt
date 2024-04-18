{% macro distinct_evm_eoa_addresses(chain) %}
select 
    distinct from_address as address, 'eoa' as address_type
from {{ chain }}_flipside.core.fact_transactions
{% endmacro %}