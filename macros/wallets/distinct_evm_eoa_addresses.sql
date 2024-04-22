{% macro distinct_evm_eoa_addresses(chain) %}
    {% if chain == "tron" %}
        select 
            distinct from_address as address, 'eoa' as address_type
        from tron_allium.raw.transactions
    {% else %}
        select 
            distinct from_address as address, 'eoa' as address_type
        from {{ chain }}_flipside.core.fact_transactions
    {% endif %}
{% endmacro %}