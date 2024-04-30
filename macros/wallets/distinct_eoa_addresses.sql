{% macro distinct_eoa_addresses(chain) %}
    {% if chain == "tron" %}
        select 
            distinct from_address as address, 'eoa' as address_type
        from tron_allium.raw.transactions
    {% elif chain == "solana" %}
        select distinct signer as address,  'signer' as address_type
        from solana_flipside.core.ez_signers 
    {% else %}
        select 
            distinct from_address as address, 'eoa' as address_type
        from {{ chain }}_flipside.core.fact_transactions
    {% endif %}
{% endmacro %}