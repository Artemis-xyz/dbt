{% macro evm_get_all_addresses(chain) %}
    -- We are grabbing trace-level addresses instead of transaction-level addresses for all EVM-based chains
    WITH source_data AS (
        SELECT from_address, to_address, modified_timestamp as last_updated
        FROM {{ chain }}_flipside.core.fact_traces
        {% if is_incremental() %}
            WHERE modified_timestamp > (SELECT MAX(last_updated) FROM {{ this }})
        {% endif %}
    ),
    union_data AS (
        SELECT trim(from_address) AS address, 'from_address' AS address_type, last_updated FROM source_data
        UNION ALL
        SELECT trim(to_address) AS address, 'to_address' AS address_type, last_updated FROM source_data
    )
    SELECT DISTINCT 
        address, 
        address_type,
        '{{ chain }}' as chain,
        MAX(last_updated) OVER () AS last_updated
    FROM union_data
    WHERE address IS NOT NULL
{% endmacro %}