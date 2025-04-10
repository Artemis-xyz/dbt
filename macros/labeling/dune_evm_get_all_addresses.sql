{% macro dune_evm_get_all_addresses(chain) %}
    -- We are grabbing trace-level addresses instead of transaction-level addresses for all EVM-based chains
    WITH source_data AS (
        SELECT tx_from_hex as from_address, tx_to_hex as to_address, type, block_time::timestamp as last_updated
        FROM zksync_dune.{{ chain }}.traces
        {% if is_incremental() %}
            WHERE block_time::timestamp > (SELECT MAX(last_updated) FROM {{ this }})
        {% endif %}
    ),
    union_data AS (
        SELECT 
            trim(from_address) AS address,
            'from_address' AS transaction_trace_type, 
            NULL AS address_type,
            last_updated 
        FROM source_data
        UNION ALL
        SELECT 
            trim(to_address) AS address, 
            'to_address' AS transaction_trace_type, 
            CASE 
                WHEN to_address IS NOT NULL AND type IN ('create', 'create2') THEN 'smart_contract'
                ELSE NULL
            END AS address_type,
            last_updated 
        FROM source_data
    )
    SELECT 
        address,
        ARRAY_AGG(DISTINCT transaction_trace_type) AS transaction_trace_type,
        COALESCE(MAX(address_type), NULL) AS address_type,
        '{{ chain }}' AS chain,
        MAX(last_updated) AS last_updated
    FROM union_data
    WHERE address IS NOT NULL
    GROUP BY address 
{% endmacro %}