{% macro get_balancer_v2_swap_fee_changes(chain) %}
    SELECT 
        block_timestamp,
        contract_address as pool_address,
        decoded_log:swapFeePercentage::number as swap_fee_percentage,
        -- Create lead timestamp to know when this fee was valid until
        LEAD(block_timestamp) OVER (PARTITION BY contract_address ORDER BY block_timestamp) as valid_until
    FROM {{ source((chain | upper) ~ '_FLIPSIDE', 'ez_decoded_event_logs') }}
    WHERE event_name = 'SwapFeePercentageChanged'
    -- Filter only for known Balancer pool addresses
    AND contract_address IN (
        SELECT lower(pool_address) 
        FROM {{ ref('fact_balancer_v2_' ~ chain ~ '_pool_metadata') }}
    )
{% endmacro %}