{% macro get_balancer_v2_pool_metadata(chain) %}
    
    WITH pool_registrations AS (
        -- Get all pool registrations to map poolId to pool address
        SELECT 
            decoded_log:poolId::string as pool_id,
            decoded_log:poolAddress::string as pool_address
        FROM {{ source((chain | upper) ~ '_FLIPSIDE', 'ez_decoded_event_logs') }}
        WHERE contract_address = lower('0xBA12222222228d8Ba445958a75a0704d566BF2C8') -- Vault
        AND event_name = 'PoolRegistered'
    ),

    pool_tokens AS (
        -- Get token composition of pools
        SELECT 
            decoded_log:poolId::string as pool_id,
            decoded_log:tokens as token_addresses
        FROM {{ source((chain | upper) ~ '_FLIPSIDE', 'ez_decoded_event_logs') }}
        WHERE contract_address = lower('0xBA12222222228d8Ba445958a75a0704d566BF2C8')
        AND event_name = 'TokensRegistered'
    )

    SELECT
        pool_registrations.pool_id,
        pool_registrations.pool_address,
        pool_tokens.token_addresses
    FROM pool_registrations
    JOIN pool_tokens ON pool_registrations.pool_id = pool_tokens.pool_id

{% endmacro %}