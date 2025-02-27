{% macro get_balancer_v2_PoolBalanceChanged(chain) %}
    SELECT 
        block_timestamp,
        tx_hash,
        decoded_log:poolId::string as pool_id,
        decoded_log:liquidityProvider::string as liquidity_provider,
        t.value::string as token_address,
        d.value::number as token_delta,
        t.index as token_index
    FROM {{ source((chain | upper) ~ '_FLIPSIDE', 'ez_decoded_event_logs') }} e,
    LATERAL FLATTEN(input => decoded_log:tokens) t,
    LATERAL FLATTEN(input => decoded_log:deltas) d
    WHERE 1=1
        AND lower(contract_address) = lower('0xBA12222222228d8Ba445958a75a0704d566BF2C8')
        AND event_name = 'PoolBalanceChanged'
        AND t.index = d.index
{% endmacro %}