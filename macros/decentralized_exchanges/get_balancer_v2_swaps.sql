{% macro get_balancer_v2_swaps(chain) %}
    
    WITH swap_fees AS (
        -- Get historical swap fees for each pool
        SELECT * FROM {{ ref('fact_balancer_v2_' ~ chain ~ '_swap_fee_changes') }}
    ),

    swaps AS (
        -- Get all swap events
        SELECT 
            block_timestamp,
            tx_hash,
            origin_from_address as from_address,
            origin_to_address as to_address,
            decoded_log:poolId::string as pool_id,
            decoded_log:tokenIn::string as token_in,
            decoded_log:tokenOut::string as token_out,
            decoded_log:amountIn::number as amount_in,
            decoded_log:amountOut::number as amount_out
        FROM {{ source((chain | upper) ~ '_FLIPSIDE', 'ez_decoded_event_logs') }}
        WHERE contract_address = lower('0xBA12222222228d8Ba445958a75a0704d566BF2C8')
        AND event_name = 'Swap'
        AND tx_succeeded = TRUE
        {% if is_incremental() %}
            AND block_timestamp > (SELECT MAX(block_timestamp) FROM {{ this }})
        {% endif %}
    ),

    token_prices AS (
        SELECT 
            token_address,
            hour,
            price,
            decimals,
            symbol
        FROM {{ source((chain | upper) ~ '_FLIPSIDE_PRICE', 'ez_prices_hourly') }}
        WHERE blockchain = '{{ chain }}'
        AND token_address IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY token_address, hour ORDER BY modified_timestamp DESC) = 1
    )
    , agg_swaps as (
        SELECT 
            s.block_timestamp,
            '{{chain}}' as chain,
            s.tx_hash,
            s.from_address,
            s.to_address,
            pm.pool_address,
            s.pool_id,
            pm.token_addresses as tokens_in_pool,

            s.amount_in / pow(10, p_in.decimals) as amount_in_native,
            s.amount_in * COALESCE(p_in.price, 0) / pow(10, p_in.decimals) as amount_in_usd,
            amount_in_native * (f.swap_fee_percentage / 1e18) as amount_in_fee_native,
            (amount_in_native * COALESCE(p_in.price, 0)) * (f.swap_fee_percentage / 1e18) as amount_in_fee_usd,
            p_in.symbol as token_in_symbol,
            p_in.token_address as token_in_address,

            s.amount_out / pow(10, p_out.decimals) as amount_out_native,
            s.amount_out * COALESCE(p_out.price, 0) / pow(10, p_out.decimals) as amount_out_usd,
            amount_out_native * (f.swap_fee_percentage / 1e18) as amount_out_fee_native,
            (amount_out_native * COALESCE(p_out.price, 0)) * (f.swap_fee_percentage / 1e18) as amount_out_fee_usd,
            p_out.symbol as token_out_symbol,
            p_out.token_address as token_out_address,

            -- Fee information
            f.swap_fee_percentage / 1e18 as swap_fee_pct,  -- Convert from 18 decimals
            -- Calculate fee amount in USD (based on input amount)
            p_in.symbol as fee_token,
            coalesce(amount_in_fee_native, amount_out_fee_native) as fee_native,
            coalesce(amount_in_fee_usd, amount_out_fee_usd) as fee_usd
        FROM swaps s
        LEFT JOIN {{ ref('fact_balancer_v2_' ~ chain ~ '_pool_metadata') }} pm 
            ON s.pool_id = pm.pool_id
        -- Join with swap fees valid at the time of the swap
        LEFT JOIN swap_fees f 
            ON lower(pm.pool_address) = f.pool_address
            AND s.block_timestamp >= f.block_timestamp 
            AND (s.block_timestamp < f.valid_until OR f.valid_until IS NULL)
        LEFT JOIN token_prices p_in 
            ON lower(s.token_in) = lower(p_in.token_address)
            AND DATE_TRUNC('hour', s.block_timestamp) = p_in.hour
        LEFT JOIN token_prices p_out 
            ON lower(s.token_out) = lower(p_out.token_address)
            AND DATE_TRUNC('hour', s.block_timestamp) = p_out.hour
        ORDER BY s.block_timestamp DESC
    )
    SELECT * FROM agg_swaps
    WHERE amount_in_usd < 1e9
{% endmacro %}