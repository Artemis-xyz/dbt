{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='raw',
    )
}}

WITH SWAP_DETAILS AS (
    SELECT
        DATE_TRUNC('DAY', swaps.block_timestamp) AS block_date, -- Extracts the date part from the timestamp
        swaps.block_timestamp,
        swaps.block_number,
        origin_from_address,
        origin_to_address,
        event_index,
        tx_hash,
        swaps.hour as hour,
        'ethereum' AS chain,
        pool,
        caller,
        tokenIn, -- Directly references extracted field
        tokenAmountIn, -- Directly references extracted field
        t2.price AS tokenInPrice,
        t2.symbol AS tokenInSymbol,
        t2.decimals AS tokenInDecimals,
        tokenOut, -- Directly references extracted field
        tokenAmountOut, -- Directly references extracted field
        t3.price AS tokenOutPrice,
        t3.symbol AS tokenOutSymbol,
        t3.decimals AS tokenOutDecimals,
    FROM {{ ref('fact_balancer_v1_ethereum_Bpool_swaps') }} AS swaps
    LEFT JOIN
        {{ source("ETHEREUM_FLIPSIDE_PRICE", "ez_prices_hourly")}} t2 --ethereum_flipside.price.ez_prices_hourly t2
        on (lower(swaps.tokenIn) = lower(t2.token_address) and t2.hour = swaps.hour)
    LEFT JOIN
        {{ source("ETHEREUM_FLIPSIDE_PRICE", "ez_prices_hourly")}} t3 --ethereum_flipside.price.ez_prices_hourly t3
        on (lower(swaps.tokenOut) = lower(t3.token_address) and t3.hour = swaps.hour)
    ORDER BY block_date ASC -- Sorts the results by date in ascending order
),
SWAPS_USD_RAW AS (
    SELECT
        swap.block_date,
        swap.block_timestamp,
        source_fees.block_timestamp AS set_fee_block_timestamp,
        swap.block_number,
        source_fees.block_number,
        swap.event_index,
        swap.tx_hash,
        hour AS swap_hour,
        chain,
        pool,
        origin_from_address,
        origin_to_address,
        caller,
        source_fees.tx_hash as set_fee_tx_hash,
        tokenIn, 
        tokenAmountIn,
        tokenInPrice,
        tokenInSymbol,
        tokenInDecimals,
        tokenAmountIn  / pow(10, tokenInDecimals) * tokenInPrice as tokenAmountInUSD,
        tokenAmountIn  / pow(10, tokenInDecimals) as tokenAmountInNative,
        tokenOut, 
        tokenAmountOut, 
        tokenOutPrice,
        tokenOutSymbol,
        tokenOutDecimals,
        tokenAmountOut  / pow(10, tokenOutDecimals) * tokenOutPrice as tokenAmountOutUSD,
        tokenAmountOut  / pow(10, tokenOutDecimals) as tokenAmountOutNative,
        source_fees.decoded_input_data:swapFee / 1e18 as swapFee,
        swapFee * tokenAmountInUSD as swapFeeUSD,
        swapFee * tokenAmountInNative as swapFeeNative,
        ROW_NUMBER() OVER (PARTITION BY source_fees.to_address, swap.tx_hash, swap.event_index ORDER BY source_fees.block_number DESC NULLS FIRST) AS row_num
    FROM SWAP_DETAILS swap
    LEFT JOIN {{ source("ETHEREUM_FLIPSIDE", "ez_decoded_traces")}} source_fees --ethereum_flipside.core.ez_decoded_traces source_fees 
        ON lower(source_fees.to_address) = lower(swap.pool)
        AND source_fees.block_number < swap.block_number
    WHERE FUNCTION_NAME = 'setSwapFee' 
),

SWAPS_USD AS (
    SELECT * 
    FROM SWAPS_USD_RAW
    WHERE row_num = 1
)
SELECT
    block_timestamp,
    chain,
    'balancer' as app,
    'v1' as version,

    -- Transaction information
    tx_hash AS tx_hash,
    origin_from_address AS sender, 
    origin_to_address AS recipient, 
    
    -- Pool information
    pool AS pool_address,

    -- Input token information
    tokenAmountIn AS amount_in_native,
    tokenAmountInUSD AS amount_in_usd,
    tokenInSymbol AS token_in_symbol,
    tokenIn AS token_in_address,

    -- Output token information
    tokenAmountOut AS amount_out_native,
    tokenAmountOutUSD AS amount_out_usd,
    tokenOutSymbol AS token_out_symbol,
    tokenOut AS token_out_address,

    -- Fee information
    swapFee AS swap_fee_pct,
    swapFeeUSD AS fee_usd,
    swapFeeNative as fee_native,
    0 as treasury_cash_flow,
    0 as treasury_cash_flow_native,
    0 as vebal_cash_flow,
    0 as vebal_cash_flow_native,
    swapFeeUSD as service_cash_flow,
    swapFeeNative as service_cash_flow_native
FROM SWAPS_USD