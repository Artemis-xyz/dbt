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
        tokenOut, 
        tokenAmountOut, 
        tokenOutPrice,
        tokenOutSymbol,
        tokenOutDecimals,
        tokenAmountOut  / pow(10, tokenOutDecimals) * tokenOutPrice as tokenAmountOutUSD,
        source_fees.decoded_input_data:swapFee / 1e18 as swapFee,
        swapFee * tokenAmountInUSD as swapFeeUSD,
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
    'ethereum' AS blockchain,
    'balancer' AS project,
    'v1' AS version,
    block_date,
    DATE_TRUNC('MONTH', block_timestamp) AS block_month,
    block_timestamp AS block_time,
    tokenOutSymbol AS token_bought_symbol,
    tokenInSymbol AS token_sold_symbol,
    CONCAT(tokenOutSymbol, '-', tokenInSymbol) AS token_pair,
    tokenAmountOut AS token_bought_amount_raw,
    tokenAmountIn AS token_sold_amount_raw,
    tokenAmountOutUSD AS token_bount_amount_usd,
    tokenAmountInUSD AS token_sold_amount_usd,
    tokenOut AS token_bought_address,
    tokenIn AS token_sold_address,
    origin_from_address AS taker, --??
    ' ' AS maker,                 --??
    pool AS balancer_pool_address,
    swapFee AS swap_fee,
    swapFeeUSD AS swap_fee_usd,
    'v1' AS pool_type,
    tx_hash AS tx_hash,
    origin_from_address AS tx_from, 
    origin_to_address AS tx_to, 
    event_index AS evt_index
FROM SWAPS_USD