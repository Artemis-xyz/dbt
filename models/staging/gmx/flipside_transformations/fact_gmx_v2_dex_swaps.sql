{{ config(materialized="table", snowflake_warehouse="GMX") }}

with GMXSwapEvents_arbitrum_v2 as ( 
    select
        date_trunc('day', block_timestamp) as date,
        block_timestamp,
        tx_hash,
        event_index,
        CONCAT(tx_hash, '-', event_index) as log_id,
        decoded_log:msgSender::STRING AS sender,
        decoded_log:eventData[0][0][2][1]::STRING AS tokenIn,
        decoded_log:eventData[0][0][3][1]::STRING AS tokenOut,
        decoded_log:eventData[1][0][2][1]::NUMBER AS amountIn,
        decoded_log:eventData[1][0][3][1]::NUMBER AS amountInAfterFees,
        decoded_log:eventData[1][0][2][1]::NUMBER - decoded_log:eventData[1][0][3][1]::NUMBER AS amount_fees,
        decoded_log:eventData[1][0][4][1]::NUMBER AS amountOut
    from arbitrum_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0xC8ee91A54287DB53897056e12D9819156D3822Fb')
    and decoded_log:eventName::STRING = 'SwapInfo' 
), GMXSwapEventsUSD_arbitrum_v2 as (
    SELECT
        swaps.date,
        'arbitrum' AS chain,
        'GMX' AS protocol,
        'v2' AS version,
        swaps.block_timestamp,
        swaps.tx_hash,
        swaps.event_index,
        swaps.log_id,
        swaps.sender,
        swaps.tokenIn,
        swaps.tokenOut,
        --amountIn
        swaps.amountIn,
        swaps.amountIn / POW(10, token_in_price.decimals) AS amountIn_nominal,
        amountIn_nominal * token_in_price.price AS amountIn_usd,
        --amountOut
        swaps.amountOut,
        swaps.amountOut / POW(10, token_in_price.decimals) AS amountOut_nominal,
        amountOut_nominal * token_in_price.price AS amountOut_usd,
        --amountOutFees
        swaps.amount_fees,
        swaps.amount_fees / POW(10, token_in_price.decimals) AS amount_fees_nominal,
        amount_fees_nominal * token_in_price.price AS amount_fees_usd,
    FROM GMXSwapEvents_arbitrum_v2 swaps
    LEFT JOIN arbitrum_flipside.price.ez_prices_hourly token_in_price
        ON LOWER(swaps.tokenIn) = LOWER(token_in_price.token_address)
        AND date_trunc('hour', swaps.block_timestamp) = token_in_price.hour
    LEFT JOIN arbitrum_flipside.price.ez_prices_hourly token_out_price
        ON LOWER(swaps.tokenOut) = LOWER(token_out_price.token_address)
        AND date_trunc('hour', swaps.block_timestamp) = token_out_price.hour
    WHERE token_in_price.price IS NOT NULL 
      AND token_out_price.price IS NOT NULL
    ORDER BY swaps.block_timestamp DESC
), GMXSwapEvents_avalanche_v2 as ( 
    select
        date_trunc('day', block_timestamp) as date,
        block_timestamp,
        tx_hash,
        event_index,
        CONCAT(tx_hash, '-', event_index) as log_id,
        decoded_log:msgSender::STRING AS sender,
        decoded_log:eventData[0][0][2][1]::STRING AS tokenIn,
        decoded_log:eventData[0][0][3][1]::STRING AS tokenOut,
        decoded_log:eventData[1][0][2][1]::NUMBER AS amountIn,
        decoded_log:eventData[1][0][3][1]::NUMBER AS amountInAfterFees,
        decoded_log:eventData[1][0][2][1]::NUMBER - decoded_log:eventData[1][0][3][1]::NUMBER AS amount_fees,
        decoded_log:eventData[1][0][4][1]::NUMBER AS amountOut
    from avalanche_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0xDb17B211c34240B014ab6d61d4A31FA0C0e20c26')
    and decoded_log:eventName::STRING = 'SwapInfo' 
), GMXSwapEventsUSD_avalanche_v2 as (
    SELECT
        swaps.date,
        'avalanche' AS chain,
        'GMX' AS protocol,
        'v2' AS version,
        swaps.block_timestamp,
        swaps.tx_hash,
        swaps.event_index,
        swaps.log_id,
        swaps.sender,
        swaps.tokenIn,
        swaps.tokenOut,
        --amountIn
        swaps.amountIn,
        swaps.amountIn / POW(10, token_in_price.decimals) AS amountIn_nominal,
        amountIn_nominal * token_in_price.price AS amountIn_usd,
        --amountOut
        swaps.amountOut,
        swaps.amountOut / POW(10, token_out_price.decimals) AS amountOut_nominal,
        amountOut_nominal * token_out_price.price AS amountOut_usd,
        --*amountOutFees
        swaps.amount_fees,
        swaps.amount_fees / POW(10, token_in_price.decimals) AS amount_fees_nominal,
        amount_fees_nominal * token_in_price.price AS amount_fees_usd
    FROM GMXSwapEvents_avalanche_v2 swaps
    LEFT JOIN avalanche_flipside.price.ez_prices_hourly token_in_price
        ON LOWER(swaps.tokenIn) = LOWER(token_in_price.token_address)
        AND date_trunc('hour', swaps.block_timestamp) = token_in_price.hour
    LEFT JOIN avalanche_flipside.price.ez_prices_hourly token_out_price
        ON LOWER(swaps.tokenOut) = LOWER(token_out_price.token_address)
        AND date_trunc('hour', swaps.block_timestamp) = token_out_price.hour
    WHERE token_in_price.price IS NOT NULL 
      AND token_out_price.price IS NOT NULL
    ORDER BY swaps.block_timestamp DESC
),
gmx_v2_swap_events as (
    select * 
    from GMXSwapEventsUSD_arbitrum_v2
    union all
    select *
    from GMXSwapEventsUSD_avalanche_v2
) select * from gmx_v2_swap_events


/*, spot_metrics as (
    select
        date,
        chain,
        'v2' as version,
        count(distinct tx_hash) as spot_txns,
        count(distinct sender) as spot_dau,
        sum(coalesce(amountOut_usd, 0)) as spot_volume,
        sum(coalesce(amount_fees_usd, 0)) as spot_fees,
    from GMXSwapEventsUSD_v2
    group by 1,2,3
)
SELECT
    spot_metrics.date,
    spot_metrics.chain,
    spot_metrics.version,
    spot_metrics.spot_txns,
    spot_metrics.spot_dau,
    spot_metrics.spot_volume,
    spot_metrics.spot_fees,
    CASE
        WHEN version = 'v1' THEN 0.7 * spot_fees
        WHEN version = 'v2' THEN 0.63 * spot_fees
    END as spot_lp_fee_allocation,
    CASE
        WHEN version = 'v1' THEN 0.3 * spot_fees
        WHEN version = 'v2' THEN 0.27 * spot_fees
    END as spot_stakers_fee_allocation,
    CASE
        WHEN version = 'v1' THEN 0 * spot_fees
        WHEN version = 'v2' THEN 0.012 * spot_fees
    END as spot_oracle_fee_allocation,
    CASE
        WHEN version = 'v1' THEN 0 * spot_fees
        WHEN version = 'v2' THEN 0.088 * spot_fees
    END as spot_treasury_fee_allocation
from spot_metrics*/