{{ config(materialized="table", snowflake_warehouse="GMX") }}

-- VERSION 2
WITH v2_trade_fees_arbitrum AS (
    SELECT
        date_trunc('day', block_timestamp) as date,
        block_timestamp,
        tx_hash,
        contract_address,
        'arbitrum' as chain,
        decoded_log,
        decoded_log:eventData[0][0][3][1]::STRING as trader,
        decoded_log:eventData[0][0][0][1]::STRING as market,
        decoded_log:eventData[0][0][1][1]::STRING as collateral_token,
        decoded_log:eventData[1][0][0][1]::NUMBER as collateral_token_price_min,
        decoded_log:eventData[1][0][1][1]::NUMBER as collateral_token_price_max,
        --(decoded_log:eventData[1][0][0][1]::NUMBER + decoded_log:eventData[1][0][1][1]::NUMBER) / 2 as collateral_token_price_avg,
        decoded_log:eventData[1][0][2][1]::NUMBER / 1e30 as volume,
        decoded_log:eventData[1][0][10][1]::NUMBER as borrowing_fee,
        decoded_log:eventData[1][0][19][1]::NUMBER as position_fee_amount,
        coalesce(decoded_log:eventData[1][0][27][1]::NUMBER, 0) as trader_discount_amount,
        decoded_log:eventData[1][0][20][1]::NUMBER as total_cost_amount,
        COALESCE(fe.value[1]::NUMBER, 0) as liquidation_fee_raw,
        --coalesce(decoded_log:eventData[1][0][29][1]::NUMBER, 0) as liquidation_fee_raw,
        decoded_log:eventData[4][0][0][1]::STRING as order_key,
    FROM arbitrum_flipside.core.ez_decoded_event_logs, LATERAL FLATTEN(input => decoded_log:eventData[1][0]) as fe 
    WHERE fe.value[0]::STRING = 'liquidationFeeAmount'
    and contract_address = lower('0xC8ee91A54287DB53897056e12D9819156D3822Fb')
    and decoded_log:eventName::STRING = 'PositionFeesCollected'
), 
v2_trade_fees_usd_arbitrum as (
    select
        g.date,
        g.chain,
        g.block_timestamp,
        g.tx_hash,
        g.order_key,
        g.decoded_log,
        g.trader,
        g.market,
        t.price,
        g.volume,
        g.collateral_token,
        t.symbol,
        t.decimals,
        g.position_fee_amount,
        g.trader_discount_amount,
        g.borrowing_fee,
        g.total_cost_amount,
        g.position_fee_amount - coalesce(g.trader_discount_amount, 0) as net_position_fee_amount,
        g.liquidation_fee_raw,
        g.position_fee_amount / POW(10, t.decimals)  as position_fee_amount_usd,
        (g.position_fee_amount - coalesce(g.trader_discount_amount, 0)) / POW(10, t.decimals)  as net_position_fee_amount_usd,
        g.borrowing_fee / POW(10, t.decimals) as borrowing_fee_amount_usd,
        g.liquidation_fee_raw / POW(10, t.decimals) as liquidation_fee_amount_adjusted,
        ((g.liquidation_fee_raw / POW(10, t.decimals)) * t.price) as fees,
        (((g.liquidation_fee_raw - g.trader_discount_amount) / POW(10, t.decimals)) * t.price) as total_cost_fees
    from v2_trade_fees_arbitrum g
    left join arbitrum_flipside.price.ez_prices_hourly t
        on t.token_address = g.collateral_token
        and t.hour = date_trunc('hour',g.block_timestamp)
    where symbol is not null
) 
, v2_trades_arbitrum AS (
    SELECT
        'arbitrum' AS chain,
        block_timestamp,
        date_trunc('day', block_timestamp) as date,
        decoded_log:eventData[0][0][0][1]::STRING as address,
        TRY_TO_NUMBER(decoded_log:eventData[1][0][12][1]::STRING) / 1e30 as volume,
        decoded_log:eventData[4][0][0][1]::STRING as order_key,
        decoded_log:eventData[1][0][16][1]::NUMBER as order_type,
        decoded_log,
        tx_hash
    FROM arbitrum_flipside.core.ez_decoded_event_logs
    WHERE contract_address = lower('0xC8ee91A54287DB53897056e12D9819156D3822Fb')
    AND decoded_log:eventName::STRING = 'PositionDecrease'
    AND decoded_log:eventData[1][0][16][1]::NUMBER = 7 -- Exclude liquidation order types
),
trades_and_fees_joined_arbitrum as (
    select f.*, t.volume as volume2
    from v2_trades_arbitrum t
    inner join v2_trade_fees_usd_arbitrum f
        on f.order_key = t.order_key
),-- select * from trades_and_fees_joined_arbitrum;

--AVALANCHE
v2_trade_fees_avalanche AS (
    SELECT
        date_trunc('day', block_timestamp) as date,
        block_timestamp,
        tx_hash,
        contract_address,
        'avalanche' as chain,
        decoded_log,
        decoded_log:eventData[0][0][3][1]::STRING as trader,
        decoded_log:eventData[0][0][0][1]::STRING as market,
        decoded_log:eventData[0][0][1][1]::STRING as collateral_token,
        decoded_log:eventData[1][0][0][1]::NUMBER as collateral_token_price_min,
        decoded_log:eventData[1][0][1][1]::NUMBER as collateral_token_price_max,
        --(decoded_log:eventData[1][0][0][1]::NUMBER + decoded_log:eventData[1][0][1][1]::NUMBER) / 2 as collateral_token_price_avg,
        decoded_log:eventData[1][0][2][1]::NUMBER / 1e30 as volume,
        decoded_log:eventData[1][0][10][1]::NUMBER as borrowing_fee,
        decoded_log:eventData[1][0][19][1]::NUMBER as position_fee_amount,
        coalesce(decoded_log:eventData[1][0][27][1]::NUMBER, 0) as trader_discount_amount,
        decoded_log:eventData[1][0][20][1]::NUMBER as total_cost_amount,
        COALESCE(fe.value[1]::NUMBER, 0) as liquidation_fee_raw,
        --coalesce(decoded_log:eventData[1][0][29][1]::NUMBER, 0) as liquidation_fee_raw,
        decoded_log:eventData[4][0][0][1]::STRING as order_key,
    FROM avalanche_flipside.core.ez_decoded_event_logs, LATERAL FLATTEN(input => decoded_log:eventData[1][0]) as fe 
    WHERE fe.value[0]::STRING = 'liquidationFeeAmount'
    and contract_address = lower('0xDb17B211c34240B014ab6d61d4A31FA0C0e20c26')
    and decoded_log:eventName::STRING = 'PositionFeesCollected'
), 
v2_trade_fees_usd_avalanche as (
    select
        g.date,
        g.chain,
        g.block_timestamp,
        g.tx_hash,
        g.order_key,
        g.decoded_log,
        g.trader,
        g.market,
        t.price,
        g.volume,
        g.collateral_token,
        t.symbol,
        t.decimals,
        g.position_fee_amount,
        g.trader_discount_amount,
        g.borrowing_fee,
        g.total_cost_amount,
        g.position_fee_amount - coalesce(g.trader_discount_amount, 0) as net_position_fee_amount,
        g.liquidation_fee_raw,
        g.position_fee_amount / POW(10, t.decimals)  as position_fee_amount_usd,
        (g.position_fee_amount - coalesce(g.trader_discount_amount, 0)) / POW(10, t.decimals)  as net_position_fee_amount_usd,
        g.borrowing_fee / POW(10, t.decimals) as borrowing_fee_amount_usd,
        g.liquidation_fee_raw / POW(10, t.decimals) as liquidation_fee_amount_adjusted,
        ((g.liquidation_fee_raw / POW(10, t.decimals)) * t.price) as fees,
        (((g.liquidation_fee_raw - g.trader_discount_amount) / POW(10, t.decimals)) * t.price) as total_cost_fees
    from v2_trade_fees_avalanche g
    left join avalanche_flipside.price.ez_prices_hourly t
        on t.token_address = g.collateral_token
        and t.hour = date_trunc('hour',g.block_timestamp)
    where symbol is not null
) 
, v2_trades_avalanche AS (
    SELECT
        'avalanche' AS chain,
        block_timestamp,
        date_trunc('day', block_timestamp) as date,
        decoded_log:eventData[0][0][0][1]::STRING as address,
        TRY_TO_NUMBER(decoded_log:eventData[1][0][12][1]::STRING) / 1e30 as volume,
        decoded_log:eventData[4][0][0][1]::STRING as order_key,
        decoded_log:eventData[1][0][16][1]::NUMBER as order_type,
        decoded_log,
        tx_hash
    FROM avalanche_flipside.core.ez_decoded_event_logs
    WHERE contract_address = lower('0xDb17B211c34240B014ab6d61d4A31FA0C0e20c26')
    AND decoded_log:eventName::STRING = 'PositionDecrease'
    AND decoded_log:eventData[1][0][16][1]::NUMBER = 7 -- Exclude liquidation order types
   -- AND tx_hash = '0xa7c7a28695823d59bfdf461f7999886a928ba9ea534a63e00a0fbe90daf2c0fe'
),
trades_and_fees_joined_avalanche as (
    select f.*, t.volume as volume2, t.order_type as order_type
    from v2_trades_avalanche t
    inner join v2_trade_fees_usd_avalanche f 
        on f.order_key = t.order_key
) --select * from trades_and_fees_joined_avalanche;

select 
    date,
    chain,
    'v2' as version,
    sum(fees) as fees
from trades_and_fees_joined_arbitrum
group by 1, 2, 3

union all 

select 
    date,
    chain,
    'v2' as version,
    sum(fees) as fees
from trades_and_fees_joined_avalanche
group by 1, 2, 3

