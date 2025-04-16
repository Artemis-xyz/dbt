{{ config(materialized="table", snowflake_warehouse="GMX") }}


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
        decoded_log:eventData[1][0][2][1]::NUMBER / 1e30 as volume,
        decoded_log:eventData[1][0][9][1]::NUMBER / 1e30 as borrowing_fee_usd,
        pos.value[1]::NUMBER as position_fee_amount,
        coalesce(decoded_log:eventData[1][0][27][1]::NUMBER, 0) as trader_discount_amount,
        decoded_log:eventData[1][0][20][1]::NUMBER as total_cost_amount,
        decoded_log:eventData[4][0][0][1]::STRING as order_key,
    FROM arbitrum_flipside.core.ez_decoded_event_logs, LATERAL FLATTEN(input => decoded_log:eventData[1][0]) as pos
    where pos.value[0]::STRING = 'positionFeeAmount'
    and contract_address = lower('0xC8ee91A54287DB53897056e12D9819156D3822Fb')
    and decoded_log:eventName::STRING = 'PositionFeesCollected'
), 
v2_trade_fees_usd_arbitrum as (
    select
        g.date,
        g.chain,
        'v2' as version,
        g.block_timestamp,
        g.tx_hash,
        g.order_key,
        g.trader,
        g.market,
        g.volume,
        g.collateral_token,
        t.price,
        t.symbol,
        t.decimals,
        g.position_fee_amount,
        g.position_fee_amount / POW(10, t.decimals)  as position_fee_amount_raw,
        (g.position_fee_amount / POW(10, t.decimals)) * t.price as position_fee_amount_usd,
        g.trader_discount_amount,
        g.position_fee_amount - coalesce(g.trader_discount_amount, 0) as net_position_fee_amount,
        (g.position_fee_amount - coalesce(g.trader_discount_amount, 0)) / POW(10, t.decimals)  as net_position_fee_amount_raw,
        ((g.position_fee_amount - coalesce(g.trader_discount_amount, 0)) / POW(10, t.decimals)) * t.price as net_position_fee_amount_usd,
        g.borrowing_fee_usd,
        g.total_cost_amount,
        (g.position_fee_amount / POW(10, t.decimals) * t.price) + g.borrowing_fee_usd as fees,
        (((g.position_fee_amount - coalesce(g.trader_discount_amount, 0)) / POW(10, t.decimals)) * t.price) + g.borrowing_fee_usd as total_cost_fees
    from v2_trade_fees_arbitrum g
    left join arbitrum_flipside.price.ez_prices_hourly t
        on t.token_address = g.collateral_token
        and t.hour = g.date
    where symbol is not null
), 
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
        decoded_log:eventData[1][0][2][1]::NUMBER / 1e30 as volume,
        decoded_log:eventData[1][0][9][1]::NUMBER / 1e30 as borrowing_fee_usd,
        pos.value[1]::NUMBER as position_fee_amount,
        coalesce(decoded_log:eventData[1][0][27][1]::NUMBER, 0) as trader_discount_amount,
        decoded_log:eventData[1][0][20][1]::NUMBER as total_cost_amount,
        decoded_log:eventData[4][0][0][1]::STRING as order_key,
    FROM avalanche_flipside.core.ez_decoded_event_logs, LATERAL FLATTEN(input => decoded_log:eventData[1][0]) as pos
    where pos.value[0]::STRING = 'positionFeeAmount'
    and contract_address = lower('0xDb17B211c34240B014ab6d61d4A31FA0C0e20c26')
    and decoded_log:eventName::STRING = 'PositionFeesCollected'
), 
v2_trade_fees_usd_avalanche as (
    select
        g.date,
        g.chain,
        'v2' as version,
        g.block_timestamp,
        g.tx_hash,
        g.order_key,
        g.trader,
        g.market,
        g.volume,
        g.collateral_token,
        t.price,
        t.symbol,
        t.decimals,
        g.position_fee_amount,
        g.position_fee_amount / POW(10, t.decimals)  as position_fee_amount_raw,
        (g.position_fee_amount / POW(10, t.decimals)) * t.price as position_fee_amount_usd,
        g.trader_discount_amount,
        g.position_fee_amount - coalesce(g.trader_discount_amount, 0) as net_position_fee_amount,
        (g.position_fee_amount - coalesce(g.trader_discount_amount, 0)) / POW(10, t.decimals)  as net_position_fee_amount_raw,
        ((g.position_fee_amount - coalesce(g.trader_discount_amount, 0)) / POW(10, t.decimals)) * t.price as net_position_fee_amount_usd,
        g.borrowing_fee_usd,
        g.total_cost_amount,
        (g.position_fee_amount / POW(10, t.decimals) * t.price) + g.borrowing_fee_usd as fees,
        (((g.position_fee_amount - coalesce(g.trader_discount_amount, 0)) / POW(10, t.decimals)) * t.price) + g.borrowing_fee_usd as total_cost_fees
    from v2_trade_fees_avalanche g
    left join avalanche_flipside.price.ez_prices_hourly t
        on t.token_address = g.collateral_token
        and t.hour = g.date
    where symbol is not null
)
select
    date,
    'arbitrum' as chain,
    'v2' as version,
    block_timestamp,
    tx_hash,
    collateral_token,
    fees,
    volume,
    trader
    from v2_trade_fees_usd_arbitrum
union all
select
    date,
    'avalanche' as chain,
    'v2' as version,
    block_timestamp,
    tx_hash,
    collateral_token,
    fees,
    volume,
    trader
from v2_trade_fees_usd_avalanche