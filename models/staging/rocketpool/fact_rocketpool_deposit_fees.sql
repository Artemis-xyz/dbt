{{ config(
    materialized="table",
    snowflake_warehouse="ROCKETPOOL"
    )
}}

with prices as (
    SELECT hour, price FROM ethereum_flipside.price.ez_prices_hourly
    WHERE is_native = true
)
SELECT
    date(block_timestamp) as date
    , sum(decoded_log:amount::number/1e18) as deposit_amount_eth
    , sum(decoded_log:amount::number/1e18 * 0.0005) as deposit_fee_eth
    , sum(decoded_log:amount::number/1e18 * p.price) as deposit_amount_usd
    , sum(decoded_log:amount::number/1e18 * p.price * 0.0005) as deposit_fee_usd
FROM
ethereum_flipside.core.ez_decoded_event_logs l
LEFT JOIN prices p on p.hour = date_trunc('hour', l.block_timestamp)
WHERE event_name = 'DepositReceived'
and contract_address in (lower('0xDD3f50F8A6CafbE9b31a427582963f465E745AF8'), lower('0x9304B4ebFbE68932Cf9Af8De4d21D7e7621f701a'), lower('0x2cac916b2A963Bf162f076C0a8a4a8200BCFBfb4'))
GROUP BY 1