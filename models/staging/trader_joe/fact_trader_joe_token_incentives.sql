{{ config(materialized="table") }}

with token_incentives as (
    select
        block_timestamp::date as date,
        tx_hash,
        decoded_log:user::STRING as recipient,
        decoded_log:amount::NUMBER as amount, 
        t.decimals as decimal, 
        t.price as price,
        amount / POW(10, t.decimals) as amount_adjusted,
        amount / POW(10, t.decimals) * t.price as amount_usd
    from avalanche_flipside.core.ez_decoded_event_logs 
    left join avalanche_flipside.price.ez_prices_hourly t
        on token_address = lower('0x6e84a6216eA6dACC71eE8E6b0a5B7322EEbC0fDd') and date_trunc('hour', block_timestamp::date) = t.hour 
    where contract_address in (lower('0x4483f0b6e2F5486D06958C20f8C39A7aBe87bf8F'), lower('0x188bED1968b795d5c9022F6a0bb5931Ac4c18F00'), lower('0xd6a4F121CA35509aF06A0Be99093d08462f53052') ) --MasterChefJoe contracts v1, v2, v3
    and lower(recipient) not in (lower('0x4483f0b6e2F5486D06958C20f8C39A7aBe87bf8F'), lower('0x188bED1968b795d5c9022F6a0bb5931Ac4c18F00'), lower('0xd6a4F121CA35509aF06A0Be99093d08462f53052')) --Only harvests from MasterChefJoe
    and event_name = 'Harvest'
    and amount_usd < 10000000 --Harvests over 10M USD filtered out as edge cases
    and amount_usd is not null
    order by amount_usd desc
) select * from token_incentives