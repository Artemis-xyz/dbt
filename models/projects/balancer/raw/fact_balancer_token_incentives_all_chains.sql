{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='raw',
        alias='fact_balancer_token_incentives_all_chains'
    )
}}

with merkle_redeem as (
    select
        block_timestamp::date as date,
        price,
        decimals,
        decoded_log:_balance::NUMBER as amount,
        decoded_log:_claimant::STRING as recipient_address,
        (decoded_log:_balance::NUMBER / pow(10, decimals)) * price as amount_usd
    from {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }}
    left join {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} 
        on lower('0xba100000625a3754423978a60c9317c58a424e3d') = token_address -- BAL token
        and date_trunc('hour', block_timestamp) = hour
    where contract_address = lower('0x6d19b2bf3a36a61530909ae65445a906d98a2fa8')
    and event_name = 'Claimed'
)
, merkle_orchard as (
    select
        block_timestamp::date as date,
        price,
        decimals,
        decoded_log,
        decoded_log:amount::NUMBER as amount,
        decoded_log:claimer::STRING as recipient_address,
        decoded_log:token::STRING as token_address,
        (decoded_log:amount::NUMBER / pow(10, decimals)) * price as amount_usd
    from {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }}
    left join {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} 
        on lower('0xba100000625a3754423978a60c9317c58a424e3d') = token_address -- BAL token
        and date_trunc('hour', block_timestamp) = hour
    where contract_address = lower('0xdae7e32adc5d490a43ccba1f0c736033f2b4efca')
    and event_name = 'DistributionClaimed'
    and decoded_log:token::STRING = lower('0xba100000625a3754423978a60c9317c58a424e3d')
)
, merkle_orchard_arbitrum as (
    select
        block_timestamp::date as date,
        price,
        decimals,
        decoded_log,
        decoded_log:amount::NUMBER as amount,
        decoded_log:claimer::STRING as recipient_address,
        decoded_log:token::STRING as token_address,
        lower(decoded_log:token::STRING) as lower_token_address,
        (decoded_log:amount::NUMBER / pow(10, decimals)) * price as amount_usd
    from {{ source('ARBITRUM_FLIPSIDE', 'ez_decoded_event_logs') }} a
    left join {{ source('ARBITRUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} t
        on t.token_address = '0x040d1edc9569d4bab2d15287dc5a4f10f56a56b8' -- BAL token
        and date_trunc('hour', block_timestamp) = hour
    where contract_address = lower('0x751A0bC0e3f75b38e01Cf25bFCE7fF36DE1C87DE')
    and event_name = 'DistributionClaimed'
    and lower_token_address = '0x040d1edc9569d4bab2d15287dc5a4f10f56a56b8' --lower('0x040d1EdC9569d4Bab2D15287Dc5A4F10F56a56B8 ')
)

, merkle_orchard_polygon as (
    select
        block_timestamp::date as date,
        price,
        decimals,
        decoded_log,
        decoded_log:amount::NUMBER as amount,
        decoded_log:claimer::STRING as recipient_address,
        decoded_log:token::STRING as token_address,
        (decoded_log:amount::NUMBER / pow(10, decimals)) * price as amount_usd
    from {{ source('POLYGON_FLIPSIDE', 'ez_decoded_event_logs') }}
    left join {{ source('POLYGON_FLIPSIDE_PRICE', 'ez_prices_hourly') }} 
        on lower('0x9a71012b13ca4d3d0cdc72a177df3ef03b0e76a3') = token_address -- BAL token
        and date_trunc('hour', block_timestamp) = hour
    where contract_address = lower('0x0f3e0c4218b7b0108a3643cfe9d3ec0d4f57c54e')
    and event_name = 'DistributionClaimed'
    and decoded_log:token::STRING = lower('0x9a71012b13ca4d3d0cdc72a177df3ef03b0e76a3')
)
, emissions as (
    select
        date(block_timestamp) as date,
        'ethereum' as chain,
        'BAL' as token,
        to_address as emission_contract,
        sum(amount) as amount,
        sum(amount_usd) as amount_usd
    from {{ source('ETHEREUM_FLIPSIDE', 'ez_token_transfers') }}
    where 
        contract_address = lower('0xba100000625a3754423978a60c9317c58a424e3d') -- BAL token
        AND from_address = '0x0000000000000000000000000000000000000000' -- Zero address (minting)
        AND block_timestamp >= '2022-04-01'
    group by 1, 2, 3, 4
)
select
    date,
    'ethereum' as chain,
    sum(amount) as amount,
    sum(amount_usd) as amount_usd
from merkle_orchard
group by date, chain
union all
select
    date,
    'ethereum' as chain,
    sum(amount) as amount,
    sum(amount_usd) as amount_usd
from merkle_redeem
group by date, chain
union all
select
    date,
    'arbitrum' as chain,
    sum(amount) as amount,
    sum(amount_usd) as amount_usd
from merkle_orchard_arbitrum
group by date, chain
union all
select
    date,
    'polygon' as chain,
    sum(amount) as amount,
    sum(amount_usd) as amount_usd
from merkle_orchard_polygon
group by date, chain
union all
select
    date,
    'ethereum_emissions' as chain,
    sum(amount) as amount,
    sum(amount_usd) as amount_usd
from emissions
group by date, chain