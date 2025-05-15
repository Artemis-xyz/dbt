{{ config(materialized="table") }}

--Initial mint of 1,000,000,000 VRTX tokens
with vertex_initial_mint as (
    select
        date(block_timestamp) as date,
        'arbitrum' as chain,
        'VRTX' as token,
        to_address as emission_contract,
        sum(amount) as minted_amount,
    from arbitrum_flipside.core.ez_token_transfers
    where 
        contract_address = lower('0x95146881b86b3ee99e63705ec87afe29fcc044d9') -- VRTX token
        AND from_address = '0x0000000000000000000000000000000000000000' -- Zero address (minting)
        AND block_timestamp <= '2023-12-01' 
    group by 1, 2, 3, 4
)

--Vrtx claim events
, claim_events as (
    select
        block_timestamp,
        date(block_timestamp) as date,
        'arbitrum' as chain,
        'VRTX' as token,
        decoded_log:account::STRING as to_address,
        decoded_log:amount::NUMBER as amount,
    from arbitrum_flipside.core.ez_decoded_event_logs
    where 
        contract_address = lower('0xafe39cd8e17fa4172144ff95274bb665da411f80') -- VRTX token
        AND event_name = 'ClaimVrtx' -- Zero address (minting)
        AND block_timestamp <= '2024-07-01'
)

select
    date,
    sum(amount) as amount,
    sum( (amount / pow(10, decimals)) * price ) as amount_usd
from claim_events
left join arbitrum_flipside.price.ez_prices_hourly
    on token_address = lower('0x95146881b86b3ee99e63705ec87afe29fcc044d9')
    and hour = date_trunc('hour', block_timestamp)
group by date
union all
select
    date,
    sum(minted_amount) as amount,
    sum(minted_amount * .019368) as amount_usd --Initial price of VRTX on mint
from vertex_initial_mint
group by date