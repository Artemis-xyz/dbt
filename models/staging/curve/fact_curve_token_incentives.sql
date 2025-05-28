{{ config(materialized="table") }}

select
    date(block_timestamp) as date,
    'ethereum' as chain,
    'CRV' as token,
    to_address as emission_contract,
    sum(amount) as minted_amount,
    sum(amount_usd) as minted_usd
from {{source('ETHEREUM_FLIPSIDE', 'ez_token_transfers')}}
where contract_address = lower('0xD533a949740bb3306d119CC777fa900bA034cd52') -- CRV token
and from_address = '0x0000000000000000000000000000000000000000' -- Zero address (minting)
group by 1, 2, 3, 4