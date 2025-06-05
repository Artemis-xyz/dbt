{{ config(materialized="table") }}

select
    block_timestamp::date as date,
    sum(amount_usd) as token_incentives
from {{ source("OPTIMISM_FLIPSIDE", "ez_token_transfers") }}
where from_address = lower('0x2501c477d0a35545a387aa4a3eee4292a9a8b3f0')
and contract_address = lower('0x4200000000000000000000000000000000000042')
and amount_usd < 10000000
group by date