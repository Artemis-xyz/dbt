{{ config(materialized="table") }}

select
    block_timestamp::date as date,
    tx_hash,
    amount,
    amount_precise,
    amount_usd,
    from_address,
    to_address
from {{source('BSC_FLIPSIDE', 'ez_token_transfers')}}
where from_address = lower('0xa5f8C5Dbd5F286960b9d90548680aE5ebFf07652') --PancakeSwap MasterChef V2
and contract_address = lower('0x0E09FaBB73Bd3Ade0a17ECC321fD13a19e81cE82') --CAKE token
and amount_usd < 90000 -- filter out outliers which include edge cases (burns, protocol self-distribution, etc)
and block_timestamp::date < to_date(sysdate())
