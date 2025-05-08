{{ config(materialized="incremental", snowflake_warehouse="TRON", unique_key=["transaction_hash", "event_index"]) }}
with
prices as ({{get_multiple_coingecko_price_with_latest('tron')}})
-- TRON USDT supply for the address below is negative to begin with, this means its first transfer is out 
-- not in, the data at the beginning of tron is pretty iffy and the block explorer seems to fail the closer you
-- get to the genesis block. it is only max negative by $10 over its history so I am giving it an inital supply of 10000000/1e6 USDT
-- THPvaUhoh2Qn2y9THCZML3H815hhFhn5YC is the contract creator
-- https://tronscan.org/#/tools/advanced-filter?type=transfer&secondType=20&times=1530417600000%2C1556769599999&fromAddress=THPvaUhoh2Qn2y9THCZML3H815hhFhn5YC&toAddress=THPvaUhoh2Qn2y9THCZML3H815hhFhn5YC&token=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t&imgUrl=https%3A%2F%2Fstatic.tronscan.org%2Fproduction%2Flogo%2Fusdtlogo.png&tokenName=Tether%20USD&tokenAbbr=USDT&relation=or
, inital_token_transfers as (
    --This Transaction is never emitted on chain so the values are hardcoded
    select block_timestamp, block_number, transaction_hash, transaction_index, event_index, contract_address, from_address, to_address, amount_raw, amount_native, 
    from (
        values 
        ( '2019-04-16 07:51:09.000'::timestamp, 8418439, '0000000000000000000000000000000000000000000000000000000000000000', -1, 1, 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t', 'T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb', 'THPvaUhoh2Qn2y9THCZML3H815hhFhn5YC', 10000000, 10)
    ) as t(block_timestamp, block_number, transaction_hash, transaction_index, event_index, contract_address, from_address, to_address, amount_raw, amount_native)
    {% if is_incremental() %}
        where block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
)
, token_transfers as (
    select
        datetime as block_timestamp
        , block_number
        , transaction_hash
        , transaction_index
        , log_index as event_index
        , token_address as contract_address
        , trx_from_address as from_address
        , trx_to_address as to_address
        , source_value as amount_raw
        , value as amount_native
    from SONARX_TRON.TRON_SHARE.TOKEN_TRANSFERS
    where type = 'TRC20' and transaction_info_result = 'SUCCESS'
    {% if is_incremental() %}
        and datetime >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
    union all
    select block_timestamp, block_number, transaction_hash, transaction_index, event_index, contract_address, from_address, to_address, amount_raw, amount_native
    from inital_token_transfers
)  
select
    block_timestamp
    , block_number
    , transaction_hash
    , transaction_index
    , event_index
    , token_transfers.contract_address
    , from_address
    , to_address
    , amount_raw
    , amount_native
    , amount_native * prices.price as amount
    , prices.price
from token_transfers
left join prices
    on token_transfers.block_timestamp::date = prices.date
    and lower(token_transfers.contract_address) = lower(prices.contract_address)
where amount_raw > 0