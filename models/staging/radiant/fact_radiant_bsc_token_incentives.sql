{{config(materialized="table", snowflake_warehouse='RADIANT')}}

select
    cast(block_timestamp as date) as date
    , 'bsc' as chain
    , sum(amount) as amount_native
    , sum(amount_usd) as amount_usd
from bsc_flipside.core.ez_token_transfers
where contract_address = lower('0xf7DE7E8A6bd59ED41a4b5fe50278b3B7f31384dF')
    and from_address in (lower('0x7C16aBb090d3FB266E9d17F60174B632f4229933'))
    -- moving RDNT from the incentives contract to a multisig wallet (not token incentives)
    and tx_hash != '0x8b91f3726a06df4e661312ebe8757bf07dec0e9eef2bcf7efa6d92adc3296365'
group by date, chain
