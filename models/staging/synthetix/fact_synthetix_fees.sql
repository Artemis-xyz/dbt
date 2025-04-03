{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with optimism_fees as (
    select 
        date(block_timestamp) as date, 
        sum(amount) as op_fees
    from optimism_flipside.core.ez_token_transfers
    where to_address ILIKE '0xfeefeefeefeefeefeefeefeefeefeefeefeefeef' 
        and symbol ILIKE 'sUSD'
        and block_timestamp > '2020-05-18'
    group by date
), 

ethereum_fees as (
    select 
        date(block_timestamp) as date,
        sum(case 
                when contract_address ILIKE '0x57ab1ec28d129707052df4df418d58a2d46d5f51' then amount_usd
                when contract_address ILIKE '0xb3f67de9a919476a4c0fe821d67bf5c4637d8429' then (raw_amount/1e18)*1.4
                when contract_address ILIKE '0x57ab1e02fee23774580c119740129eac7081e9d3' then raw_amount/1e18
                when contract_address ILIKE '0x62492f15cf60c5847d3053e482cade8c5c29af88' then (raw_amount/1e18)*1.4
                else 0 end
            ) as eth_fees
    from ethereum_flipside.core.ez_token_transfers
    where to_address ILIKE '0xfeefeefeefeefeefeefeefeefeefeefeefeefeef'
        and block_timestamp > '2020-05-18'
    group by date
)

select 
    coalesce(op.date, eth.date) as date, 
    coalesce(op_fees,0) + coalesce(eth_fees,0) as fees, 
from optimism_fees as op
full join ethereum_fees as eth
    on op.date = eth.date
order by date asc