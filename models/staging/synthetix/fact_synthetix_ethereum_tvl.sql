{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with partitioned_transactions as (
    select 
        date_trunc('hour', block_timestamp) as hourly_timestamp, 
        contract_address, 
        balance, 
        row_number() over (
            partition by date_trunc('day', block_timestamp), contract_address
            order by block_timestamp desc
        ) as rn
    from ethereum_flipside.core.fact_token_balances
    where user_address = '0xffffffaeff0b96ea8e4f94b2253f31abdd875847' and block_timestamp > '2025-03-01'
    order by block_timestamp desc
)

select 
    date_trunc('day',hourly_timestamp) as date, 
    sum(coalesce(balance,0)*eph.price) as tvl_usd
from partitioned_transactions as pt
inner join ethereum_flipside.price.ez_prices_hourly as eph
    on pt.contract_address = eph.token_address and pt.hourly_timestamp = eph.hour
where rn = 1
group by date
order by date desc