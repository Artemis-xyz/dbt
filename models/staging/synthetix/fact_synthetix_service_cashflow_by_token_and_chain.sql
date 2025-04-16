{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with ethereum_service_cashflow as (
    select
        date(block_timestamp) as date, 
        'Ethereum' as chain,
        symbol as token,
        sum(amount_usd) as service_cashflow
    from ethereum_flipside.core.ez_token_transfers
    where lower(origin_function_signature) in (lower('0x458efde3'), lower('0x34c7fec9'))                                        
        and lower(symbol) = lower('SNX')
    group by 1, 2, 3
    order by date desc
), 

optimism_service_cashflow as (
    select
        date(block_timestamp) as date, 
        'Optimism' as chain,
        symbol as token,
        sum(amount_usd) as service_cashflow
    from optimism_flipside.core.ez_token_transfers
    where lower(origin_function_signature) in (lower('0x34c7fec9'))                                        
        and lower(symbol) = lower('SNX')
    group by 1, 2, 3
    order by date desc
)

select * from ethereum_service_cashflow
union all
select * from optimism_service_cashflow


