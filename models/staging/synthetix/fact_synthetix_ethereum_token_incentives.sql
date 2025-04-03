{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with reward_distributions as (
    select
        date(block_timestamp) as date, 
        sum(
            case
                when amount_usd is not null then amount_usd
                when contract_address ILIKE '0xc011a72400e58ecd99ee497cf89e3775d4bd732f' then (raw_amount/1e18)*eph.price
                else 0 end
        ) as incentives
    from ethereum_flipside.core.ez_token_transfers as ez
    join ethereum_flipside.price.ez_prices_hourly as eph
        on eph.hour = date_trunc('hour', ez.block_timestamp) and eph.symbol = 'SNX'
    where from_address ILIKE '0x29C295B046a73Cde593f21f63091B072d407e3F2' 
    group by date
), 

token_incentive_programs as (
    select 
        date(block_timestamp) as date, 
        sum(
            case
                when contract_address ILIKE '0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f ' and amount_usd is not null then amount_usd
                when contract_address ILIKE '0xc011a72400e58ecd99ee497cf89e3775d4bd732f' then (raw_amount/1e18)*eph.price
                when contract_address ILIKE '0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f' and amount_usd is null then (raw_amount/1e18)*eph.price
                else 0 end
        ) as incentives    
    from ethereum_flipside.core.ez_token_transfers as ez
    join ethereum_flipside.price.ez_prices_hourly as eph
        on eph.hour = date_trunc('hour', ez.block_timestamp) and eph.symbol = 'SNX'
    where (from_address ILIKE '0xdcb6a51ea3ca5d3fd898fd6564757c7aaec3ca92' or 
            from_address ILIKE '0x3f27c540ADaE3a9E8c875C61e3B970b559d7F65d' or
            from_address ILIKE '0x12DC1273915A16ab8BD47bA7866B240c253e4c12' or 
            from_address ILIKE '0xCed4055b47cfD0421f3727a35F69CE659c8bAF7a' or 
            from_address ILIKE '0xF0de877F2F9E7A60767f9BA662F10751566AD01c' or 
            from_address ILIKE '0xb59e8d8Ad40d06571DC0Cf9936E727846dfae93f' or 
            from_address ILIKE '0x167009dcDA2e49930a71712D956f02cc980DcC1b' or 
            from_address ILIKE '0x48D7f315feDcaD332F68aafa017c7C158BC54760' or  
            from_address ILIKE '0x8302FE9F0C509a996573D3Cc5B0D5D51e4FDD5eC' or    
            from_address ILIKE '0xFBaEdde70732540cE2B11A8AC58Eb2dC0D69dE10') 
        and contract_address ILIKE '0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f'
    group by date
)

select 
    coalesce(rd.date, tip.date) as date, 
    'ethereum' as chain,
    coalesce(rd.incentives,0) + coalesce(tip.incentives,0) as token_incentives
from reward_distributions as rd
full join token_incentive_programs as tip
    on rd.date = tip.date
order by date asc
