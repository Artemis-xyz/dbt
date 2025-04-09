{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with null_address_txns_to_0xd9 as (
    select 
        date(block_timestamp) as date, 
        symbol as token,
        sum(coalesce(amount_usd, 0)) as cash_flow
    from ethereum_flipside.core.ez_token_transfers
    where to_address ILIKE '0xd939611c3ca425b4f6d4a82591eab3da43c2f4a0'
        and from_address ILIKE '0x0000000000000000000000000000000000000000'
        and lower(origin_function_signature) not in (lower('0xee52a2f3'), 
                                                     lower('0x405d3adf'), 
                                                     lower('0xef7fae7c'), 
                                                     lower('0x30EAD760')
                                                     ) -- Exchanging one synth for another
    group by date, token
    order by date desc, token
), 

null_address_txns_to_0x99 as (
    select 
        date(block_timestamp) as date, 
        symbol as token,
        sum(
            case
                when origin_function_signature ILIKE '0x6A761202' and symbol NOT ILIKE 'sUSD' then 0
                else coalesce(amount_usd, 0)
            end
        ) as cash_flow
    from ethereum_flipside.core.ez_token_transfers
    where to_address ILIKE '0x99F4176EE457afedFfCB1839c7aB7A030a5e4A92'
        and from_address ILIKE '0x0000000000000000000000000000000000000000'
    group by date, token
    order by date desc, token
), 

treasury_inflow_excluding_null_address as (
    select
        date(block_timestamp) as date, 
        symbol as token, 
        sum(coalesce(amount_usd, 0)) as cash_flow
    from ethereum_flipside.core.ez_token_transfers
    where lower(to_address) in (lower('0xd939611c3ca425b4f6d4a82591eab3da43c2f4a0'), 
                                lower('0x99F4176EE457afedFfCB1839c7aB7A030a5e4A92'), -- Synthetix Treasury Council
                                lower('0x547d28cDd6A69e3366d6aE3EC39543F09Bd09417'), -- Treasury to receive inflows from Optimism
                                lower('0x579b66d0A7C48eEe63B3BD2bcA17bf0Fa0F0787b'), 
                                lower('0x7b955E5CA4d0D65e91e8d945Af5696d5F0707Bec'), 
                                lower('0xB91ef9a2c37C20803EfD33d98F09296d2468403b')
                                )
        -- Excluding exchanges from the recipients as this indicates an exchange and not direct cash flow
        and lower(from_address) not in (lower('0x9008d19f58aabd9ed0d60971565aa8510560ab41'), --CoWSwap
                                        lower('0xe069cb01d06ba617bcdf789bf2ff0d5e5ca20c71'), -- 1Inch
                                        lower('0xfd3dfb524b2da40c8a6d703c62be36b5d8540626'), -- 1Inch  
                                        lower('0x11111254369792b2ca5d084ab5eea397ca8fa48b'), -- 1Inch
                                        lower('0x7951c7ef839e26f63da87a42c9a87986507f1c07'), -- 1Inch
                                        lower('0x1111111254fb6c44bac0bed2854e76f90643097d'), -- Aggregator
                                        lower('0x28c6c06298d514db089934071355e5743bf21d60'), -- Binance
                                        lower('0xdfd5293d8e347dfe59e90efd55b2956a1343963d'), -- Binance
                                        lower('0x0681d8db095565fe8a346fa0277bffde9c0edbbf'), -- Binance
                                        lower('0x745daa146934b27e3f0b6bff1a6e36b9b90fb131'), -- DEX Aggregator  
                                        lower('0xba12222222228d8ba445958a75a0704d566bf2c8'), -- Balancer
                                        lower('0x6d19b2bf3a36a61530909ae65445a906d98a2fa8'), -- Balancer
                                        lower('0xae2d4617c862309a3d75a0ffb358c7a5009c673f'), -- Kraken
                                        lower('0x43d6a102db838943cc8e77ac6fb4f3565dceb8df'), -- Sushi
                                        lower('0x0C1887e602da2AA96Aa0642A37a32DbC2a142213') -- Uniswap
                                        )
            -- Excluding inflow from other treasury addresses to avoid double counting
        and lower(from_address) not in (lower('0xd939611c3ca425b4f6d4a82591eab3da43c2f4a0'), 
                                        lower('0x99F4176EE457afedFfCB1839c7aB7A030a5e4A92'), -- Synthetix Treasury Council
                                        lower('0x547d28cDd6A69e3366d6aE3EC39543F09Bd09417'), -- Treasury to receive inflows from Optimism
                                        lower('0x579b66d0A7C48eEe63B3BD2bcA17bf0Fa0F0787b'), 
                                        lower('0x7b955E5CA4d0D65e91e8d945Af5696d5F0707Bec'), 
                                        lower('0xB91ef9a2c37C20803EfD33d98F09296d2468403b')
                                        )
            -- Excluding inflow from null address because this is counted by the CTE
        and from_address NOT ILIKE '0x0000000000000000000000000000000000000000'                                
    group by date, token
    order by date desc, token
)

select
    coalesce(cte_nonnull.date, cte_0xd9.date, cte_0x99.date) as date, 
    coalesce(cte_nonnull.token, cte_0xd9.token, cte_0x99.token) as token, 
    coalesce(cte_nonnull.cash_flow, 0) + coalesce(cte_0xd9.cash_flow, 0) + coalesce(cte_0x99.cash_flow, 0) as cash_flow
from treasury_inflow_excluding_null_address as cte_nonnull
full join null_address_txns_to_0xd9 as cte_0xd9
    on cte_nonnull.date = cte_0xd9.date and cte_nonnull.token = cte_0xd9.token
full join null_address_txns_to_0x99 as cte_0x99
    on coalesce(cte_nonnull.date, cte_0xd9.date) = cte_0x99.date and coalesce(cte_nonnull.token, cte_0xd9.token) = cte_0x99.token
order by date desc, token