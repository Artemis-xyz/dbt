{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with ethereum_fee_sharing as (
    select
        date(block_timestamp) as date, 
        'Ethereum' as chain,
        symbol as token,
        sum(amount_usd) as fee_sharing_cash_flow
    from ethereum_flipside.core.ez_token_transfers
            -- DAO Addresses
    where lower(from_address) in (lower('0xeb3107117fead7de89cd14d463d340a2e6917769'), --ProtocolDAO
                                lower('0x49BE88F0fcC3A8393a59d3688480d7D253C37D2A') --sDAO
                                ) 
            -- All Reward Contracts
        and lower(to_address) in (lower('0xc746bc860781dc90bbfcd381d6a058dc16357f8d'), --LP Rewards iETH
                                lower('0x167009dcda2e49930a71712d956f02cc980dcc1b'), --Staking Rewards iBTC
                                lower('0xCed4055b47cfD0421f3727a35F69CE659c8bAF7a'), --Shorting Rewards sBTC
                                lower('0x12DC1273915A16ab8BD47bA7866B240c253e4c12'), --Shorting Rewards sETH
                                lower('0xFBaEdde70732540cE2B11A8AC58Eb2dC0D69dE10'), --Staking Rewards Balancer SNX
                                lower('0x7af65f1740c0eB816A27FD808EaF6Ab09F6Fa646'), --Staking Rewards Balancer AAPL
                                lower('0xDC338C7544654c7dadFEb7E44076E457963113B0'), --Staking Rewards Balancer AMZN
                                lower('0x1C1D97f6338759AB814a5A717aE359573Ab5D5d4'), --Staking Rewards Balancer COIN
                                lower('0x26Fa0665660c1D3a3666584669511d3c66Ad37Cb'), --Staking Rewards Balancer FB
                                lower('0x6fB7F0E78582746bd01BcB6dfbFE62cA5F4F9175'), --Staking Rewards Balancer GOOG
                                lower('0x9D003Cc298E7Ea141A809C241C0a703176DA3ba3'), --Staking Rewards Balancer MSFT
                                lower('0x8Ef8cA2AcAaAfEc19fB366C11561718357F780F2'), --Staking Rewards Balancer NFLX
                                lower('0xF0de877F2F9E7A60767f9BA662F10751566AD01c'), --Staking Rewards Balancer TSLA
                                lower('0xc0d8994Cd78eE1980885DF1A0C5470fC977b5cFe'), --Staking Rewards sEUR Curve
                                lower('0x8302FE9F0C509a996573D3Cc5B0D5D51e4FDD5eC') --Staking Rewards Uniswap V2 (sXAU)
                                )
        and amount_usd is not null
    group by 1, 2, 3
    order by date desc
), 

optimism_fee_sharing as (
    select
        date(block_timestamp) as date, 
        'Optimism' as chain, 
        symbol as token,
        sum(amount_usd) as fee_sharing_cash_flow
    from optimism_flipside.core.ez_token_transfers
    where lower(to_address) = lower('0xfD49C7EE330fE060ca66feE33d49206eB96F146D')
        and lower(from_address) = lower('0x9644A6920bd0a1923C2C6C1DddF691b7a42e8A65')
        and amount_usd is not null
    group by 1, 2, 3
    order by date desc
)

select * from ethereum_fee_sharing
union all
select * from optimism_fee_sharing



