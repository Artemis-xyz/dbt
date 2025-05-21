SELECT
    date
    , 'ethereum' AS chain
    , trading_volume
    , total_trades
    , total_fees_usd
    , active_wallets
    , total_platform_fees
    , total_creator_fees
FROM 
    {{ ref('fact_magiceden_ethereum_transactions') }}

UNION ALL

SELECT
    date
    , 'solana' as chain
    , trading_volume
    , total_trades
    , total_fees_usd
    , active_wallets
    , total_platform_fees
    , total_creator_fees
FROM 
    {{ ref('fact_magiceden_solana_transactions') }}