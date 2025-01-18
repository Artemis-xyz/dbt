SELECT
    date,
    'ethereum' AS chain,
    daily_trading_volume_usd,
    active_wallets,
    collections_transacted,
    total_trades,
    total_platform_fees,
    total_creator_fees,
    total_fees_usd
FROM 
    {{ ref('fact_magiceden_ethereum') }}

UNION ALL

SELECT
    date,
    'solana' as chain,
    daily_trading_volume_usd,
    active_wallets,
    collections_transacted,
    total_trades,
    total_platform_fees,
    total_creator_fees,
    total_fees_usd
FROM 
    {{ ref('fact_magiceden_solana') }}