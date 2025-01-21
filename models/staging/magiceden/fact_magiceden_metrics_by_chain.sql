SELECT
    date,
    'ethereum' AS chain,
    SUM(trading_volume)as daily_trading_volume,
    SUM(active_wallets) as active_wallets,
    SUM(collections_transacted) as collections_transacted,
    SUM(total_trades) as total_trades,
    SUM(total_platform_fees) as total_platform_fees,
    SUM(total_creator_fees) as total_creator_fees,
    SUM(total_fees_usd) as total_fees_usd
FROM 
    {{ ref('fact_magiceden_ethereum_transactions') }}
GROUP BY
    1

UNION ALL

SELECT
    date,
    'solana' as chain,
    SUM(trading_volume) as daily_trading_volume,
    SUM(active_wallets) as active_wallets,
    SUM(collections_transacted) as collections_transacted,
    SUM(total_trades) as total_trades,
    SUM(total_platform_fees) as total_platform_fees,
    SUM(total_creator_fees) as total_creator_fees,
    SUM(total_fees_usd) as total_fees_usd
FROM 
    {{ ref('fact_magiceden_solana_transactions') }}
GROUP BY
    1 