{{ config(materialized="table") }}
select
    app,
    chain,
    category,
    coingecko_id,
    defillama_chain_name,
    defillama_protocol_id,
    data_quality_priority
from
    (
        values
            -- app, chain, category, coingecko_id, DL chain name, DL protocol ID,
            -- priorty
            (null, 'bitcoin', null, 'bitcoin', 'bitcoin', null, 'High'),
            (null, 'ethereum', null, 'ethereum', 'ethereum', null, 'High'),
            (null, 'bsc', null, 'binancecoin', 'binance', null, 'High'),
            (null, 'solana', null, 'solana', 'solana', null, 'High'),
            (null, 'cardano', null, 'cardano', 'cardano', null, 'High'),
            (null, 'avalanche', null, 'avalanche-2', 'avalanche', null, 'High'),
            (null, 'tron', null, 'tron', 'tron', null, 'High'),
            (null, 'polygon', null, 'matic-network', 'polygon', null, 'High'),
            (null, 'cosmoshub', null, 'cosmos', 'cosmos', null, 'High'),
            (null, 'near', null, 'near', 'near', null, 'High'),
            (null, 'aptos', null, 'aptos', 'aptos', null, 'High'),
            (null, 'optimism', null, 'optimism', 'optimism', null, 'High'),
            (null, 'arbitrum', null, 'arbitrum', 'arbitrum', null, 'High'),
            ('chainlink', null, 'Utility', 'chainlink', null, 2623, 'High'),
            ('uniswap', null, 'DeFi', 'uniswap', null, 2198, 'High'),
            ('lido', null, 'DeFi', 'lido-dao', null, 182, 'High'),
            ('thegraph', null, 'Utility', 'the-graph', null, null, 'High'),
            ('aave', null, 'DeFi', 'aave', null, 111, 'High'),
            (null, 'stacks', null, 'blockstack', 'stacks', null, 'High'),
            ('makerdao', null, 'DeFi', 'maker', null, 118, 'High'),
            (null, 'flow', null, 'flow', 'flow', null, 'High'),
            ('sandbox', null, 'Gaming', null, null, null, 'High'),
            (null, 'fantom', null, 'fantom', 'fantom', null, 'High'),
            ('axieinfinity', null, 'Gaming', null, null, null, 'High'),
            ('gala', null, 'Gaming', 'gala', null, null, 'High'),
            ('decentraland', null, 'Gaming', 'decentraland', null, null, 'High'),
            ('beam', null, 'Token', null, null, null, 'High'),
            (null, 'sui', null, 'sui', 'sui', null, 'High'),
            ('fraxshare', null, 'Token', null, null, null, 'High'),
            ('curvefi', null, 'DeFi', null, null, null, 'High'),
            ('apecoin', null, 'Gaming', 'apecoin', null, 2665, 'High'),
            ('illuvium', null, 'Gaming', 'illuvium', null, 447, 'High'),
            ('pancakeswap', null, 'DeFi', 'pancakeswap-token', null, 2769, 'High'),
            (null, 'gnosis', null, 'gnosis', 'xdai', null, 'High'),
            ('rocketpool', null, 'DeFi', 'rocket-pool', null, null, 'High'),
            ('blur', null, 'NFT Apps', 'blur', null, 2414, 'High'),
            ('dydx', null, 'DeFi', 'dydx', null, 144, 'High'),
            ('dydx_v4', 'dydx_v4', 'DeFi', null, null, 46, 'High'),
            ('1inch', null, 'DeFi', null, null, null, 'High'),
            ('compound', null, 'DeFi', 'compound-governance-token', null, null, 'High'),
            (null, 'zksync', null, null, 'zksync era', null, 'High'),
            (null, 'polygon_zk', null, 'matic-network', 'polygon zkevm', null, 'High')
    ) as results(
        app,
        chain,
        category,
        coingecko_id,
        defillama_chain_name,
        defillama_protocol_id,
        data_quality_priority
    )
