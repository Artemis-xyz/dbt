{{ config(snowflake_warehouse="DEBRIDGE", materialized="table") }}
select id, chain, coingecko_id, decimals
from
    (
        values
            (42161, 'arbitrum', 'ethereum', 18),
            (43114, 'avalanche', 'avalanche-2', 18),
            (56, 'bsc', 'binancecoin', 18),
            (1, 'ethereum', 'ethereum', 18),
            (137, 'polygon', 'matic-network', 18),
            (250, 'fantom', 'fantom', 18),
            (7565164, 'solana', 'solana', 9),
            (59144, 'linea', 'ethereum', 18),
            (10, 'optimism', 'ethereum', 18),
            (8453, 'base', 'ethereum', 18),
            (100000001, 'neon', 'neon', 18),
            (100000002, 'gnosis', 'dai', 18),
            (100000003, 'lightlink', 'ethereum', 18),
            (100000004, 'metis', 'metis-token', 18),
            (100000005, 'bitrock', 'bitrock', 18),
            (100000014, 'sonic', 'sonic-3', 18),
            (100000006, 'crossfi', 'crossfi-2', 18),
            (100000010, 'cronos_zkevm','cronos-zkevm-cro', 18),
            (100000017, 'abstract', 'ethereum', 18),
            (100000020, 'berachain', 'berachain-bera', 18),
            (100000013, 'story', 'story-2', 18),
            (100000022, 'hyperliquid', 'hyperliquid', 18),
            (100000015, 'zircuit', 'ethereum', 18)
    ) as t(id, chain, coingecko_id, decimals)
