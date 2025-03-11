{{ config(snowflake_warehouse="WORMHOLE", materialized="table") }}
select id, chain
from
    (
        values
            (1, 'solana'),
            (2, 'ethereum'),
            (3, 'terra'),
            (4, 'bsc'),
            (5, 'polygon'),
            (6, 'avalanche'),
            (7, 'oasis'),
            (8, 'algorand'),
            (9, 'aurora'),
            (10, 'fantom'),
            (11, 'karura'),
            (12, 'acala'),
            (13, 'klaytn'),
            (14, 'celo'),
            (15, 'near'),
            (16, 'moonbeam'),
            (17, 'neon'),
            (18, 'terra2'),
            (19, 'injective'),
            (20, 'osmosis'),
            (21, 'sui'),
            (22, 'aptos'),
            (23, 'arbitrum'),
            (24, 'optimism'),
            (25, 'gnosis'),
            (26, 'pythnet'),
            (28, 'xpla'),
            (30, 'base'),
            (32, 'sei'),
            (33, 'rootstock'),
            (34, 'scroll'),
            (35, 'mantle'),
            (36, 'blast'),
            (37, 'xlayer'),
            (38, 'linea'),
            (39, 'berachain'),
            (40, 'sei'),
            (4000, 'cosmoshub'),
            (4001, 'evmos'),
            (4002, 'kujira'),
            (4003, 'neutron'),
            (4004, 'celestia'),
            (4005, 'stargaze'),
            (4006, 'seda'),
            (4007, 'dymension'),
            (4008, 'provenance')
    ) as t(id, chain)
