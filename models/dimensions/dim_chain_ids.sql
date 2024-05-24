{{ config(materialized="table") }}
select id, chain
from
    (
        values
            (1, 'ethereum'),
            (10, 'optimism'),
            (137, 'polygon'),
            (288, 'boba'),
            (42161, 'arbitrum'),
            (8453, 'base'),
            (324, 'zksync'),
            (59144, 'linea'),
            (1313161554, 'aurora'),
            (250, 'fantom'),
            (1088, 'metis'),
            (1284, 'moonbeam'),
            (8217, 'klaytn'),
            (43114, 'avalanche'),
            (1285, 'moonriver'),
            (7700, 'canto'),
            (2000, 'dogechain'),
            (53935, 'dfk'),
            (1666600000, 'harmony'),
            (56, 'bsc'),
            (25, 'cronos'),
            (81457, 'blast'),
            (534352, 'scroll'),
            (34443, 'mode')
    ) as t(id, chain)