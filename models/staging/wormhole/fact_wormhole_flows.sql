{{ config(materialized="table") }}

with
    transfers as (
        select
            id,
            timestamp,
            amount,
            app_ids,
            fee,
            fee_address,
            fee_chain,
            from_address,
            from_chain,
            to_address,
            to_chain,
            token_address,
            token_chain,
            amount_usd,
            symbol
        from {{ ref("fact_wormhole_transfers") }}
        where amount_usd is not null
    ),

    -- https://docs.wormhole.com/wormhole/reference/constants
    chain_ids as (
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
                    (40, 'seievm'),
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
    ),

    dim_contracts as (
        select distinct address, chain, category
        from {{ ref("dim_contracts_gold") }} 
        where category is not null and chain is not null
    ),

    volume_by_chain_and_symbol as (
        select
            date_trunc('day', timestamp) as date,
            c1.chain as source_chain,
            c2.chain as destination_chain,
            coalesce(t2.category, t3.category, 'Not Categorized') as category,
            amount_usd
        from transfers t
        left join chain_ids c1 on t.from_chain = c1.id
        left join chain_ids c2 on t.to_chain = c2.id
        left join dim_contracts t2 on lower(t.token_address) = lower(t2.address) and c1.chain = t2.chain
        left join dim_contracts t3 on lower(t.token_address) = lower(t3.address) and c2.chain = t3.chain
    )

-- note source and destination chain may be null if not provided above
select
    date,
    'wormhole' as app,
    source_chain,
    destination_chain,
    category,
    sum(amount_usd) as amount_usd,
    null as fee_usd
from volume_by_chain_and_symbol
where source_chain is not null and destination_chain is not null
group by 1, 2, 3, 4, 5
