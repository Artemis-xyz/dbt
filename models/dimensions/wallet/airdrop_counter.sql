with
    optimism_airdrop as (
        select
            from_address,
            date(block_timestamp) as received_date,
            'optimism_1' as airdrop
        from pc_dbt_db.prod.fact_optimism_transactions_v2
        where
            contract_address like '0xfedfaf1a10335448b7fa0268f56d2b44dbd357de'
            and date(block_timestamp) >= date('2022-06-01')
    ),

    ethereum_airdrop as (

        select from_address, date(block_timestamp) as received_date, app as airdrop
        from pc_dbt_db.prod.fact_ethereum_transactions_v2
        where
            contract_address in (
                '0xf2d15c0a89428c9251d71a0e29b39ff1e86bce25',
                '0xc5102fe9359fd9a28f877a67e36b0f050d81a3cc',
                '0x090d4613473dee047c3f2706764f49e0821d256e',
                '0xde3e5a990bce7fc60a6f017e7c4a95fc4939299e',
                '0x639192d54431f8c816368d3fb4107bc168d0e871',
                '0xc18360217d8f7ab5e7c516566761ea12ce7f9d72',
                '0x090e53c44e8a9b6b1bca800e881455b921aec420',
                '0xa35dce3e0e6ceb67a30b8d7f4aee721c949b5970',
                '0x3efa30704d2b8bbac821307230376556cf8cc39e',
                '0xe50b2ceac4f60e840ae513924033e753e2366487',
                '0xf92cdb7669a4601dd76b728e187f2a98092b6b7d',
                '0x1a9a4d919943340b7e855e310489e16155f4ed29',
                '0xe6949137b24ad50cce2cf6b124b3b874449a41fa',
                '0x92e130d5ed0f14199edfa050071116ca60e99aa5',
                '0x7902e4bfb1eb9f4559d55417aee1dc6e4b8cc1bf',
                '0xe810281d189f19572b5250556369c39f5ebc6b00'
            )
            and block_timestamp > '2020-09-01'
    ),

    arbitrum_airdrop as (
        select
            from_address, date(block_timestamp) as received_date, 'arbitrum' as airdrop
        from arbitrum_flipside.core.fact_transactions
        where
            to_address like '0x67a24ce4321ab3af51c2d0a4801c3e111d88c9d9'
            and origin_function_signature = '0x4e71d92d'
            and block_timestamp between '2023-03-15' and '2023-07-15'
    )

select *
from arbitrum_airdrop

union all

select *
from optimism_airdrop

union all

select *
from ethereum_airdrop
