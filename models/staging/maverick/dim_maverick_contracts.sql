-- base
select
    to_address as address,
    'maverick' as app,
    'base' as chain,
    'Pool' as name,
    1 as version,
    'DeFi' as category
from base_flipside.core.fact_traces
where
    from_address = lower('0xB2855783a346735e4AAe0c1eb894DEf861Fa9b45')
    and output like '0x60806040%'
union
select address, app, chain, name, version, category
from
    (
        values
            (
                '0xB2855783a346735e4AAe0c1eb894DEf861Fa9b45',
                'maverick',
                'base',
                'Factory',
                1,
                'DeFi'
            ),
            (
                '0x0d8127A01bdb311378Ed32F5b81690DD917dBa35',
                'maverick',
                'base',
                'Position',
                1,
                'DeFi'
            ),
            (
                '0x550056A68cB155b6Cc3DeF4A7FA656260e7842e2',
                'maverick',
                'base',
                'PositionInspector',
                1,
                'DeFi'
            ),
            (
                '0x6E230D0e457Ea2398FB3A22FB7f9B7F68F06a14d',
                'maverick',
                'base',
                'PoolInformation',
                1,
                'DeFi'
            ),
            (
                '0xC402D13B0D04867649a632F17528c753d8f6FBD2',
                'maverick',
                'base',
                'PoolPositionManager',
                1,
                'DeFi'
            ),
            (
                '0xbBF1EE38152E9D8e3470Dc47947eAa65DcA94913',
                'maverick',
                'base',
                'PoolPositionAndRewardFactorySlim',
                1,
                'DeFi'
            ),
            (
                '0x32AED3Bce901DA12ca8489788F3A99fCe1056e14',
                'maverick',
                'base',
                'Router',
                1,
                'DeFi'
            ),
            (
                '0x64b88c73A5DfA78D1713fE1b4c69a22d7E0faAa7',
                'maverick',
                'base',
                'MAV',
                1,
                'Token'
            ),
            (
                '0xFcCB5263148fbF11d58433aF6FeeFF0Cc49E0EA5',
                'maverick',
                'base',
                'veMAV',
                1,
                'Token'
            )
    ) as t(address, app, chain, name, version, category)
-- bsc
union
select
    to_address as address,
    'maverick' as app,
    'bsc' as chain,
    'Pool' as name,
    1 as version,
    'DeFi' as category
from bsc_flipside.core.fact_traces
where
    from_address = lower('0x76311728FF86054Ad4Ac52D2E9Ca005BC702f589')
    and output like '0x60806040%'
union
select address, app, chain, name, version, category
from
    (
        values
            (
                '0x76311728FF86054Ad4Ac52D2E9Ca005BC702f589',
                'maverick',
                'bsc',
                'Factory',
                1,
                'DeFi'
            ),
            (
                '0x23Aeaf001E5DF9d7410EE6C6916f502b7aC8e9D0',
                'maverick',
                'bsc',
                'Position',
                1,
                'DeFi'
            ),
            (
                '0x70Cd6087033E0b99e4e449D3B904FaD194D888A0',
                'maverick',
                'bsc',
                'PositionInspector',
                1,
                'DeFi'
            ),
            (
                '0xB3916179619EEF2497C646e664Be6e13cd1AB445',
                'maverick',
                'bsc',
                'PoolInformation',
                1,
                'DeFi'
            ),
            (
                '0x2D11545d36FfA0b8558e83C26e45cFaF14BDBAB2',
                'maverick',
                'bsc',
                'PoolPositionManager',
                1,
                'DeFi'
            ),
            (
                '0xFC328EA7700A86a9CcBE281D44C258385E26a9c0',
                'maverick',
                'bsc',
                'PoolPositionAndRewardFactorySlim',
                1,
                'DeFi'
            ),
            (
                '0xD53a9f3FAe2bd46D35E9a30bA58112A585542869',
                'maverick',
                'bsc',
                'Router',
                1,
                'DeFi'
            ),
            (
                '0xd691d9a68C887BDF34DA8c36f63487333ACfD103',
                'maverick',
                'bsc',
                'MAV',
                1,
                'Token'
            ),
            (
                '0xE6108f1869d37E5076a56168C66A1607EdB10819',
                'maverick',
                'bsc',
                'veMAV',
                1,
                'Token'
            )
    ) as t(address, app, chain, name, version, category)
-- ethereum
union
select
    to_address as address,
    'maverick' as app,
    'ethereum' as chain,
    'Pool' as name,
    1 as version,
    'DeFi' as category
from ethereum_flipside.core.fact_traces
where
    from_address = lower('0xEb6625D65a0553c9dBc64449e56abFe519bd9c9B')
    and output like '0x60806040%'
union
select address, app, chain, name, version, category
from
    (
        values
            (
                '0xEb6625D65a0553c9dBc64449e56abFe519bd9c9B',
                'maverick',
                'ethereum',
                'Factory',
                1,
                'DeFi'
            ),
            (
                '0x4A3e49f77a2A5b60682a2D6B8899C7c5211EB646',
                'maverick',
                'ethereum',
                'Position',
                1,
                'DeFi'
            ),
            (
                '0x456A37144162900799f405be34f815dE7C3DA53C',
                'maverick',
                'ethereum',
                'PositionInspector',
                1,
                'DeFi'
            ),
            (
                '0x0087D11551437c3964Dddf0F4FA58836c5C5d949',
                'maverick',
                'ethereum',
                'PoolInformation',
                1,
                'DeFi'
            ),
            (
                '0xE7583AF5121a8f583EFD82767CcCfEB71069D93A',
                'maverick',
                'ethereum',
                'PoolPositionManager',
                1,
                'DeFi'
            ),
            (
                '0x4F24D73773fCcE560f4fD641125c23A2B93Fcb05',
                'maverick',
                'ethereum',
                'PoolPositionAndRewardFactorySlim',
                1,
                'DeFi'
            ),
            (
                '0xbBF1EE38152E9D8e3470Dc47947eAa65DcA94913',
                'maverick',
                'ethereum',
                'Router',
                1,
                'DeFi'
            ),
            (
                '0x7448c7456a97769f6cd04f1e83a4a23ccdc46abd',
                'maverick',
                'ethereum',
                'MAV',
                1,
                'Token'
            ),
            (
                '0x4949Ac21d5b2A0cCd303C20425eeb29DCcba66D8',
                'maverick',
                'ethereum',
                'veMAV',
                1,
                'Token'
            )
    ) as t(address, app, chain, name, version, category)
