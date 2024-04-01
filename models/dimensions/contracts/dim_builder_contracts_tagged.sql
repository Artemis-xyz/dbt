{{ config(materialized="table") }}
select name, address, censors, entity, category, 'ethereum' as chain
from
    (
        values
            (
                'rsync-builder',
                '0x1f9090aaE28b8a3dCeaDf281B0F12828e676c326',
                'Censoring',
                'rsync-builder',
                'MEV Builder'
            ),
            (
                'Flashbots: Builder',
                '0xDAFEA492D9c6733ae3d56b7Ed1ADB60692c98Bc5',
                'Censoring',
                'Flashbots',
                'MEV Builder'
            ),
            (
                'Eden Network: Builder',
                '0xAAB27b150451726EC7738aa1d0A94505c8729bd1',
                'Censoring',
                'Eden Network',
                'MEV Builder'
            ),
            (
                'beaverbuild',
                '0x95222290DD7278Aa3Ddd389Cc1E1d165CC4BAfe5',
                'Semi-Censoring',
                'beaverbuild',
                'MEV Builder'
            ),
            (
                'builder0x69',
                '0x690b9a9e9aa1c9db991c7721a92d351db4fac990',
                'Semi-Censoring',
                'builder0x69',
                'MEV Builder'
            )
    ) as t(name, address, censors, entity, category)
