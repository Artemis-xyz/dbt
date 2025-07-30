{{ config(materialized="table") }}
-- Premint addresses for ethereum are representative of USDT that has been bridged to other L2s
select contract_address, premint_address
from
    (
        values
            -- USDT
            --  Polygon
            --      Matic Bridge
            (
                '0xdAC17F958D2ee523a2206206994597C13D831ec7',
                '0x40ec5B33f54e0E8A33A975908C5BA1c14e5BbbDf'
            ),
            --      Wormhole
            (
                '0xdAC17F958D2ee523a2206206994597C13D831ec7',
                '0x3ee18B2214AFF97000D974cf647E7C347E8fa585'
            ),
            (
                '0xdC035D45d973E3EC169d2276DDab16f1e407384F',
                '0x7d4958454a3f520bDA8be764d06591B054B0bf33'
            ),
            --      Axelar
            (
                '0xdAC17F958D2ee523a2206206994597C13D831ec7',
                '0x4F4495243837681061C4743b74B3eEdf548D56A5'
            ),
            -- Arbitrum
            (
                '0xdAC17F958D2ee523a2206206994597C13D831ec7',
                '0xcEe284F754E854890e311e3280b767F80797180d'
            ),
            -- Optimism
            (
                '0xdAC17F958D2ee523a2206206994597C13D831ec7',
                '0x99C9fc46f92E8a1c0deC1b1747d010903E884bE1'
            ),
            -- Base
            (
                '0xdAC17F958D2ee523a2206206994597C13D831ec7',
                '0x3154Cf16ccdb4C6d922629664174b904d80F2C35'
            ),
            -- BSC
            (
                '0xdAC17F958D2ee523a2206206994597C13D831ec7',
                '0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503'
            ),
            -- USDT0
            (
                '0xdAC17F958D2ee523a2206206994597C13D831ec7',
                '0x6C96dE32CEa08842dcc4058c14d3aaAD7Fa41dee'
            )
    ) as results(contract_address, premint_address)
