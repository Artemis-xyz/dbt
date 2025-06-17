{{ config(materialized="table") }}
-- Premint addresses for ethereum are representative of USDT that has been bridged to other L2s
select contract_address, premint_address
from
    (
        values
            -- Un-released Supply 
            (
                '0xdAC17F958D2ee523a2206206994597C13D831ec7',
                '0x5754284f345afc66a98fbb0a0afe71e0f007b949'
            ),
            -- crvUSD pegKeeper
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0x9201da0D97CaAAff53f01B2fB56767C7072dE340'
            ),
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0xFb726F57d251aB5C731E5C64eD4F5F94351eF9F3'
            ),
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0x3fA20eAa107DE08B38a8734063D605d5842fe09C'
            ),
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0x0a05FF644878B908eF8EB29542aa88C07D9797D3'
            ),
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0x503E1Bf274e7a6c64152395aE8eB57ec391F91F8'
            ),
            -- crvUSD controller
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0x8472a9a7632b173c8cf3a86d3afec50c35548e76'
            ),
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0x100daa78fc509db39ef7d04de0c1abd299f4c6ce'
            ),
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0x4e59541306910ad6dc1dac0ac9dfb29bd9f15c67'
            ),
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0xa920de414ea4ab66b97da1bfe9e6eca7d4219635'
            ),
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0xec0820efafc41d8943ee8de495fc9ba8495b15cf'
            ),
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0x1c91da0223c763d2e0173243eadaa0a2ea47e704'
            ),
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0xf8C786b1064889fFd3c8A08B48D5e0c159F4cBe3'
            ),
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0x8aca5A776a878Ea1F8967e70a23b8563008f58Ef'
            ),
            (
                '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e',
                '0x652aEa6B22310C89DCc506710CaD24d2Dba56B11'
            )

    ) as results(contract_address, premint_address)
