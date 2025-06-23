{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913', 6, 'usd-coin', 0),
            ('EURC', '0x60a3E35Cc302bFA44Cb288Bc5a4F316Fdb1adb42', 6, 'euro-coin', 0),
            ('DAI', '0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb', 18, 'dai', 0),
            (
                'USDC',
                '0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA',
                6,
                'bridged-usd-coin-base',
                0
            ),
            ('USDS', '0x820C137fa70C8691f0e44Dc420a5e53c168921Dc', 18, 'usds', 0),
            ('DOLA', '0x4621b7a9c75199271f773ebd9a499dbd165c3191', 18, 'dola-usd', 0),
            ('USDz','0x04d5ddf5f3a8939889f11e97f8c4bb48317f1938', 18, 'anzen-usdz', 0),
            ('crvUSD', '0x417ac0e078398c154edfadd9ef675d30be60af93', 18, 'crvusd', 0),
            ('cgUSD', '0xca72827a3d211cfd8f6b00ac98824872b72cab49', 6, 'cygnus-finance-global-usd', 0),
            ('USD3', '0xefb97aaf77993922ac4be4da8fbc9a2425322677', 18, 'web-3-dollar', 0),
            ('IDRX', '0x18Bc5bcC660cf2B9cE3cd51a404aFe1a0cBD3C22', 2, 'idrx', 0),
            ('cNGN', '0x46C85152bFe9f96829aA94755D9f915F9B10EF5F', 6, 'celo-nigerian-naira', 0),
            ('BRZ', '0xE9185Ee218cae427aF7B9764A011bb89FeA761B4', 18, 'brz', 0),
            ('CADC', '0x043eB4B75d0805c43D7C834902E335621983Cf03', 18, 'cad-coin', 0),
            ('MXNe', '0x269caE7Dc59803e5C596c95756faEeBb6030E0aF', 6, 'real-mxn', 0),
            ('ZARP', '0xb755506531786C8aC63B756BaB1ac387bACB0C04', 18, 'zarp-stablecoin', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
