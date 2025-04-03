{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 6, 'usd-coin', 0),
            ('DAI', '0x6B175474E89094C44Da98b954EedeAC495271d0F', 18, 'dai', 0),
            ('USDT', '0xdAC17F958D2ee523a2206206994597C13D831ec7', 6, 'tether', 0),
            (
                'BUSD',
                '0x4Fabb145d64652a948d72533023f6E7A623C7C53',
                18,
                'binance-usd',
                0
            ),
            (
                'USDe',
                '0x4c9edd5852cd905f086c759e8383e09bff1e68b3',
                18,
                'ethena-usde',
                0
            ),
            ('PYUSD', '0x6c3ea9036406852006290770BEdFcAbA0e23A0e8', 6, 'paypal-usd', 0),
            ('EURC', '0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c', 6, 'euro-coin', 0),
            ('USDP', '0x8e870d67f660d95d5be530380d0ec0bd388289e1', 18, 'paxos-standard', 0),
            ('AUSD', '0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a', 6, 'agora-dollar', 0),
            ('USDS', '0xdC035D45d973E3EC169d2276DDab16f1e407384F', 18, 'usds', 0),
            ('FDUSD', '0xc5f0f7b66764F6ec8C8Dff7BA683102295E16409', 18, 'first-digital-usd', 0),
            ('USDY', '0x96F6eF951840721AdBF46Ac996b59E0235CB985C', 18, 'ondo-us-dollar-yield', 0),
            ('USD0', '0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5', 18, 'usual-usd', 0),
            ('USDX', '0xf3527ef8dE265eAa3716FB312c12847bFBA66Cef', 18, 'usdx-money-usdx', 0),
            ('USR', '0x66a1E37c9b0eAddca17d3662D6c05F4DECf3e110', 18, 'resolv-usr', 0),
            ('TUSD', '0x0000000000085d4780B73119b644AE5ecd22b376', 18, 'true-usd', 0),
            ('FRAX', '0x853d955acef822db058eb8505911ed77f175b99e', 18, 'frax', 0),
            ('RLUSD', '0x8292bb45bf1ee4d140127049757c2e0ff06317ed', 18, 'ripple-usd', 0),
            ('USDa', '0x8a60e489004ca22d775c5f2c657598278d17d9c2', 18, 'usda-2', 0),
            ('DEUSD', '0x15700b564ca08d9439c58ca5053166e8317aa138', 18, 'elixir-deusd', 0),
            ('GHO', '0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f', 18, 'gho', 0),
            ('DOLA', '0x865377367054516e17014ccded1e7d814edc9ce4', 18, 'dola-usd', 0),
            ('FLEXUSD', '0xa774ffb4af6b0a91331c084e1aebae6ad535e6f3', 18, 'flex-usd', 0),
            ('USDz', '0xa469b7ee9ee773642b3e93e842e5d9b5baa10067', 18, 'anzen-usdz', 0),
            ('USDtb', '0xc139190f447e929f090edeb554d95abb8b18ac1c', 18, 'usdtb', 0),
            ('crvUSD', '0xf939e0a03fb07f59a73314e73794be0e57ac1b4e', 18, 'crvusd', 0),
            ('GUSD', '0x056fd409e1d7a124bd7017459dfea2f387b6d5cd', 2, 'gemini-dollar', 0),
            ('S_USD', '0x57Ab1ec28D129707052df4dF418D58a2D46d5f51', 18, 'nusd', 0),
            ('USDD', '0x3D7975EcCFc61a2102b08925CbBa0a4D4dBB6555', 18, 'usdd', 0),
            ('FRXUSD', '0xcacd6fd266af91b8aed52accc382b4e165586e29', 18, 'frax-usd', 0),
            ('MIM', '0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3', 18, 'magic-internet-money', 0),
            ('USN', '0xdA67B4284609d2d48e5d10cfAc411572727dc1eD', 18, 'noon-usn', 0),
            ('USD3', '0x0d86883faf4ffd7aeb116390af37746f45b6f378', 18, 'web-3-dollar', 0),
            ('USDG', '0xe343167631d89B6Ffc58B88d6b7fB0228795491D', 6, 'global-dollar', 0),
            ('ANGLE_USD', '0x0000206329b97db379d5e1bf586bbdb969c63274', 18, 'angle-usd', 0),
            ('fxUSD', '0x085780639cc2cacd35e474e71f4d000e2405d8f6', 18, 'f-x-protocol-fxusd', 0),
            ('EURT', '0xc581b735a1688071a1746c968e0798d642ede491', 6, 'tether-eurt', 0),
            ('AEUR', '0xA40640458FBc27b6EefEdeA1E9C9E17d4ceE7a21', 18, 'anchored-coins-eur', 0),
            ('LUSD', '0x5f98805A4E8be255a32880FDeC7F6728C6568bA0', 18, 'liquity-usd', 0),
            ('BOLD', '0xb01dd87B29d187F3E3a4Bf6cdAebfb97F3D9aB98', 18, 'liquity-bold', 0),
            ('EURS', '0xdB25f211AB05b1c97D595516F45794528a807ad8', 2, 'stasis-eurs', 0),
            ('EURA', '0x1a7e4e63778B4f12a199C062f3eFdD288afCBce8', 18, 'ageur', 0),
            ('IDRT', '0x998FFE1E43fAcffb941dc337dD0468d52bA5b48A', 2, 'rupiah-token', 0),
            ('TRYB', '0x2C537E5624e4af88A7ae4060C022609376C8D0EB', 6, 'bilira', 0),
            ('LVLUSD', '0x7c1156e515aa1a2e851674120074968c905aaf37', 18, 'level-usd', 0),
            ('USDFALCON', '0xfa2b947eec368f42195f24f36d2af29f7c24cec2', 18, 'falcon-finance', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
