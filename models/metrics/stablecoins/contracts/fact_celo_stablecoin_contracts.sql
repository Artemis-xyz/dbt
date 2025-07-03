{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0xcebA9300f2b948710d2653dD7B07f33A8B32118C', 6, 'usd-coin', 0),
            ('USDT', '0x48065fbBE25f71C9282ddf5e1cD6D6A887483D5e', 6, 'tether', 0),
            ('cEUR', '0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73', 18, 'celo-euro', 0),
            ('cREAL', '0xe8537a3d056da446677b9e9d6c5db704eaab4787', 18, 'celo-real-creal', 0),
            ('USDGLO', '0x4F604735c1cF31399C6E711D5962b2B3E0225AD3', 18, 'glo-dollar', 0),
            ('cUSD', '0x765DE816845861e75A25fCA122bb6898B8B1282a', 18, 'celo-dollar', 0),
            ('cKES', '0x456a3d042c0dbd3db53d5489e98dfb038553b0d0', 18, 'celo-kenyan-shilling', 0),
            ('cGHS', '0xfaea5f3404bba20d3cc2f8c4b0a888f55a3c7313', 18, 'cghs', 0),
            ('PUSO', '0x105d4a9306d2e55a71d2eb95b81553ae1dc20d7b', 18, 'puso', 0),
            ('cJPY', '0xc45eCF20f3CD864B32D9794d6f76814aE8892e20', 18, 'celo-japanese-yen', 0),
            ('cNGN', '0xE2702Bd97ee33c88c8f6f92DA3B733608aa76F71', 18, 'celo-nigerian-naira', 0),
            ('cCHF', '0xb55a79F398E759E43C95b979163f30eC87Ee131D', 18, 'cchf', 0),
            ('cGBP', '0xccf663b1ff11028f0b19058d0f7b674004a40746', 18, 'celo-british-pound', 0),
            ('cAUD', '0x7175504C455076F15c04A2F90a8e352281F492F9', 18, 'celo-australian-dollar', 0),
            ('cCAD', '0xff4ab19391af240c311c54200a492233052b6325', 18, 'celo-canadian-dollar', 0),
            ('cZAR', '0x4c35853a3b4e647fd266f4de678dcc8fec410bf6', 18, 'celo-south-african-rand', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
