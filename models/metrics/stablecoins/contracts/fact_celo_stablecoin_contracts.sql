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
            ('cUSD', '0x765DE816845861e75A25fCA122bb6898B8B1282a', 18, 'celo-dollar', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
