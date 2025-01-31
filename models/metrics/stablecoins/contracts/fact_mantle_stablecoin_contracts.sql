{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9', 6, 'usd-coin', 0),
            ('USDT', '0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE', 6, 'tether', 0),
            (
                'USDe',
                '0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34',
                18,
                'ethena-usde',
                0
            ),
            ('AUSD', '0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a', 6, 'agora-dollar', 0),
            ('USDY', '0x5bE26527e817998A7206475496fDE1E68957c5A6', 18, 'ondo-us-dollar-yield', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
