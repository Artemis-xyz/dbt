{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', '0x608792deb376cce1c9fa4d0e6b7b44f507cffa6a', 6, 'usd-coin', 0)
            , ('USDC', '0xe2053bcf56d2030d2470fb454574237cf9ee3d4b', 6, 'usd-coin', 0)
            , ('USDT', '0x5c13e303a62fc5dedf5b52d66873f2e59fedadc2', 6, 'tether', 0)
            , ('USDT', '0x9025095263d1e548dc890a7589a4c78038ac40ab', 6, 'tether', 0)
            , ('USDT', '0xd077a400968890eacc75cdc901f0356c943e4fdb', 6, 'tether', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
