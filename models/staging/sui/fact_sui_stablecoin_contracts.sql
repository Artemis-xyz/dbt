{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('AUSD', '0x2053d08c1e2bd02791056171aab0fd12bd7cd7efad2ab8f6b9c8902f14df2ff2::ausd::AUSD', 6, 'agora-dollar', 0),
            ('USDC', '0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC', 6, 'usd-coin', 0),
            ('USDY', '0x960b531667636f39e85867775f52f6b1f220a058c4de786905bdf761e06a56bb::usdy::USDY', 6, 'ondo-us-dollar-yield', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
-- ('USDT', '0xc060006111016b8a020ad5b33834984a437aaa7d3c74c18e09a95d48aceab08c::coin::COIN', 6, 'tether', 0),