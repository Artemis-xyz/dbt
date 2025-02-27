{{config(materialized="table")}}
select token_messaging_address, stargate_implementation_pool, token_address, symbol, decimals, coingecko_id
from (
    values
        ('0x41B491285A4f888F9f636cEc8a363AB9770a0AEF', '0xAc290Ad4e0c891FDc295ca4F0a6214cf6dC6acDC', '0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9', 'USDC', 6, 'usd-coin')
        , ('0x41B491285A4f888F9f636cEc8a363AB9770a0AEF', '0xB715B85682B731dB9D5063187C450095c91C57FC', '0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE', 'USDT', 6, 'tether')
        , ('0x41B491285A4f888F9f636cEc8a363AB9770a0AEF', '0xF7628d84a2BbD9bb9c8E686AC95BB5d55169F3F1', '0xcDA86A272531e8640cD7F1a92c01839911B90bb0', 'mETH', 18, 'mantle-staked-ether')
        , ('0x41B491285A4f888F9f636cEc8a363AB9770a0AEF', '0x4c1d3Fc3fC3c177c3b633427c2F769276c547463', '0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111', 'ETH', 18, 'ethereum')
) as t(token_messaging_address, stargate_implementation_pool, token_address, symbol, decimals, coingecko_id)