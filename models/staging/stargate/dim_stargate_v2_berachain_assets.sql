{{config(materialized="table")}}
select token_messaging_address, stargate_implementation_pool, token_address, symbol, decimals, coingecko_id
from (
    values
        ('0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6', '0xAF54BE5B6eEc24d6BFACf1cce4eaF680A8239398', '0x549943e04f40284185054145c6E4e9568C1D3241', 'USDC', 6, 'usd-coin')
        , ('0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6', '0x45f1A95A4D3f3836523F5c83673c797f4d4d263B', '0x7DCC39B4d1C53CB31e1aBc0e358b43987FEF80f7', 'ETH', 18, 'ethereum')
) as t(token_messaging_address, stargate_implementation_pool, token_address, symbol, decimals, coingecko_id)
