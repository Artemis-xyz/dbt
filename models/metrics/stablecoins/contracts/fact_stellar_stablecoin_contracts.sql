{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDC', 'USDC-GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN', 7, 'usd-coin', 0),
            ('EURC', 'EURC-GDHU6WRG4IEQXM5NZ4BMPKOXHW76MZM4Y2IEMFDVXBSDP6SJY4ITNPP2', 7, 'euro-coin', 0),
            ('EURS', 'EURS-GC5FGCDEOGOGSNWCCNKS3OMEVDHTE3Q5A5FEQWQKV3AXA7N6KDQ2CUZJ', 7, 'stasis-eurs', 0),
            ('USDGLO', 'USDGLO-GBBS25EGYQPGEZCGCFBKG4OAGFXU6DSOQBGTHELLJT3HZXZJ34HWS6XV', 7, 'glo-dollar', 0),
            ('GYEN', 'GYEN-GDF6VOEGRWLOZ64PQQGKD2IYWA22RLT37GJKS2EJXZHT2VLAGWLC5TOB', 7, 'gyen', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
