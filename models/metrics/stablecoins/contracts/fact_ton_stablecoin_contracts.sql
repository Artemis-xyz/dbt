{{ config(materialized="table") }}
select symbol, contract_address, num_decimals, coingecko_id, initial_supply
from
    (
        values
            ('USDT', '0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE', 6, 'tether', 0),
            ('USDe', '0:086FA2A675F74347B08DD4606A549B8FDB98829CB282BC1949D3B12FBAED9DCC', 6, 'ethena-usde', 0)
    ) as results(symbol, contract_address, num_decimals, coingecko_id, initial_supply)
