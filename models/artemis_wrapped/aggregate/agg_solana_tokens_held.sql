{{config(materialized='table')}}
--WOLF OF WALLSTREET
--RUGGED RAT
with
    tokens as (
        select contract_address, symbol
        from (
            values
                ('DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263', 'BONK')
                , ('EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm', 'WIF')
                , ('2qEHjDLDLbuBgRYvsxhc5D6uDWAivNFZGan56P1tpump', 'PNUT')
                , ('7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr', 'POPCAT')
                , ('9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump', 'FARTCOIN')
                , ('HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC', 'AI16Z')
                , ('CzLSujWBLFsSjncfkh59rUFqvafWcY5tzedWJSuypump', 'GOAT')
                , ('MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5', 'MEW')
                , ('63LfDmNb3MQ8mw9MtZ2To9bEA2M71kZUUGq5tiJxcqj9', 'GIGA')
                , ('ukHH6c7mMyiWCf1b9pnWe25TSpkDDt3H5pQZgZ74J82', 'BOME')
                , ('GJAFwWjJ3vnTsrQVabjBVK2TYB1YtRCQXRDfDgUnpump', 'ACT')
                , ('8x5VqbHA8D7NkD52uNuS5nnt3PwA8pLD34ymskeSo2Wn', 'ZEREBRO')
                , ('ED5nyyWEzpPPiWimP8vYm7sD7TD3LAt3Q3gRTWHzPJBY', 'MOODENG')
                , ('Df6yfrKC8kZE3KNkrHERKzAetSxbrWeniQfyJY4Jpump', 'CHILLGUY')
                , ('A8C3xuqscfmyLrte3VmTqrAq8kgMASius9AFNANwpump', 'FWOG')


                , ('4GFe6MBDorSy5bLbiUMrgETr6pZcjyfxMDm5ehSgpump', 'HAWKTUAH')
                , ('3an8rhdepsLCya22af7qDBKPbdomw8K4iCHXaA2Gpump', 'QUANT')
                
            ) as t(contract_address, symbol)
    )
select lower(address) as address, symbol, min(block_timestamp) as first_seen, coalesce(max(block_timestamp), sysdate()) as last_interaction_timestamp
from {{ ref('fact_solana_address_balances_by_token') }} t
inner join tokens on lower(t.contract_address) = lower(tokens.contract_address)
where block_timestamp > '2023-12-31' 
group by 1, 2