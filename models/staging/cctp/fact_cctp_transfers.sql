{{ config(
    materialized="incremental",
    snowflake_warehouse="CCTP",
)}}
with
tranfers as (
    {{ cctp_transfers("ethereum", "0xbd3fa81b58ba92a82136038b25adec7066af3155", 0) }}
    union all
    {{ cctp_transfers("avalanche", "0x6b25532e1060ce10cc3b0a99e5683b91bfde6982", 1) }}
    union all
    {{ cctp_transfers("optimism", "0x2B4069517957735bE00ceE0fadAE88a26365528f", 2) }}
    union all
    {{ cctp_transfers("arbitrum", "0x19330d10D9Cc8751218eaf51E8885D058642E08A", 3) }}
    union all
    {{ cctp_transfers("base", "0x1682Ae6375C4E4A97e4B583BC394c861A46D8962", 6) }}
    union all
    {{ cctp_transfers("polygon", "0x9daF8c91AEFAE50b9c0E69629D3F6Ca40cA3B3FE", 7) }}
    union all
    {{ cctp_transfers("solana", "CCTPiPYPc6AsJuwueEnWgSgucamXDZwBd53dQ11YiKX3", 5) }}
    union all
    {{ cctp_transfers("noble", "noble1afmt2kk6n9fr7pwkjhe25hz86dlmkp2v4phl98", 4) }}
)
, usdc_prices as ({{ get_coingecko_price_with_latest("usd-coin") }})
, eurc_prices as ({{ get_coingecko_price_with_latest("euro-coin") }})
, chain_id_map as (
    select chain_id, chain
    from  (
        values
            (0, 'ethereum'),
            (1, 'avalanche'),
            (2, 'optimism'),
            (3, 'arbitrum'),
            (4, 'noble'),
            (5, 'solana'),
            (6, 'base'),
            (7, 'polygon')
            
    ) as t(chain_id, chain)
)
select
    block_timestamp
    , block_number
    , tx_hash
    , contract_address
    , sender
    , nonce
    , reciepient
    , amount
    , case 
        when burn_token in (
            '0x0b2c639c533813f4aa9d7837caf62653d097ff85',
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
            '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913',
            '0x3c499c542cef5e3811e1192ce70d8cc03d5c3359',
            '0xaf88d065e77c8cc2239327c5edb3a432268e5831',
            '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
            '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e',
            'uusdc'
        ) then coalesce((amount/1e6) * usdc_prices.price, (amount/1e6))
        when burn_token in (
            '0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c',
            '0xc891eb4cbdeff6e073e859e987815ed1505c2acd',
            'HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr'
        ) then (amount/1e6) * eurc_prices.price
        else null
    end as amount_usd
    , burn_token
    , source_domain_id
    , destination_domain_id
    , c1.chain as src_chain
    , c2.chain as dst_chain
from tranfers
left join usdc_prices on usdc_prices.date = tranfers.block_timestamp::date
left join eurc_prices on eurc_prices.date = tranfers.block_timestamp::date
left join chain_id_map c1 on tranfers.source_domain_id = c1.chain_id
left join chain_id_map c2 on tranfers.destination_domain_id = c2.chain_id
