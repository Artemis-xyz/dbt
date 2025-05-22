{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'BELIEVE',
    )
}}

with 
    believe_coins_minted as (
        select
            block_timestamp,
            coins_minted_address
        from PC_DBT_DB.PROD.fact_believe_coins_minted
    )

    , all_swaps as (
        select
            distinct
                block_timestamp,
                tx_id,
                swapper as trader,
                swap_from_amount as amount_native,
                avg(p.price) over (partition by tx_id, date_trunc(hour, block_timestamp)) as price,
            case
                when swap_from_amount_usd is null then
                    swap_to_amount_usd
                else
                    amount_native * price
            end as amount_usd,
            swap_from_mint as coins_minted_address
        from solana_flipside.defi.ez_dex_swaps s
        left join solana_flipside.price.ez_prices_hourly p
            on p.hour = date_trunc(hour, s.block_timestamp)
            and p.token_address in (select coins_minted_address from believe_coins_minted)
        where s.swap_from_mint in (select coins_minted_address from believe_coins_minted)

        union all

        select
            block_timestamp
            , tx_id
            , swapper as trader
            , swap_to_amount as amount_native
            , price
            , swap_to_amount * p.price as amount_usd
            , swap_from_mint as coins_minted_address
        from solana_flipside.defi.ez_dex_swaps
        left join solana_flipside.price.ez_prices_hourly p
            on p.hour = date_trunc(hour, block_timestamp)
            and p.is_native
        where swap_from_mint in (select coins_minted_address from believe_coins_minted)
        and swap_to_mint = 'So11111111111111111111111111111111111111112'
    )
select
    block_timestamp
    , tx_id
    , trader
    , amount_native
    , price
    , amount_usd
    , coins_minted_address
from all_swaps
