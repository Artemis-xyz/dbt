{% macro fact_solana_dex_swaps(protocol) %}
With price as(
    select 
        date
        , price
        , token_address
    from {{ ref("fact_solana_dex_token_prices") }}
)

select 
    block_timestamp
    , swapper
    , inserted_timestamp
    , fact_swaps_id
    , swap_from_mint
    , swap_from_amount
    , swap_to_mint
    , swap_to_amount
    , coalesce(swap_from_amount * from_price.price, 0) as swap_from_amount_usd
    , coalesce(swap_to_amount * to_price.price, 0) as swap_to_amount_usd
    from solana_flipside.defi.fact_swaps as swaps
    left join price as from_price on from_price.date = date_trunc('day', swaps.block_timestamp) and swap_from_mint = from_price.token_address
    left join price as to_price on to_price.date = date_trunc('day', swaps.block_timestamp) and swap_to_mint = to_price.token_address
where succeeded and lower(swap_program) like '%{{protocol}}%'
{% if is_incremental() %}
    AND block_timestamp::date >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
{% elif protocol == 'raydium' %}
    AND block_timestamp::date >= date('2022-04-22')
{% endif %}
{% endmacro %}