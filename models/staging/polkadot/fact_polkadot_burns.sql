with   
    burns as (
        {{ parse_parity_parquets("polkadot", "burns") }}
    ) 
    , prices as ({{ get_coingecko_price_with_latest("polkadot") }})
    select
        chain
        , type
        , amount as raw_amount
        , pallet
        , method
        , timestamp::date as date
        , timestamp
        , coalesce(burns.amount, 0) / POW(10, decimals) as burns_native
        , coalesce(burns.amount, 0) / POW(10, decimals) * price as burns
    FROM burns
    left join prices on prices.date = burns.timestamp::date 
