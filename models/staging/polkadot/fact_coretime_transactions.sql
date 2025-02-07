with
    transactions as (
        {{ parse_parity_parquets("coretime", "transactions") }}
    ),
    fees as (
        {{ parse_parity_parquets("coretime", "fees") }}
    ),
    decimals as (
        select
            chain
            , unit
            , decimals
        from {{ source("MANUAL_STATIC_TABLES", "polkadot_token_decimals") }}
        where chain = 'polkadot'
    ),
    prices as ({{ get_coingecko_price_with_latest("polkadot") }})
    select
        coalesce(t.timestamp::date, f.timestamp::date) as date
        , coalesce(t.timestamp, f.timestamp) as timestamp
        , coalesce(t.number, f.number) as number
        , coalesce(t.hash, f.hash) as hash
        , success
        , signer_id
        , coalesce(t.relay_chain, f.relay_chain) as relay_chain
        , coalesce(f.fees, 0) / POW(10, decimals) as fees_native
        , coalesce(f.fees, 0) / POW(10, decimals) * price as fees_usd
    FROM transactions as t
    full join fees as f on t.hash = f.hash
    left join prices on prices.date = coalesce(t.timestamp::date, f.timestamp::date)
    where
        coalesce(t.timestamp, f.timestamp) < date(sysdate())
