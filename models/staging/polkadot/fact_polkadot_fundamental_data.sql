with
    transactions as (
        {{ parse_parity_parquets("polkadot", "transactions") }}
    ),
    fees as (
        {{ parse_parity_parquets("polkadot", "fees") }}
    ),
    prices as ({{ get_coingecko_price_with_latest("polkadot") }})
    select
        coalesce(t.timestamp, f.timestamp) as timestamp
        , coalesce(t.number, f.number) as number
        , coalesce(t.hash, f.hash) as hash
        , success
        , signer_id
        , coalesce(t.relay_chain, f.relay_chain) as relay_chain
    FROM transactions as t
    full join fees as f on t.hash = f.hash
    where
        coalesce(t.timestamp, f.timestamp) < dateadd('day', -1, current_date)

