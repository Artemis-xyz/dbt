with
    transactions as (
        {{ parse_parity_parquets("frequency", "transactions") }}
    ),
    fees as (
        {{ parse_parity_parquets("frequency", "fees") }}
    ),
    decimals as (
        select
            chain
            , unit
            , decimals
        from {{ source("MANUAL_STATIC_TABLES", "polkadot_token_decimals") }}
        where chain = 'frequency'
    )
    select
        coalesce(t.timestamp::date, f.timestamp::date) as date
        , coalesce(t.timestamp, f.timestamp) as timestamp
        , coalesce(t.number, f.number) as number
        , coalesce(t.hash, f.hash) as hash
        , success
        , signer_id
        , coalesce(t.relay_chain, f.relay_chain) as relay_chain
        , coalesce(f.fees, 0) / POW(10, decimals) as fees_native
    FROM transactions as t
    full join fees as f on t.hash = f.hash
    where
        coalesce(t.timestamp, f.timestamp) < date(sysdate())
