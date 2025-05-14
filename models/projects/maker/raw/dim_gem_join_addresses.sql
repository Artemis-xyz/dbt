{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="dim_gem_join_addresses"
    )
}}

with join_addresses as (
    select
        '0x' || SUBSTR(topics [1], 27) as join_address,
        *
    from
        ethereum_flipside.core.fact_event_logs
    where
        topics [0] = lower(
            '0x65fae35e00000000000000000000000000000000000000000000000000000000'
        )
        and contract_address = lower('0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b')
)
, contract_creation_hashes as(
    select
        address as join_address,
        created_tx_hash
    from
        ethereum_flipside.core.dim_contracts
    where
        address in (SELECT lower(join_address) FROM join_addresses)
)
SELECT
    '0x' || RIGHT(t.input, 40) as gem_address,
    h.join_address
FROM
     contract_creation_hashes h
LEFT JOIN ethereum_flipside.core.fact_traces t ON h.created_tx_hash = t.tx_hash
HAVING length(gem_address) = 42