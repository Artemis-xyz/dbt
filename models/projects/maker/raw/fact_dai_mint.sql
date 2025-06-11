{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_dai_mint"
    )
}}

SELECT
    block_timestamp,
    tx_hash,
    to_address as usr,
    raw_amount_precise as wad
FROM ethereum_flipside.core.ez_token_transfers
where from_address = '0x0000000000000000000000000000000000000000'
and lower(contract_address) = lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')
