{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_pause_proxy_mkr_trxns_raw"
    )
}}

SELECT
    block_timestamp AS ts,
    tx_hash AS hash,
    CAST(raw_amount_precise AS DOUBLE) AS expense,
    to_address AS address
FROM ethereum_flipside.core.ez_token_transfers
WHERE contract_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' -- MKR token address
  AND from_address = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb' -- pause proxy
  AND to_address != '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb' -- excluding transfers to itself

UNION ALL

SELECT 
    block_timestamp AS ts,
    tx_hash AS hash,
    -CAST(raw_amount_precise AS DOUBLE) AS expense,
    from_address AS address
FROM ethereum_flipside.core.ez_token_transfers
WHERE contract_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' -- MKR token address
  AND to_address = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb' -- pause proxy
    AND from_address NOT IN ('0x8ee7d9235e01e6b42345120b5d270bdb763624c7', '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb') -- excluding initial transfers in and transfers from itself