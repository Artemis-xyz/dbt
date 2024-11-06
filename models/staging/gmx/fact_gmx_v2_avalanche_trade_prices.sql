{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GMX'
    )
}}

SELECT
    p.block_timestamp,
    p.tx_hash,
    p.event_index,
    p.chain,
    m.index_token_address as token_address,
    CASE WHEN lower(c.address) = lower('0x152b9d0FdC40C096757F570A51E494bd4b943E50') THEN
        p.execution_price::number / pow(10, 22)
    ELSE
        p.execution_price::number / pow(10, 12)
    END as price,
    c.symbol
FROM {{ ref('fact_gmx_v2_avalanche_position_changes_and_markets') }} p
LEFT JOIN {{ ref('dim_gmx_v2_avalanche_market_to_underlying') }} m on lower(m.market) = lower(p.market)
LEFT JOIN {{ ref('fact_gmx_v2_avalanche_tokens') }} c on lower(c.address) = lower(m.index_token_address)
WHERE lower(m.index_token_address) in  (lower('0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB'), lower('0x152b9d0FdC40C096757F570A51E494bd4b943E50'))