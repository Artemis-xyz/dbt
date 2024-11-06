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
    CASE WHEN lower(c.address) = lower('0x47904963fc8b2340414262125aF798B9655E58Cd') THEN
        p.execution_price::number / pow(10, 22)
    ELSE
        p.execution_price::number / pow(10, 12)
    END as price,
    c.symbol
FROM {{ ref('fact_gmx_v2_arbitrum_position_changes_and_markets') }} p
LEFT JOIN {{ ref('dim_gmx_v2_arbitrum_market_to_underlying') }} m on lower(m.market) = lower(p.market)
LEFT JOIN {{ ref('fact_gmx_v2_arbitrum_tokens') }} c on lower(c.address) = lower(m.index_token_address)
WHERE lower(m.index_token_address) in  (lower('0x47904963fc8b2340414262125aF798B9655E58Cd'), lower('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'))
