{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GMX'
    )
}}

SELECT
    block_timestamp,
    tx_hash,
    execution_price,
    market,
    index_token_address as token_address
FROM {{ ref('fact_gmx_v2_avalanche_position_changes_and_markets') }} p
LEFT JOIN {{ ref('dim_gmx_v2_avalanche_market_to_underlying') }} m on m.market = p.market
LEFT JOIN {{ ref('fact_gmx_v2_avalanche_tokens') }} c on c.token_address = p.index_token_address