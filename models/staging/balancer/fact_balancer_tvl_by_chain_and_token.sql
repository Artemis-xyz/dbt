{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
    )
}}

with all_tvl_by_chain_and_token as (
    SELECT * FROM {{ ref('fact_balancer_ethereum_v2_tvl_by_token') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_balancer_arbitrum_v2_tvl_by_token') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_balancer_polygon_v2_tvl_by_token') }}
)

select
    date,
    chain,
    version,
    contract_address,
    symbol,
    balance_native as tvl_native,
    balance_usd as tvl_usd
from all_tvl_by_chain_and_token