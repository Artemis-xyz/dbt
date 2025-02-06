{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='raw',
        alias='fact_balancer_tvl_by_chain_and_token'
    )
}}

with all_tvl_by_chain_and_token as (
    SELECT * FROM {{ ref('fact_balancer_v2_ethereum_tvl_by_pool_and_token') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_balancer_v2_arbitrum_tvl_by_pool_and_token') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_balancer_v2_polygon_tvl_by_pool_and_token') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_balancer_v2_gnosis_tvl_by_pool_and_token') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_balancer_v1_tvl') }}
)

select
    date,
    chain,
    version,
    contract_address,
    token,
    native_balance as tvl_native,
    usd_balance as tvl_usd
from all_tvl_by_chain_and_token