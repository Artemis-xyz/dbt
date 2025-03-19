{{
    config(
        materialized='table',
        snowflake_warehouse='SYNTHETIX',
        database='SYNTHETIX',
        schema='raw',
        alias='fact_synthetix_tvl_by_chain_and_token'
    )
}}

with all_tvl_by_chain_and_token as (
    SELECT * FROM {{ ref('fact_synthetix_ethereum_tvl') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_synthetix_optimism_tvl') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_synthetix_base_tvl') }}
)

select
    date,
    chain,
    token,
    tvl_usd
from all_tvl_by_chain_and_token